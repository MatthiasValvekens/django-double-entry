import datetime
import logging
from collections import defaultdict
from decimal import Decimal
from itertools import chain

from django import forms
from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db.models import Q
from django.forms.models import ModelForm, modelformset_factory
from django.utils import timezone
from django.utils.translation import (
    ugettext_lazy as _,
)
from djmoney.money import Money

from . import base
from .base import *
from .utils import GnuCashFieldMixin
from ..utils import ParserErrorMixin, CSVUploadForm
from ... import payments, models
from ...payments import _dt_fallback
from ...widgets import (
    DatalistInputWidget, MoneyWidget,
)

logger = logging.getLogger(__name__)

__all__ = [
    'EphemeralPaymentForm', 'EphemeralAddDebtForm',
    'MiscDebtPaymentPopulator', 'AddDebtFormsetPopulator',
    'BulkAddDebtFormSet', 'BulkPaymentFormSet',
    'BulkPaymentUploadForm', 'BulkDebtUploadForm',
    'ProfileAddDebtForm', 'InternalPaymentSplitFormSet'
] + base.__all__


class EphemeralPaymentForm(ModelForm):

    member_id = forms.IntegerField()

    # other fields are for passing around UI data, basically
    # we don't really need them
    ogm = forms.CharField(max_length=21, required=False)

    name = forms.CharField(required=False)

    email = forms.EmailField(required=False)

    debt_filter = forms.SlugField(required=False)

    class Meta:
        model = models.InternalPayment
        # we don't include the member field; this is intentional
        # since we don't want the member records to be fetched at
        # the form level. Two reasons:
        #  (a) inefficient as fuck
        #  (b) we need to query them together with the debt data
        #      to generate the splits
        fields = (
            'nature',
            'total_amount',
            'timestamp'
        )


class EphemeralAddDebtForm(ModelForm):
    member_id = forms.IntegerField()
    gnucash = forms.CharField()
    name = forms.CharField(required=False)
    email = forms.EmailField(required=False)
    filter_slug = forms.SlugField(required=False)

    class Meta:
        model = models.InternalDebtItem
        fields = ('comment', 'total_amount', 'timestamp')


class ProfileAddDebtForm(GnuCashFieldMixin):
    gnucash = forms.CharField(
        label=_('GnuCash category'),
        widget=DatalistInputWidget(
            choices=models.GnuCashCategory.objects.all
        )
    )

    class Meta:
        model = models.InternalDebtItem
        fields = ('total_amount', 'comment')
        widgets = {
            'comment': forms.TextInput(attrs={'size': 30}),
            'total_amount': MoneyWidget
        }

    def __init__(self, member, *args, **kwargs):
        self.member = member
        super(ProfileAddDebtForm, self).__init__(*args, **kwargs)

    def save(self, commit=True):
        instance = super(ProfileAddDebtForm, self)._save(
            commit=False, set_category=commit
        )
        instance.member = self.member
        if commit:
            instance.save()
        return instance


class FetchMembersMixin(ParserErrorMixin):

    def error_at_line(self, line_no, member_str, msg, params=None):
        self.error_at_lines([line_no], member_str, msg, params)

    def error_at_lines(self, line_nos, member_str, msg, params=None):
        if params is None:
            params = {'member_str': member_str}
        else:
            params['member_str'] = member_str

        fmtd_msg = msg % params
        self._errors.insert(0, (sorted(line_nos), fmtd_msg))

    def unknown_member(self, member_str, line_nos):
        msg = _(
            '%(member_str)s does not designate a registered member.'
        )

        self.error_at_lines(
            line_nos, member_str, msg
        )

    def process_member_identifiers(self, data_rows):
        # split the list into email and name indices
        email_index, name_index = defaultdict(list), defaultdict(list)
        for info in data_rows:
            use_email = '@' in info.member_str
            targ = email_index if use_email else name_index
            key = info.member_str if use_email else info.member_str.casefold()
            targ[key].append(info)

        member_email_qs, unseen = models.ChoirMember.objects \
            .select_related('user').with_debt_annotations().by_emails(
                email_index.keys(), validate_unseen=True
            )

        for email in unseen:
            ts = email_index[email]
            self.unknown_member(email, [t.line_no for t in ts])

        # TODO Restrict to active members only, maybe?
        member_name_qs, unseen, duplicates = models.ChoirMember.objects \
            .select_related('user').with_debt_annotations().by_full_names(
                name_index.keys(), validate_unseen=True, validate_nodups=True
            )

        for name in unseen:
            ts = name_index[name.casefold()]
            self.unknown_member(name, [t.line_no for t in ts])

        for name in duplicates:
            ts = name_index[name.casefold()]
            msg = _(
                '%(member_str)s designates multiple registered members. '
                'Skipped processing.',
            )
            self.error_at_lines(
                [t.line_no for t in ts], name, msg
            )

        def member_email_tuples():
            for m in member_email_qs:
                member_str = m.user.email
                yield (m, email_index[member_str], member_str)

        def member_name_tuples():
            for m in member_name_qs:
                member_str = m.full_name
                imember_str = member_str.casefold()
                if imember_str not in duplicates:
                    yield (m, name_index[imember_str], member_str)

        return list(chain(member_email_tuples(), member_name_tuples()))


class MiscDebtPaymentPopulator(FetchMembersMixin):
    DEBT_MISC_PREFIX = 'bulk-debt-misc'

    def __init__(self, cash_parser):
        super().__init__(cash_parser)
        if cash_parser is not None:
            cash_data = cash_parser.parsed_data
        else:
            cash_data = []
        member_list = self.process_member_identifiers(cash_data)

        # prepare duplicate checking
        historical_buckets = bucket_transaction_history(
            payments.PAYMENT_NATURE_CASH,
            cash_data
        )

        # TODO: reduce code duplication!
        # also compute total contribution to 
        # check for overpayments later
        self.debt_contributions = {}

        def make_initials(t):
            member, tinfos, member_str = t
            dupcheck = defaultdict(list)
            contribution_by_category = defaultdict(
                lambda: Money(Decimal('0.00'), settings.BOOKKEEPING_CURRENCY)
            )
            for tinfo in tinfos:
                if tinfo.amount.amount < 0:
                    self.error_at_line(
                        tinfo.line_no, member_str,
                        _('Payment amount %(amount)s is negative.'),
                        params={'amount': tinfo.amount}
                    )
                    continue
                k = bucket_key(member, tinfo)
                occ_so_far = dupcheck[k]
                occ_so_far.append(tinfo)
                # ok, we've DEFINITELY not seen this one before
                if len(occ_so_far) > historical_buckets[k]:
                    contribution_by_category[tinfo.debt_filter] += tinfo.amount
                    yield {
                        'nature': tinfo.nature,
                        'email': member.user.email,
                        'member_id': member.pk,
                        'name': member.full_name,
                        'total_amount': tinfo.amount,
                        'timestamp': _dt_fallback(tinfo.timestamp),
                        'debt_filter': tinfo.debt_filter
                    }

            # save to debt_contributions
            self.debt_contributions[member] = sum(
                contribution_by_category.values(),
                Money(Decimal('0.00'), settings.BOOKKEEPING_CURRENCY)
            )

            # report on possible duplicates
            do_dupcheck(
                self.error_at_lines,
                member, historical_buckets, dupcheck
            )

            # check for per-filter overpayment if we are running in filtered
            # mode
            filter_slugs = contribution_by_category.keys()
            if any(filter_slugs):
                tallies = member.debts.balances_by_filter_slug(
                    filter_slugs
                )
                for slug in filter_slugs:
                    total = contribution_by_category[slug]
                    balance = tallies[slug]
                    if total > balance:
                        lines = [
                            t.line_no for t in tinfos if t.debt_filter == slug
                        ]
                        self.error_at_lines(
                            lines, member_str, _(
                                'Member %(member_str)s overpaid debts in '
                                'category \'%(filter_slug)s\': balance is '
                                '%(balance)s, but received %(total)s. '
                            ), params={
                                'filter_slug': slug,
                                'balance': balance,
                                'total': total
                            }
                        )

        initial_data = list(chain(*map(make_initials, member_list)))
        self.debt_formset = BulkPaymentFormSet(
            queryset=models.InternalPayment.objects.none(),
            initial=initial_data,
            prefix=self.DEBT_MISC_PREFIX
        )
        self.debt_formset.extra = len(initial_data)


class AddDebtFormsetPopulator(FetchMembersMixin):

    def __init__(self, debt_parser):
        super().__init__(debt_parser)
        if debt_parser is not None:
            debt_data = debt_parser.parsed_data
        else:
            debt_data = []
        member_list = self.process_member_identifiers(debt_data)

        def make_initials(t):
            member, dinfos, member_str = t
            for dinfo in dinfos:
                if dinfo.amount.amount < 0:
                    self.error_at_line(
                        dinfo.line_no, member_str,
                        _('Debt amount %(amount)s is negative.'),
                        params={'amount': dinfo.amount}
                    )
                    continue
                yield {
                    'email': member.user.email,
                    'member_id': member.pk,
                    'name': member.full_name,
                    'total_amount': dinfo.amount,
                    'gnucash': dinfo.gnucash,
                    'comment': dinfo.comment,
                    'filter_slug': dinfo.filter_slug,
                    'timestamp': _dt_fallback(dinfo.timestamp)
                }

        initial_data = list(chain(*map(make_initials, member_list)))
        self.debt_formset = BulkAddDebtFormSet(
            queryset=models.InternalDebtItem.objects.none(),
            initial=initial_data
        )
        self.debt_formset.extra = len(initial_data)


class BaseBulkAddDebtFormSet(forms.BaseModelFormSet):

    def save(self, commit=True):

        def build_debt_objects():
            gnucash_cache = dict()
            for form in self.extra_forms:
                data = form.cleaned_data
                gnucash_raw = data['gnucash'].strip()
                try:
                    gnucash_category = gnucash_cache[gnucash_raw]
                except KeyError:
                    gnucash_category = models.GnuCashCategory.get_category(
                        gnucash_raw
                    )
                    gnucash_cache[gnucash_raw] = gnucash_category

                yield models.InternalDebtItem(
                    total_amount=data['total_amount'],
                    member_id=data['member_id'],
                    comment=data['comment'],
                    gnucash_category=gnucash_category,
                    filter_slug=data.get('filter_slug') or None,
                    timestamp=data['timestamp']
                )

        debts = list(build_debt_objects())
        if commit:
            models.InternalDebtItem.objects.bulk_create(debts)
        return debts


BulkAddDebtFormSet = modelformset_factory(
    model=models.InternalDebtItem,
    form=EphemeralAddDebtForm,
    formset=BaseBulkAddDebtFormSet
)


# This class can process both electronic transfers and
# cash payments
class BaseBulkPaymentFormSet(forms.BaseModelFormSet):

    # TODO: this kills deletion support, we should add that back in
    def save(self, commit=True):
        from django.db import connection
        can_bulk_save = connection.features.can_return_ids_from_bulk_insert
        payments_by_member = defaultdict(lambda: defaultdict(list))

        for form in self.extra_forms:
            data = form.cleaned_data
            member_id = data['member_id']
            # coerce falsy values (e.g. empty string) to None as a precaution
            debt_filter = data.get('debt_filter') or None
            payment = models.InternalPayment(
                total_amount=data['total_amount'],
                timestamp=data['timestamp'],
                nature=data['nature'],
                member_id=member_id
            )
            if commit and not can_bulk_save:
                payment.save()
            payments_by_member[member_id][debt_filter].append(payment)
        if commit and can_bulk_save:
            def all_payments():
                for payment_set in payments_by_member.values():
                    yield from chain(*iter(payment_set.values()))
            models.InternalPayment.objects.bulk_create(all_payments())
        member_qs = models.ChoirMember.objects.filter(
            pk__in=list(payments_by_member.keys())
        ).with_debt_annotations()

        # if filter strings appear anywhere, we assume that they appear
        # everywhere (the parser guarantees this)
        filtered_mode = any(
            any(payment_set.keys())
            for payment_set in payments_by_member.values()
        )

        def filtered(member):
            member_payments = payments_by_member[member.pk]
            all_debts = member.debts.unpaid().order_by('timestamp')
            debts_by_filter = defaultdict(list)
            for debt in all_debts:
                debts_by_filter[debt.filter_slug].append(debt)

            for filter_slug, pmts in member_payments.items():
                # we handle unrestricted payments later
                if filter_slug is None:
                    raise SuspiciousOperation(
                        '\'None\' filter slug found in filtered '
                        'payment import. This shouldn\'t happen without '
                        'tampering.'
                    )
                yield from payments.make_payment_splits(
                    payments=sorted(pmts, key=lambda p: p.timestamp),
                    debts=debts_by_filter[filter_slug]
                )

        def unfiltered(member):
            member_payments = payments_by_member[member.pk][None]
            return payments.make_payment_splits(
                payments=sorted(member_payments, key=lambda p: p.timestamp),
                debts=member.debts.unpaid().order_by('timestamp')
            )

        split_generator = filtered if filtered_mode else unfiltered
        splits_to_create = chain(
            *(split_generator(member) for member in member_qs)
        )

        if commit:
            models.InternalPaymentSplit.objects.bulk_create(splits_to_create)

        return payments


BulkPaymentFormSet = modelformset_factory(
    model=models.InternalPayment,
    form=EphemeralPaymentForm,
    formset=BaseBulkPaymentFormSet
)


# duplication heuristics
# Problem: the resolution of most banks' reporting is a day.
# Hence, we cannot use an exact timestamp as a cutoff point between
# imports, which would eliminate the need for duplicate checking in practice.
def extr_date(trans):
    ts = trans.timestamp
    if isinstance(ts, datetime.datetime):
        return timezone.localdate(ts)
    else:
        return ts


def bucket_key(member, trans):
    return member.pk, trans.amount.amount, extr_date(trans)


# TODO: allow this thing to cope with the fact that our cash csv function
# can now accommodate different payment natures
def bucket_transaction_history(nature, trans_data):
    historical_buckets = defaultdict(int)
    if not trans_data:
        return historical_buckets
    min_date = _dt_fallback(min(map(extr_date, trans_data)))
    max_date = _dt_fallback(max(map(extr_date, trans_data)))
    payment_history = models.InternalPayment.objects.filter(
        nature=nature,
        timestamp__gte=min_date,
        timestamp__lte=max_date
    )

    for p in payment_history:
        # p.timestamp is in UTC by default
        the_date = timezone.localdate(p.timestamp)
        historical_buckets[
            (p.member.pk, p.total_amount.amount, the_date)
        ] += 1
    return historical_buckets


MULTIPLE_DUP_MESSAGE = _(
    'A payment by %(member)s for amount %(amount)s '
    'on date %(date)s appears %(hist)d times '
    'in history, and %(import)d time(s) in '
    'the current batch of data. '
    'Resolution: %(dupcount)d ruled as duplicate(s).'
)

SINGLE_DUP_MESSAGE = _(
    'A payment by %(member)s for amount %(amount)s '
    'on date %(date)s already appears in the payment history. '
    'Resolution: likely duplicate, skipped processing.'
)


def do_dupcheck(errf, member, historical_buckets, dupcheck):
    for k, occ_in_import in dupcheck.items():
        occ_in_hist = historical_buckets[k]
        import_count = len(occ_in_import)
        if not occ_in_hist or not import_count:
            # is the latter even possible?
            continue
        trans = occ_in_import[0]  # representative sample
        params = {
            'member': member,
            'date': extr_date(trans),
            'amount': trans.amount,
            'hist': occ_in_hist,
            'import': import_count,
            'dupcount': min(occ_in_hist, import_count),
        }
        # special case, this is the most likely one to occur
        # so deserves special wording
        if occ_in_hist == import_count == 1:
            errf(
                [trans.line_no],
                None,
                SINGLE_DUP_MESSAGE,
                params
            ) 
        else:
            # we now know that occ_in_hist is at least 2,
            errf(
                [t.line_no for t in occ_in_import],
                None,
                MULTIPLE_DUP_MESSAGE,
                params
            )


class BulkPaymentUploadForm(CSVUploadForm):

    transfer_csv = forms.FileField(
        label=_('Electronic transfers (.csv)'),
        required=False,
    )

    misc_debt_csv = forms.FileField(
        label=_('Misc. internal debt payments (.csv)'),
        required=False,
    )

    # TODO: we should cut down on boilerplate by implementing a registration
    # mechanism. On the other hand, messing with django's form metaclasses
    # might prove nasty
    def clean_transfer_csv(self):
        return self._validate_csv('transfer_csv')

    def clean_misc_debt_csv(self):
        return self._validate_csv('misc_debt_csv')

    @property
    def csv_parser_classes(self):
        # TODO: make parser configurable in globals
        # (this is why I don't want to override FileField:
        #   we'd have to override it again to make this property dynamic)
        return {
            'transfer_csv': payments.FortisCSVParser,
            'misc_debt_csv': payments.MiscDebtPaymentCSVParser
        }


class BulkDebtUploadForm(CSVUploadForm):

    debt_csv = forms.FileField(
        label=_('Debt data (.csv)'),
        required=True,
    )

    def clean_debt_csv(self):
        return self._validate_csv('debt_csv')

    @property
    def csv_parser_classes(self):
        return {
            'debt_csv': payments.DebtCSVParser
        }


class InternalPaymentSplitFormSet(InlineTransactionSplitFormSet):

    def base_filter(self):
        qs = Q(member=self.instance.member)
        if isinstance(self.instance, models.InternalDebtItem):
            qs &= Q(timestamp__gte=self.instance.timestamp)
        elif isinstance(self.instance, models.InternalPayment):
            qs &= Q(timestamp__lte=self.instance.timestamp)
        else:
            raise TypeError
        return qs
