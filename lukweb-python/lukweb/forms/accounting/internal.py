import logging
import re
from collections import defaultdict
from decimal import Decimal
from itertools import chain
from typing import Tuple, Iterable

from django import forms
from django.core.exceptions import SuspiciousOperation
from django.forms.models import ModelForm, modelformset_factory
from django.shortcuts import render
from django.utils.translation import (
    ugettext_lazy as _, ugettext
)

from lukweb.models.accounting import base as accounting_base
from lukweb.models.accounting.base import TransactionPartyMixin
from . import bulk_utils
from .base import *
from .bulk_utils import (
    make_payment_splits,
    ApportionmentResult,
)
from ...payments import (DebtCSVParser, MiscDebtPaymentCSVParser)
from .utils import GnuCashFieldMixin
from ... import models
from ...widgets import (
    DatalistInputWidget, MoneyWidget,
)

logger = logging.getLogger(__name__)

__all__ = [
    'BulkPaymentUploadForm', 'BulkDebtUploadForm',
    'ProfileAddDebtForm', 'InternalPaymentSplitFormSet'
]


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
        # we don't include the member field as such; this is intentional
        # since we don't want the member records to be fetched at
        # the form level. Two reasons:
        #  (a) inefficient as fuck
        #  (b) we need to query them together with the debt data
        #      to generate the splits
        fields = ('nature', 'total_amount', 'timestamp', 'member_id')


class EphemeralAddDebtForm(ModelForm):
    member_id = forms.IntegerField()
    gnucash = forms.CharField()
    name = forms.CharField(required=False)
    email = forms.EmailField(required=False)
    filter_slug = forms.SlugField(required=False)
    activity_id = forms.IntegerField(required=False)

    class Meta:
        model = models.InternalDebtItem
        fields = ('comment', 'total_amount', 'timestamp', 'member_id')


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


class BaseBulkAddDebtFormSet(forms.BaseModelFormSet):

    def save(self, commit=True):

        def build_debt_objects():
            gnucash_cache = dict()
            by_activity_id = defaultdict(list)
            for form in self.extra_forms:
                activity_id = form.cleaned_data['activity_id']
                if activity_id is None:
                    continue
                by_activity_id[activity_id].append(
                    form.cleaned_data['member_id']
                )
            _actreg_index, notfound = lookup_act_registration_ids(
                by_activity_id
            )
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

                activity_id = data['activity_id']
                member_id = data['member_id']
                if activity_id is not None:
                    try:
                        actreg_id = _actreg_index[(activity_id, member_id)]
                    except KeyError:
                        # by all accounts these debt records shouldn't be saved
                        #  since they would have been deleted if the activity
                        #  participation deletion action would have occurred
                        #  after saving the debt
                        logger.warning(
                            'Could not find activity participation record '
                            'for activity with id %(act)d and member with '
                            'id %(member)d. '
                            'Possible race condition; debt record was NOT '
                            'saved.', {
                                'member': member_id, 'act': activity_id
                            }
                        )
                        continue
                else:
                    actreg_id = None

                yield models.InternalDebtItem(
                    total_amount=data['total_amount'],
                    member_id=member_id,
                    comment=data['comment'],
                    gnucash_category=gnucash_category,
                    filter_slug=data.get('filter_slug') or None,
                    timestamp=data['timestamp'],
                    activity_participation_id=actreg_id,
                )

        from django.db import transaction
        with transaction.atomic():
            debts = list(build_debt_objects())
            if commit:
                models.InternalDebtItem.objects.bulk_create(debts)
        return debts


BulkAddDebtFormSet = modelformset_factory(
    model=models.InternalDebtItem,
    form=EphemeralAddDebtForm,
    formset=BaseBulkAddDebtFormSet
)

def lookup_act_registration_ids(member_ids_by_activity: dict):
    from functools import reduce
    from operator import or_
    if not member_ids_by_activity:
        return models.ActivityParticipation.objects.none(), set()

    qs_iter = (
        models.ActivityParticipation.objects.filter(
            activity_id=activity_id, member_id__in=member_ids
        ) for activity_id, member_ids in member_ids_by_activity.items()
    )
    reg_objs = reduce(or_, qs_iter).values('activity_id', 'member_id', 'id')

    result = {
        (r['activity_id'], r['member_id']): r['id'] for r in reg_objs
    }
    all_pairs = {
        (activity_id, member_id)
        for activity_id, member_ids in member_ids_by_activity.items()
        for member_id in member_ids
    }
    unseen = all_pairs - result.keys()
    return result, unseen


UID_FORMAT = re.compile(
    r'(?P<uid>\d+)(:(?P<token>\d+-[a-z0-9]+-[0-9a-f]{20})'
    ':(?P<salt>[-_A-Za-z0-9]+))?'
)

class ByEmailIndexBuilder(bulk_utils.TransactionPartyIndexBuilder):

    @classmethod
    def lookup_key_for_account(cls, account):
        return account.user.email

    def append(self, tinfo):
        string = tinfo.account_lookup_str
        if '@' in string:
            self.transaction_index[string].append(tinfo)
            return True
        else:
            return False

    def execute_query(self) -> Iterable[TransactionPartyMixin]:
        member_email_qs, unseen = models.ChoirMember.objects \
            .select_related('user').with_debt_annotations().by_emails(
            self.transaction_index.keys(), validate_unseen=True
        )

        for email in unseen:
            ts = self.transaction_index[email]
            self.ledger_preparator.unknown_account(
                email, [t.line_no for t in ts]
            )
        return member_email_qs


class ByNameIndexBuilder(bulk_utils.TransactionPartyIndexBuilder):

    @classmethod
    def lookup_key_for_account(cls, account):
        return account.full_name

    def append(self, tinfo):
        # this method isn't picky
        string = tinfo.account_lookup_str
        self.transaction_index[string].append(tinfo)
        return True

    def execute_query(self) -> Iterable[TransactionPartyMixin]:
        member_name_qs, unseen, duplicates = models.ChoirMember.objects \
            .select_related('user').with_debt_annotations().by_full_names(
            self.transaction_index.keys(),
            validate_unseen=True, validate_nodups=True
        )

        for name in unseen:
            ts = self.transaction_index[name.casefold()]
            self.ledger_preparator.unknown_account(name, [t.line_no for t in ts])

        for name in duplicates:
            ts = self.transaction_index[name.casefold()]
            self.ledger_preparator.ambiguous_account(
                name, [t.line_no for t in ts]
            )

        return member_name_qs

class ByUIDIndexBuilder(bulk_utils.TransactionPartyIndexBuilder):

    @classmethod
    def lookup_key_for_account(cls, party):
        return str(party.pk)

    def append(self, tinfo):
        string = tinfo.account_lookup_str
        match = UID_FORMAT.match(string)
        if match is None:
            return False
        # if the validation token is left out, we trust that
        # the operator knows what they're doing
        token = match.group('token')
        uid_str = match.group('uid')
        uid = int(uid_str)
        if token is not None:
            self.transaction_index[uid].append(
                (tinfo, (token, match.group('salt')))
            )
        else:
            self.transaction_index[uid].append((tinfo, None))
        # save canonical version of the lookup str
        tinfo.account_lookup_str = uid_str
        return True

    def execute_query(self) -> Iterable[TransactionPartyMixin]:
        member_uid_qs = models.ChoirMember.objects \
            .select_related('user').with_debt_annotations().filter(
            pk__in=self.transaction_index.keys()
        )
        for m in member_uid_qs:
            tinfos = self.transaction_index[m.pk]
            for info, validation in tinfos:
                if validation is not None:
                    token, salt = validation
                    token_valid = m.validate_external_uid_token(
                        external_form_salt=salt, bare_token=token
                    )
                    if not token_valid:
                        self.ledger_preparator.error_at_line(
                            info.line_no,
                            _(
                                'Token %(token)s is invalid for uid %(uid)d '
                                'with salt value %(salt)s. Skipped processing.'
                            ), params={
                                'token': token, 'uid': m.pk, 'salt': salt,
                            }
                        )
                        # This will cause the transaction to be eliminated
                        # during the ledger preparation stage
                        info.account_lookup_str = None

        unseen_uids = self.transaction_index.keys() - set(
            m.pk for m in member_uid_qs
        )
        for pk in unseen_uids:
            ts = self.transaction_index[pk]
            self.ledger_preparator.unknown_account(
                str(pk), [t.line_no for t, v in ts]
            )
        return member_uid_qs


class FetchMembersMixin(bulk_utils.FetchTransactionAccountsMixin):
    transaction_account_model = models.ChoirMember

    unknown_account_message = _(
        '%(account)s does not designate a registered member.'
    )

    ambiguous_account_message = _(
        '%(account)s designates multiple registered members. '
        'Skipped processing.',
    )

    def get_lookup_builders(self):
        return [
            ByUIDIndexBuilder(self),
            ByEmailIndexBuilder(self),
            ByNameIndexBuilder(self)
        ]

    # TODO: In the long term I would like to get rid of these eph
    # forms as well. That should be a bit easier to plan with the
    # new bulk_utils module.
    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        member = transaction.ledger_entry.member
        kwargs['member_id'] = member.pk
        kwargs['name'] = member.full_name
        kwargs['email'] = member.user.email
        return kwargs

    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        if kwargs is None:
            return None
        try:
            member = self._by_lookup_str[transaction.account_lookup_str]
            kwargs['member'] = member
            return kwargs
        except KeyError:
            # member search errors have already been logged
            # in the preparation step, so we don't care
            return None


class InternalDebtRecordPreparator(FetchMembersMixin):
    formset_prefix = 'bulk-add-debt'
    formset_class = BulkAddDebtFormSet
    model = models.InternalDebtItem
    _actreg_index = None

    def prepare(self):
        super().prepare()
        by_activity_id = defaultdict(list)
        for info in self.transactions:
            if info.activity_id is None:
                continue
            mem = self.get_account(lookup_str=info.account_lookup_str)
            by_activity_id[info.activity_id].append(mem.pk)
        self._actreg_index, notfound = lookup_act_registration_ids(
            by_activity_id
        )
        for activity_id, member_id in notfound:
            # This function is expert-only, so screw properly formatted errors
            self.error_at_line(
                0, _(
                    'Member %(member)s with id %(member_id)d appears not to '
                    'be registered for activity with id %(act_id)d.'
                ), params={
                    'member': self.get_account(pk=member_id),
                    'member_id': member_id,
                    'act_id': activity_id
                }
            )

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        kwargs['comment'] = transaction.comment
        kwargs['gnucash'] = transaction.gnucash
        kwargs['filter_slug'] = transaction.filter_slug
        # we pass this back to the form. Passing the registration id directly
        #  is problematic because of inter-request race conditions.
        # (it's conceivable that a user might deregister inbetween requests)
        kwargs['activity_id'] = transaction.activity_id
        return kwargs

    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        if kwargs is None:
            return None
        kwargs['comment'] = transaction
        kwargs['gnucash_category'] = models.GnuCashCategory.get_category(
            transaction.gnucash, create=False
        )
        kwargs['filter_slug'] = transaction.filter_slug
        if transaction.activity_id is not None:
            try:
                kwargs['activity_participation_id'] = self._actreg_index[
                    (transaction.activity_id, kwargs['member'].pk)
                ]
            except KeyError:
                return None
        return kwargs


# This class can process both electronic transfers and
# cash payments
class BaseBulkPaymentFormSet(bulk_utils.BaseCreditApportionmentFormset):
    transaction_party_model = models.ChoirMember

    def prepare_payment_instances(self) -> Tuple[
        Iterable[int], Iterable[accounting_base.BasePaymentRecord]
    ]:
        payments_by_member = defaultdict(lambda: defaultdict(list))

        filtered_mode = None
        for form in self.extra_forms:
            data = form.cleaned_data
            member_id = data['member_id']
            # coerce falsy values (e.g. empty string) to None as a precaution
            debt_filter = data.get('debt_filter') or None
            # if filter strings appear anywhere, we assume that they appear
            # everywhere (the parser guarantees this)
            # Hence, this if statement is strictly speaking unnecessary
            if filtered_mode is None:
                filtered_mode = debt_filter is not None
            payment = models.InternalPayment(
                total_amount=data['total_amount'],
                timestamp=data['timestamp'],
                nature=data['nature'],
                member_id=member_id
            )
            payment.spoof_matched_balance(Decimal('0.00'))
            payments_by_member[member_id][debt_filter].append(payment)

        self._payments_by_member = payments_by_member
        self.filtered_mode = bool(filtered_mode)

        def all_payments():
            for payment_set in payments_by_member.values():
                yield from chain(*iter(payment_set.values()))

        return payments_by_member.keys(), list(all_payments())

    def generate_filtered_splits(self, member):
        all_results = ApportionmentResult()
        member_payments = self._payments_by_member[member.pk]
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
            results = yield from make_payment_splits(
                payments=sorted(pmts, key=lambda p: p.timestamp),
                debts=debts_by_filter[filter_slug],
                split_model=models.InternalPaymentSplit
            )
            all_results += results

        return all_results

    def generate_unfiltered_splits(self, member):
        member_payments = self._payments_by_member[member.pk][None]
        return make_payment_splits(
            payments=sorted(member_payments, key=lambda p: p.timestamp),
            debts=member.debts.unpaid().order_by('timestamp'),
            split_model=models.InternalPaymentSplit
        )

    def generate_splits(self, party):
        if self.filtered_mode:
            return self.generate_filtered_splits(party)
        else:
            return self.generate_unfiltered_splits(party)


BulkPaymentFormSet = modelformset_factory(
    model=models.InternalPayment,
    form=EphemeralPaymentForm,
    formset=BaseBulkPaymentFormSet
)


class MiscDebtPaymentPreparator(FetchMembersMixin,
                                bulk_utils.DuplicationProtectedPreparator,
                                bulk_utils.CreditApportionmentMixin):

    formset_prefix = 'bulk-debt-misc'
    formset_class = BulkPaymentFormSet
    split_model = models.InternalPaymentSplit
    model = models.InternalPayment

    _debt_buckets = None

    multiple_dup_message = _(
        'A payment of nature \'%(nature)s\' by %(member)s '
        'for amount %(amount)s on date %(date)s appears %(hist)d time(s) '
        'in history, and %(import)d time(s) in '
        'the current batch of data. '
        'Resolution: %(dupcount)d ruled as duplicate(s).'
    )

    single_dup_message = _(
        'A payment of nature \'%(nature)s\' by %(member)s '
        'for amount %(amount)s on date %(date)s already appears '
        'in the payment history. '
        'Resolution: likely duplicate, skipped processing.'
    )

    def __init__(self, parser):
        super().__init__(parser)
        self.filtered_mode = (
            parser.filters_present if parser is not None else False
        )

    def dup_error_params(self, signature_used):
        # TODO: don't use magic numbers that depend on the order of
        # dupcheck_signature_fields on the model
        params = super().dup_error_params(signature_used)
        # get human-readable value
        nature_choices = dict(models.InternalPayment.PAYMENT_NATURE_CHOICES)
        params['nature'] = nature_choices[signature_used.nature]
        params['member'] = str(self.get_account(pk=signature_used.member_id))
        return params

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        kwargs['nature'] = transaction.nature
        kwargs['debt_filter'] = transaction.debt_filter
        return kwargs

    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        if kwargs is None:
            return None
        kwargs['nature'] = transaction.nature
        return kwargs

    @property
    def overpayment_fmt_string(self):
        if self.filtered_mode:
            specific = ugettext(
                'Not all payments of %(member)s earmarked for '
                'category \'%(filter_slug)s\' can be fully utilised.'
            )
        else:
            specific = ugettext(
                'Not all payments of %(member)s can be fully utilised.'
            )

        return ' '.join(
            (
                specific,
                str(bulk_utils.CreditApportionmentMixin.overpayment_fmt_string),
                self.refund_message
             )
        )

    @property
    def refund_message(self):
        financial_globals = models.FinancialGlobals.load()
        refund_category = financial_globals.refund_credit_gnucash_acct
        autogenerate_refunds = financial_globals.autogenerate_refunds
        if autogenerate_refunds and refund_category is None:
            return _(
                'Refund records cannot be created because the '
                'corresponding setting in the financial globals is not '
                'properly configured.'
            )
        else:
            return super().refund_message

    def overpayment_error_params(self, debt_key, *args):

        params = super().overpayment_error_params(debt_key, *args)
        if self.filtered_mode:
            member_id, filter_slug = debt_key
            params['member'] = str(self.get_account(pk=member_id))
            params['filter_slug'] = filter_slug
        else:
            params['member'] = str(self.get_account(pk=debt_key))
        return params

    def transaction_buckets(self):
        # if there are no filters present, we can simply go by
        # member ID
        
        # in filtered mode, we have to work a little harder
        trans_buckets = defaultdict(list)
        filters_involved = []
        sep_filters = self.filtered_mode
        for t in self.valid_transactions:
            member_id = t.ledger_entry.member.pk
            if sep_filters:
                trans_buckets[(member_id, t.debt_filter)].append(t)
                filters_involved.append(t.debt_filter)
            else:
                trans_buckets[member_id].append(t)

        debt_qs = models.InternalDebtItem.objects.filter(
            member_id__in=self.account_ids()
        ).with_payments().unpaid().order_by('timestamp')

        debt_buckets = defaultdict(list)
        if sep_filters:
            debt_qs.filter(filter_slug__in=filters_involved)
            for debt in debt_qs:
                debt_buckets[(debt.member_id, debt.filter_slug)].append(debt)
        else:
            for debt in debt_qs:
                debt_buckets[debt.member_id].append(debt)
        
        self._debt_buckets = debt_buckets

        return trans_buckets 
    
    def debts_for(self, debt_key):
        return self._debt_buckets[debt_key]


class BulkDebtUploadForm(bulk_utils.FinancialCSVUploadForm):
    ledger_preparator_classes = (InternalDebtRecordPreparator,)
    csv_parser_class = DebtCSVParser
    upload_field_label = _('Debt data (.csv)')

    def render_confirmation_page(self, request, context=None):
        context = context or {}
        prep = self.formset_preparator
        context.update({
            'disable_margins': True,
            'debt_proc_errors': prep.errors,
            'formset': prep.formset,
            
        })

        return render(
            request, 'payments/process_bulk_debts.html', context
        )

class BulkPaymentUploadForm(bulk_utils.FinancialCSVUploadForm):
    ledger_preparator_classes = (MiscDebtPaymentPreparator,)
    csv_parser_class = MiscDebtPaymentCSVParser
    upload_field_label = _('Misc. internal debt payments (.csv)')

    def render_confirmation_page(self, request, context=None):
        context = context or {}
        prep = self.formset_preparator
        context.update({
            'disable_margins': True,
            'misc_debt_proc_errors': prep.errors,
            'misc_debt_formset': prep.formset,
        })

        return render(
            request, 'payments/process_bulk_payments.html', context
        )


class InternalPaymentSplitFormSet(InlineTransactionSplitFormSet):
    transaction_party_model = models.ChoirMember


def recompute_payment_splits(payments, debt_filter=None, **kwargs):
    """
    WARNING: this function is not intended for normal usage
    and typically destroys .qif consistency. Use only if you know
    what you're doing.
    """
    from ...models import InternalPaymentSplit, ChoirMember
    from ...models.accounting.base import DoubleBookQuerySet
    from django.db import transaction

    payments_by_member = defaultdict(list)
    for p in payments:
        # saves us two completely pointless queries later
        payments_by_member[p.member_id].append(p)
        setattr(p, DoubleBookQuerySet.MATCHED_BALANCE_FIELD, Decimal('0.00'))

    members = ChoirMember.objects.filter(
        pk__in=payments_by_member.keys()
    ).with_debts_and_payments()

    with transaction.atomic():
        # delete old splits
        InternalPaymentSplit.objects.filter(
            payment_id__in=[p.pk for p in payments]
        ).delete()

        def splits_to_create():
            for member in members:
                # pull up unpaid debts
                debts = member.debts.unpaid().order_by('timestamp')
                if debt_filter is not None:
                    debts = debts.filter(debt_filter)
                yield from make_payment_splits(
                    payments_by_member[member.pk], debts,
                    InternalPaymentSplit, **kwargs
                )
        InternalPaymentSplit.objects.bulk_create(splits_to_create())
