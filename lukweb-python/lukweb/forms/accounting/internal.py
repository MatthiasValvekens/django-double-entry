import datetime
import logging
from collections import defaultdict
from decimal import Decimal
from itertools import chain

from django import forms
from django.utils.text import slugify
from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db.models import Q
from django.forms.models import ModelForm, modelformset_factory
from django.utils import timezone
from django.utils.translation import (
    ugettext_lazy as _,
)
from djmoney.money import Money

from . import base, bulk_utils
from .bulk_utils import MemberTransactionParser, PaymentCSVParser
from .base import *
from .utils import GnuCashFieldMixin
from ..utils import ParserErrorMixin, CSVUploadForm
from ... import payments, models
from ...widgets import (
    DatalistInputWidget, MoneyWidget,
)

logger = logging.getLogger(__name__)

__all__ = [
    'EphemeralPaymentForm', 'EphemeralAddDebtForm',
    'MiscDebtPaymentPreparator', 'InternalDebtRecordPreparator',
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

class InternalDebtRecordPreparator(bulk_utils.FetchMembersMixin):
    formset_prefix = 'bulk-add-debt'
    formset_class = BulkAddDebtFormSet
    model = models.InternalDebtItem

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        kwargs['comment'] = transaction.comment
        kwargs['gnucash'] = transaction.gnucash
        kwargs['filter_slug'] = transaction.filter_slug
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
        return kwargs


# This class can process both electronic transfers and
# cash payments
class BaseBulkPaymentFormSet(forms.BaseModelFormSet):

    # TODO: this kills deletion support, we should add that back in
    def save(self, commit=True):
        from django.db import connection
        can_bulk_save = connection.features.can_return_ids_from_bulk_insert
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
                    debts=debts_by_filter[filter_slug],
                    split_model=models.InternalPaymentSplit
                )

        def unfiltered(member):
            member_payments = payments_by_member[member.pk][None]
            return payments.make_payment_splits(
                payments=sorted(member_payments, key=lambda p: p.timestamp),
                debts=member.debts.unpaid().order_by('timestamp'),
                split_model=models.InternalPaymentSplit
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


class MiscDebtPaymentCSVParser(PaymentCSVParser, MemberTransactionParser):
    delimiter = ';'
    nature_column_name = 'aard'
    filter_column_name = 'filter'
    filters_present = False

    class TransactionInfo(PaymentCSVParser.TransactionInfo, 
                          MemberTransactionParser.TransactionInfo):
        def __init__(self, *, debt_filter=None, **kwargs):
            super().__init__(**kwargs)
            self.debt_filter = debt_filter

    def get_nature(self, line_no, row): 
        nature = row.get(self.nature_column_name, payments.PAYMENT_NATURE_CASH)
        if nature in ('bank', 'overschrijving'):
            nature = payments.PAYMENT_NATURE_TRANSFER
        else:
            nature = payments.PAYMENT_NATURE_CASH
        return nature

    # required columns: lid, bedrag
    # optional columns: datum, aard, filter
    # filter column requires a value if supplied!
    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        try:
            debt_filter = slugify(row[self.filter_column_name])
            if not debt_filter:
                self.error(
                    line_no, _(
                        'You must supply a filter value for all payments in '
                        '\'Misc. internal debt payments\', or omit the '
                        '\'%(colname)s\' column entirely. '
                        'Skipped processing.'
                    ) % {'colname': self.filter_column_name}
                )
                return None
            else:
                parsed['debt_filter'] = debt_filter
                self.filters_present = True
        except KeyError:
            # proceed as normal
            pass

        return parsed


class MiscDebtPaymentPreparator(bulk_utils.FetchMembersMixin,
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
        self.filtered_mode = parser.filters_present

    def dup_error_params(self, signature_used):
        # TODO: don't use magic numbers that depend on the order of
        # dupcheck_signature_fields on the model
        params = super().dup_error_params()
        # get human-readable value
        params['nature'] = models.InternalPayments.PAYMENT_NATURE_CHOICES[
            signature_used[2] - 1
        ]
        params['member'] = str(self.get_member(pk=signature_used[3]))
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
            return _(
                'Not all payments of %(member)s earmarked for '
                'category \'%(filter_slug)s\' can be fully utilised. '
                'Received %(total_credit)s, but only %(total_used)s '
                'can be applied to outstanding debts.'
            )
        else:
            return _(
                'Not all payments of %(member)s can be fully utilised. '
                'Received %(total_credit)s, but only %(total_used)s '
                'can be applied to outstanding debts.'
            )

    def overpayment_error_params(self, debt_key, total_used, total_credit): 
        if self.filtered_mode:
            member_id, filter_slug = debt_key
            return {
                'member': str(self.get_member(pk=member_id)),
                'filter_slug': filter_slug,
                'total_used': total_used,
                'total_credit': total_credit
            }
        else:
            return {
                'member': str(self.get_member(pk=debt_key)),
                'total_used': total_used,
                'total_credit': total_credit
            }

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
            member_id__in=self.member_ids()
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
        from .transfers import FortisCSVParser
        # TODO: make parser configurable in globals
        # (this is why I don't want to override FileField:
        #   we'd have to override it again to make this property dynamic)
        return {
            'transfer_csv': FortisCSVParser,
            'misc_debt_csv': MiscDebtPaymentCSVParser
        }


class DebtCSVParser(MemberTransactionParser):
    delimiter = ';'

    comment_column_name = 'mededeling'
    gnucash_column_name = 'gnucash'

    class TransactionInfo(MemberTransactionParser.TransactionInfo): 
        def __init__(self, *, comment, gnucash, filter_slug, **kwargs):
            super().__init__(**kwargs)
            self.comment = comment
            self.gnucash = gnucash
            self.filter_slug = filter_slug

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        parsed['comment'] = row[self.comment_column_name]
        parsed['gnucash'] = row[self.gnucash_column_name]
        # coerce falsy values
        parsed['filter_slug'] = slugify(row.get('filter', '')) or None
        return parsed


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
            'debt_csv': DebtCSVParser
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
