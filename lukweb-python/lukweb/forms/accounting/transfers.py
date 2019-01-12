import logging
from collections import defaultdict

from django.conf import settings
from django.shortcuts import render
from django.utils.translation import (
    ugettext_lazy as _, ugettext,
)

from . import internal, bulk_utils
from ... import payments, models

logger = logging.getLogger(__name__)

__all__ = ['BulkTransferUploadForm']


# TODO: implement parser switching in globals
# TODO: case-insensitive column names
# TODO: clearly document parsers
# TODO: delimiter autodetection


# lookbehind doesn't work, since we don't want to constrain the
# prefix to a fixed length


class TransferRecordPreparator(bulk_utils.LedgerEntryPreparator):
    
    prefix_digit = None

    def ogm_applies(self, ogm):
        try:
            prefix, modulus = payments.parse_ogm(ogm)
            return self.prefix_digit == str(prefix)[0]
        except ValueError:
            return False

    def model_kwargs_for_transaction(self, transaction):
        if not self.ogm_applies(transaction.ogm):
            return None
        return super().model_kwargs_for_transaction(transaction)


class DebtTransferPaymentPreparator(TransferRecordPreparator,
                                    bulk_utils.DuplicationProtectedPreparator,
                                    bulk_utils.CreditApportionmentMixin):

    model = models.InternalPayment
    formset_class = internal.BulkPaymentFormSet
    split_model = models.InternalPaymentSplit
    formset_prefix = 'bulk-debt-transfers'
    prefix_digit = payments.OGM_INTERNAL_DEBT_PREFIX

    multiple_dup_message = _(
        'A bank transfer payment by %(member)s '
        'for amount %(amount)s on date %(date)s appears %(hist)d time(s) '
        'in history, and %(import)d time(s) in '
        'the current batch of data. '
        'Resolution: %(dupcount)d ruled as duplicate(s).'
    )

    single_dup_message = _(
        'A bank transfer payment by %(member)s '
        'for amount %(amount)s on date %(date)s already appears '
        'in the payment history. '
        'Resolution: likely duplicate, skipped processing.'
    )

    @property
    def overpayment_fmt_string(self):

        return ' '.join(
            (
                ugettext(
                    'Not all bank transfer payments of %(member)s '
                    'can be fully utilised.'
                ),
                str(bulk_utils.CreditApportionmentMixin.overpayment_fmt_string),
                self.refund_message
            )
        )

    def overpayment_error_params(self, debt_key, *args):
        params = super().overpayment_error_params(debt_key, *args)
        params['member'] = str(self._members_by_id[debt_key])
        return params

    @property
    def refund_message(self):
        financial_globals = models.FinancialGlobals.load()
        refund_category = financial_globals.refund_credit_gnucash_acct
        if settings.AUTOGENERATE_REFUNDS and refund_category is None:
            return _(
                'Refund records cannot be created because the '
                'corresponding setting in the financial globals is not '
                'properly configured.'
            )
        else:
            return super().refund_message

    def dup_error_params(self, signature_used):
        # TODO: don't use magic numbers that depend on the order of
        # dupcheck_signature_fields on the model
        params = super().dup_error_params(signature_used)
        params['member'] = str(self._members_by_id[signature_used[3]])
        return params

    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        if kwargs is None:
            return None
        pk = payments.parse_internal_debt_ogm(transaction.ogm)
        member = self._members_by_id[pk]
        # the pk part might match accidentally
        # so we check the hidden token digest too.
        # This shouldn't really matter all that much 
        # in the current implementation, but it can't hurt.
        if transaction.ogm != member.payment_tracking_no:
            return None
        kwargs['member'] = member
        kwargs['nature'] = payments.PAYMENT_NATURE_TRANSFER
        return kwargs

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        member = transaction.ledger_entry.member
        kwargs['ogm'] = transaction.ogm
        kwargs['member_id'] = member.pk
        kwargs['name'] = member.full_name
        kwargs['email'] = member.user.email
        kwargs['nature'] = payments.PAYMENT_NATURE_TRANSFER
        return kwargs 

    def prepare(self):
        ogms_to_query = [
            t.ogm for t in self.transactions if self.ogm_applies(t.ogm)
        ]

        member_qs, unseen = models.ChoirMember.objects.with_debt_balances()\
            .select_related('user').by_payment_tracking_nos(
                ogms_to_query, validate_unseen=True
            )

        # We don't show this error in the interface, since
        # if the OGM validates properly AND is not found in our system,
        # it probably simply corresponds to a transaction that we don't
        # care about
        if unseen:
            logger.debug(
                'OGMs not corresponding to valid user records: %s.', 
                ', '.join(unseen)
            )

        self._members_by_id = {
            member.pk: member for member in member_qs
        }

    def transaction_buckets(self):
        trans_buckets = defaultdict(list)
        for t in self.valid_transactions:
            member_id = t.ledger_entry.member.pk
            trans_buckets[member_id].append(t)
        debt_qs = models.InternalDebtItem.objects.filter(
            member_id__in=self._members_by_id.keys()
        ).with_payments().unpaid().order_by('timestamp')

        debt_buckets = defaultdict(list)
        for debt in debt_qs:
            debt_buckets[debt.member_id].append(debt)

        self._debt_buckets = debt_buckets

        return trans_buckets

    def debts_for(self, debt_key):
        return self._debt_buckets[debt_key]


class BulkTransferUploadForm(bulk_utils.FinancialCSVUploadForm):
    ledger_preparator_classes = (DebtTransferPaymentPreparator,)
    upload_field_label = _('Electronic transfers (.csv)')

    @property
    def csv_parser_class(self):
        financial_globals = models.FinancialGlobals.load()
        return financial_globals.bank_csv_parser_class

    def render_confirmation_page(self, request, context=None):
        context = context or {}
        # TODO: reservation stuff will wind up here
        internaldebt, = self.formset_preparators
        context.update({
            'disable_margins': True,
            'internaldebt_proc_errors': internaldebt.errors,
            'internaldebt_formset': internaldebt.formset,
        })

        return render(
            request, 'payments/process_bulk_transfers.html', context
        )
