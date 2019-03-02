import logging
from typing import Iterable

from django.shortcuts import render
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, ugettext,
)

from ...models.accounting.base import TransactionPartyMixin
from . import internal, bulk_utils
from ... import payments, models

logger = logging.getLogger(__name__)

__all__ = ['BulkTransferUploadForm']


# TODO: case-insensitive column names
# TODO: clearly document parsers
# TODO: delimiter autodetection


# lookbehind doesn't work, since we don't want to constrain the
# prefix to a fixed length

class TransferTransactionIndexBuilder(bulk_utils.TransactionPartyIndexBuilder):

    prefix_digit = None

    def __init__(self, ledger_preparator, prefix_digit):
        super().__init__(ledger_preparator)
        self.prefix_digit = prefix_digit

    @classmethod
    def lookup_key_for_account(cls, account):
        return account.payment_tracking_no

    def ogm_applies(self, ogm):
        try:
            prefix, modulus = payments.parse_ogm(ogm)
            return self.prefix_digit == str(prefix)[0]
        except ValueError:
            return False

    def append(self, tinfo):
        string = tinfo.account_lookup_str
        if not self.ogm_applies(string):
            return False
        else:
            self.transaction_index[string].append(tinfo)
            return True

    def execute_query(self) -> Iterable[TransactionPartyMixin]:
        account_qs, unseen = self.ledger_preparator.transaction_party_model \
            ._default_manager.with_debt_balances().select_related('user') \
            .by_payment_tracking_nos(
            self.transaction_index.keys(), validate_unseen=True
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
        return account_qs


class TransferPaymentPreparator(bulk_utils.StandardCreditApportionmentMixin,
                                bulk_utils.DuplicationProtectedPreparator):

    prefix_digit = None

    def get_lookup_builders(self):
        return [
            TransferTransactionIndexBuilder(
                self, prefix_digit=self.prefix_digit
            )
        ]

    multiple_dup_message = _(
        'A bank transfer payment by %(account)s '
        'for amount %(amount)s on date %(date)s appears %(hist)d time(s) '
        'in history, and %(import)d time(s) in '
        'the current batch of data. '
        'Resolution: %(dupcount)d ruled as duplicate(s).'
    )

    single_dup_message = _(
        'A bank transfer payment by %(account)s '
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
        params['member'] = str(self._by_id[debt_key])
        return params

    @cached_property
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

class DebtTransferPaymentPreparator(TransferPaymentPreparator):

    formset_class = internal.BulkPaymentFormSet
    formset_prefix = 'bulk-debt-transfers'
    prefix_digit = payments.OGM_INTERNAL_DEBT_PREFIX
    transaction_party_model = models.ChoirMember

    def dup_error_params(self, signature_used):
        params = super().dup_error_params(signature_used)
        params['account'] = str(self.get_account(pk=signature_used.member_id))
        return params

    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        if kwargs is None:
            return None
        kwargs['nature'] = payments.PAYMENT_NATURE_TRANSFER
        return kwargs

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        member = transaction.ledger_entry.member
        kwargs['ogm'] = transaction.account_lookup_str
        kwargs['member_id'] = member.pk
        kwargs['name'] = member.full_name
        kwargs['email'] = member.user.email
        kwargs['nature'] = payments.PAYMENT_NATURE_TRANSFER
        return kwargs

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
