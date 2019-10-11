from collections import defaultdict

from django.shortcuts import render
from django.utils.translation import ugettext_lazy as _

from lukweb import payments, models
from lukweb.forms.accounting import internal, ticketing
from double_entry.forms import bulk_utils
from double_entry.forms.transfers import (
    TransferTransactionIndexBuilder,
    TransferPaymentPreparator,
)


class InternalDebtTransferIndexBuilder(TransferTransactionIndexBuilder):
    def base_query_set(self):
        return super().base_query_set().select_related('user')


class DebtTransferPaymentPreparator(TransferPaymentPreparator):

    formset_class = internal.BulkPaymentFormSet
    formset_prefix = 'bulk-debt-transfers'
    prefix_digit = payments.OGM_INTERNAL_DEBT_PREFIX
    transaction_party_model = models.ChoirMember

    def get_lookup_builders(self):
        return [
            InternalDebtTransferIndexBuilder(
                self, prefix_digit=self.prefix_digit
            )
        ]

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
        kwargs['member_id'] = member.pk
        kwargs['name'] = member.full_name
        kwargs['email'] = member.user.email
        kwargs['nature'] = payments.PAYMENT_NATURE_TRANSFER
        return kwargs

    def debts_for(self, debt_key):
        return self._debt_buckets[debt_key]


class ReservationTransferPaymentPreparator(TransferPaymentPreparator):

    formset_class = ticketing.ReservationPaymentFormSet
    formset_prefix = 'reservation-payment-transfers'
    prefix_digit = payments.OGM_RESERVATION_PREFIX
    transaction_party_model = models.Customer
    reservations_paid = None
    incomplete_payments = None

    def dup_error_params(self, signature_used):
        params = super().dup_error_params(signature_used)
        params['account'] = str(
            self.get_account(pk=signature_used.customer_id)
        )
        return params

    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        if kwargs is None:
            return None
        kwargs['method'] = models.PAYMENT_METHOD_PREPAID
        return kwargs

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        customer = transaction.ledger_entry.customer
        kwargs['customer_id'] = customer.pk
        kwargs['name'] = customer.name
        kwargs['email'] = customer.email
        kwargs['method'] = models.PAYMENT_METHOD_PREPAID
        return kwargs

    def review(self):
        super().review()
        fp_by_customer = defaultdict(list)
        pp_by_customer = defaultdict(list)
        for reservation in self.results.fully_paid_debts:
            fp_by_customer[reservation.owner].append(reservation)
        for reservation in self.results.remaining_debts:
            # the allocation methods cache matched_balance, even though
            # this information hasn't landed in the database yet.
            # we only generate warnings for partially paid reservations, i.e.
            # reservations for which some payment has been received, but
            # haven't been paid in full
            if reservation.unmatched_balance and reservation.matched_balance:
                pp_by_customer[reservation.owner].append(reservation)

        self.reservations_paid = fp_by_customer
        self.incomplete_payments = pp_by_customer


    def debts_for(self, debt_key):
        return self._debt_buckets[debt_key]


class BulkTransferUploadForm(bulk_utils.FinancialCSVUploadForm):
    ledger_preparator_classes = (
        DebtTransferPaymentPreparator, ReservationTransferPaymentPreparator
    )
    upload_field_label = _('Electronic transfers (.csv)')

    @property
    def csv_parser_class(self):
        financial_globals = models.FinancialGlobals.load()
        return financial_globals.bank_csv_parser_class

    def render_confirmation_page(self, request, context=None):
        context = context or {}
        internaldebt, reservations = self.formset_preparators
        context.update({
            'disable_margins': True,
            'internaldebt_proc_errors': internaldebt.errors,
            'internaldebt_formset': internaldebt.formset,
            'reservation_proc_errors': reservations.errors,
            'reservation_formset': reservations.formset,
            'reservations_paid': reservations.reservations_paid.items(),
            'reservations_incomplete': reservations.incomplete_payments.items()
        })

        return render(
            request, 'payments/process_bulk_transfers.html', context
        )