import logging
from collections import defaultdict
from decimal import Decimal
from itertools import chain
from typing import Generator, Tuple, Iterable

from django import forms
from django.forms.models import ModelForm, modelformset_factory

from ...models.accounting import base as accounting_base
from ...models.accounting.base import BaseDebtPaymentSplit

from . import bulk_utils, base
from ... import models
from ...tasks import dispatch_tickets

logger = logging.getLogger(__name__)

__all__ = [
    'ReservationPaymentForm', 'ReservationPaymentFormSet',
    'ReservationPaymentSplitFormSet'
]

class ReservationPaymentForm(ModelForm):
    customer_id = forms.IntegerField()
    name = forms.CharField(required=False)
    email = forms.EmailField(required=False)
    ogm = forms.CharField(max_length=21, required=False)

    class Meta:
        model = models.ReservationPayment
        fields = ('method', 'total_amount', 'timestamp', 'customer_id')

class BaseReservationPaymentFormSet(bulk_utils.BaseCreditApportionmentFormset):
    transaction_party_model = models.Customer

    def prepare_payment_instances(self) -> Tuple[
        Iterable[int], Iterable[accounting_base.BasePaymentRecord]
    ]:
        payments_by_customer = defaultdict(list)
        for form in self.extra_forms:
            # form.instance ignores the customer_id field
            data = form.cleaned_data
            payment = models.ReservationPayment(
                method=data['method'],
                total_amount=data['total_amount'],
                timestamp=data['timestamp'],
                customer_id=data['customer_id']
            )
            payment.spoof_matched_balance(Decimal('0.00'))
            payments_by_customer[payment.customer_id].append(payment)
        all_payments = list(
            chain(*payments_by_customer.values())
        )
        self._payments_by_customer = payments_by_customer
        return payments_by_customer.keys(), all_payments

    def generate_splits(self, party) -> Generator[
        BaseDebtPaymentSplit, None, bulk_utils.ApportionmentResult
    ]:
        relevant_payments = self._payments_by_customer[party.pk]
        return bulk_utils.make_payment_splits(
            payments=sorted(relevant_payments, key=lambda p: p.timestamp),
            debts=party.debts.unpaid().order_by('timestamp'),
            split_model=self.transaction_party_model.get_split_model()
        )

    def post_debt_update(self, fully_paid_debts, remaining_debts):
        reservation_ids = models.Reservation.objects.filter(
            debt__in=fully_paid_debts
        ).values_list('pk', flat=True)
        dispatch_tickets.delay(reservation_ids)
        logger.info(
            'Queued ticket issuance for '
            'reservation ids %s.' % reservation_ids
        )
        # TODO: do we want to automatically email people that didn't pay off
        #  all relevant debts?
        return reservation_ids


ReservationPaymentFormSet = modelformset_factory(
    model=models.ReservationPayment,
    form=ReservationPaymentForm,
    formset=BaseReservationPaymentFormSet,
    extra=0
)


class ReservationPaymentSplitFormSet(base.InlineTransactionSplitFormSet):
    transaction_party_model = models.Customer
