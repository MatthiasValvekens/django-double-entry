import logging

from django import forms
from django.db import transaction
from django.forms.models import ModelForm, modelformset_factory
from djmoney.forms import MoneyField

from ... import models
from ...tasks import dispatch_tickets

logger = logging.getLogger(__name__)

__all__ = [
    'ReservationPaymentForm', 'ReservationPaymentFormSet'
]

class ReservationPaymentForm(ModelForm):
    # mostly UI data again
    # TODO: this form needs reworking (post-accounting refactor)
    total_amount = MoneyField()
    event_name = forms.CharField()
    
    class Meta:
        model = models.ReservationPayment
        fields = ()

class BaseReservationPaymentFormSet(forms.BaseModelFormSet):
    def save(self, commit=True):
        reservation_data = {
            form.cleaned_data['reservation_id']:
                form.cleaned_data['payment_timestamp']
            for form in self.forms
        }
        reservation_ids = reservation_data.keys()
        if not reservation_ids:
            return []
        elif not commit:  # never called, but let's deal with it anyway
            return reservation_ids

        # first update payment records
        with transaction.atomic():
            queryset = models.Reservation.objects.filter(
                pk__in=reservation_ids,
                payment_timestamp=None
            )
            for r in queryset:
                r.payment_timestamp = reservation_data[r.pk]
                r.save()

        dispatch_tickets.delay(reservation_ids)
        logger.info(
            'Queued ticket issuance for '
            'reservation ids %s.' % reservation_ids
        )
        return reservation_ids


ReservationPaymentFormSet = modelformset_factory(
    model=models.ReservationPayment,
    form=ReservationPaymentForm,
    formset=BaseReservationPaymentFormSet,
    extra=0
)
