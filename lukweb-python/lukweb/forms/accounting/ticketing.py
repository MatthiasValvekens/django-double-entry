import logging

from django import forms
from django.db import transaction
from django.forms.models import ModelForm, modelformset_factory
from djmoney.forms import MoneyField

from lukweb import models
from lukweb.tasks import dispatch_tickets

logger = logging.getLogger(__name__)

__all__ = [
    'ReservationPaymentForm', 'ReservationPaymentFormSet'
]


class TicketForm(ModelForm): 

    class Meta:
        model = models.Ticket
        fields = ('amount',)

    def save(self, commit=True):
        instance = super(TicketForm, self).save(commit=False)
        instance.reservation = self._reservation
        instance.category = self.category
        if instance.amount == 0:
            return instance
        if commit:
            instance.save()
        return instance


class ReservationPaymentForm(ModelForm):
    # mostly UI data again
    total_amount = MoneyField()
    event_name = forms.CharField()
    
    class Meta:
        model = models.Reservation
        fields = (
            'name', 'email', 'payment_timestamp'
        )

    def __init__(self, *args, instance=None, **kwargs):
        if instance:
            initial = kwargs.get('initial', {})
            initial['total_amount'] = instance.total_price
            initial['event_name'] = instance.event.name
            kwargs['initial'] = initial
        super(ReservationPaymentForm, self).__init__(
            *args, instance=instance, **kwargs
        )


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
    model=models.Reservation,
    form=ReservationPaymentForm,
    formset=BaseReservationPaymentFormSet,
    extra=0
)
