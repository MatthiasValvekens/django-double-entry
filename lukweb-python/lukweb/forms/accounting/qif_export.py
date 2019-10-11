from django import forms
from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.translation import ugettext_lazy as _

from lukweb import models, payments

QUERY_INTERNAL_ACCOUNTS = 1
QUERY_TICKET_SALES_ACCOUNTS = 2


class GetQifForm(forms.Form):

    start = forms.DateField(
        label=_('Start date'),
        widget=forms.DateInput(
            attrs={'type': 'date'}
        ),
        required=True
    )

    end = forms.DateField(
        label=_('End date'),
        widget=forms.DateInput(
            attrs={'type': 'date'}
        )
    )

    by_processed = forms.BooleanField(
        label=_(
            'Filter by payment import timestamp, as opposed to '
            'payment timestamp.'
        ),
        required=False
    )

    ledger = forms.ChoiceField(
        choices=(
            (QUERY_INTERNAL_ACCOUNTS, _('Internal debts')),
            (QUERY_TICKET_SALES_ACCOUNTS, _('Ticket sales'))
        ),
        widget=forms.RadioSelect,
        required=True,
        initial=QUERY_INTERNAL_ACCOUNTS
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        g = models.FinancialGlobals.load()
        self.fields['by_processed'].initial = g.last_payment_export_byprocessed

    def clean(self):
        cleaned_data = super(GetQifForm, self).clean()
        start = cleaned_data['start']
        try:
            end = cleaned_data['end']
        except KeyError:
            end = cleaned_data['end'] = timezone.localdate(timezone.now())

        if end < start:
            raise ValidationError(
                _('Start date %(startval)s is after end date %(endval)s.'),
                code='invalid',
                params={
                    'startval': start,
                    'endval': end
                }
            )

    def get_qif(self):
        start = self.cleaned_data['start']
        end = self.cleaned_data['end']
        by_processed_ts = self.cleaned_data['by_processed']
        ledger = int(self.cleaned_data['ledger'])
        if ledger == QUERY_TICKET_SALES_ACCOUNTS:
            qif_formatter = payments.TicketSalesQifFormatter
        else:
            qif_formatter = payments.InternalAccountsQifFormatter
        content = qif_formatter(
            start, end, by_processed_ts=by_processed_ts
        ).generate()
        if by_processed_ts:
            fname_fmt = _('Payments imported %(start)s-%(end)s.qif')
        else:
            fname_fmt = _('Payments made %(start)s-%(end)s.qif')
        filename = fname_fmt % {
            'start': start.strftime('%Y%m%d'),
            'end': end.strftime('%Y%m%d')
        }
        g = models.FinancialGlobals.load()
        if ledger == QUERY_TICKET_SALES_ACCOUNTS:
            g.last_ticket_payment_export_start = start
            g.last_ticket_payment_export_end = end
            g.last_ticket_payment_export_byprocessed = by_processed_ts
        else:
            g.last_payment_export_start = start
            g.last_payment_export_end = end
            g.last_payment_export_byprocessed = by_processed_ts
        g.save()
        return filename, content