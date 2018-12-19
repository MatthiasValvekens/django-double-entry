from django import forms
from django.forms import ValidationError
from django.forms.models import ModelForm
from django.utils import timezone
from django.utils.translation import (
    ugettext_lazy as _,
)

from lukweb import payments, models
from lukweb.widgets import (
    DatalistInputWidget,
)

__all__ = ['GnuCashFieldMixin', 'GetQifForm']


class GnuCashFieldMixin(ModelForm):
    require_gnucash = True
    gnucash = forms.CharField(
        label=_('GnuCash category'),
        widget=DatalistInputWidget(
            choices=models.GnuCashCategory.objects.all
        )
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        gnucash_field = self._meta.model._meta.get_field('gnucash_category')
        self.fields['gnucash'].help_text = gnucash_field.help_text
        self.fields['gnucash'].required = self.require_gnucash
        instance = kwargs.get('instance')
        if instance is not None and instance.gnucash_category is not None:
            self.fields['gnucash'].initial = instance.gnucash_category.name

    def _save(self, commit=True, set_category=True):
        instance = super(GnuCashFieldMixin, self).save(commit=False)
        if commit or set_category:
            # this potentially writes to the db, so we don't want this
            # as a clean_field method
            gnucash_raw = self.cleaned_data['gnucash']
            instance.gnucash_category = models.GnuCashCategory.get_category(
                gnucash_raw.strip()
            )
            if commit:
                instance.save()
        return instance


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
        content = payments.generate_qif(
            start, end, by_processed_ts=by_processed_ts
        )
        if by_processed_ts:
            fname_fmt = _('Payments imported %(start)s-%(end)s.qif')
        else:
            fname_fmt = _('Payments made %(start)s-%(end)s.qif')
        filename = fname_fmt % {
            'start': start.strftime('%Y%m%d'),
            'end': end.strftime('%Y%m%d')
        }
        g = models.FinancialGlobals.load()
        g.last_payment_export_start = start
        g.last_payment_export_end = end
        g.last_payment_export_byprocessed = by_processed_ts
        g.save()
        return filename, content
