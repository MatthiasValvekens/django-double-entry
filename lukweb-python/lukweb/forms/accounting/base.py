import logging
from decimal import Decimal

from django import forms
from django.conf import settings
from django.db.models import Q
from django.forms import ValidationError
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _,
)
from djmoney.money import Money

from ...models.accounting import base as accounting_base

__all__ = [
    'InlineTransactionSplitFormSet', 'BaseInlineTransactionSplitForm',
    'transaction_split_form_factory'

]
logger = logging.getLogger(__name__)

class InlineTransactionSplitFormSet(forms.BaseInlineFormSet):
 
    def get_form_kwargs(self, index):
        kwargs = super().get_form_kwargs(index)
        kwargs['parent_object'] = self.instance
        kwargs['remote_qs'] = self._admissible_counterpart_queryset
        return kwargs

    def base_filter(self):
        return Q()

    @cached_property
    def _admissible_counterpart_queryset(self):
        other_half_model = self.instance.__class__.get_other_half_model()
        other_half_objects = other_half_model._meta.default_manager
        base_qs = other_half_objects.with_remote_accounts().filter(
            self.base_filter()
        ).unmatched().order_by('-timestamp')
        
        split_model, counterpart_name = other_half_model.get_split_model()
        # query all pk's of entries in the remote account that already
        # appear in a split with the object that we are currently operating on.
        # These need to be excluded from the UI, since only one split per
        # account pair is allowed
        # TODO: (WBC) do this in one query and let the db figure things out.
        #  Probably the better option if this can be done without writing
        #  raw sql, since outsmarting the query planner is pretty hard to
        #  do these days.
        taken_pks = self.instance.split_manager.all().values_list(
            counterpart_name + '_id', flat=True
        )
        return base_qs.exclude(pk__in=taken_pks)

    def clean(self):
        if any(self.errors):
            return
        max_total = self.instance.total_amount
        split_model, __ = self.instance.__class__.get_split_model()
        col_a, col_b = split_model.get_double_book_models().keys()

        # add up amounts of all non-deleted valid splits
        def split_amounts():
            for form in self.forms:
                if form.cleaned_data.get('DELETE'):
                    continue
                col_a_value = form.cleaned_data.get(col_a)
                col_b_value = form.cleaned_data.get(col_b)
                if col_a_value and col_b_value:
                    yield form.cleaned_data['amount']

        split_total = sum(
            split_amounts(),
            Money(Decimal('0'), settings.BOOKKEEPING_CURRENCY)
        )

        if split_total > max_total:
            raise ValidationError(
                _(
                    'Splits sum to %(split_total)s. The maximal total for '
                    'this object is %(max_total)s.'
                ) % {
                    'split_total': split_total,
                    'max_total': max_total
                }
            )


class ITSFormChoiceIterator(forms.models.ModelChoiceIterator):
    def choice(self, obj):
        return (
            self.field.prepare_value(obj), obj.form_select_str()
        )


class BaseInlineTransactionSplitForm(forms.ModelForm):
    apply_select_widget_args = {
        'style': 'width: 80ch;'
    }

    def __init__(self, *args, parent_object=None, remote_qs=None, **kwargs):
        super().__init__(*args, **kwargs)
        # when it matters, we'll be called with proper kwargs
        # but we need to account for them not being there for when
        # django's admin tries to detect multipart forms
        # in it's own cute but utterly retarded way.
        # That is, by attempting to call the formsets base
        # formset constructor without arguments and then calling is_multipart
        # on the form instance. Yes, this completely ignores form_kwargs, which
        # is stupid
        self.parent_object = parent_object
        if parent_object is not None:
            home_col_model = self.parent_object.__class__
            split_model, home_col_name = home_col_model.get_split_model()
            columns = split_model.get_double_book_models().keys()
            for col in columns:
                field = self.fields[col]
                field.iterator = ITSFormChoiceIterator
                field.widget = forms.Select(attrs={'style': 'width: 80ch;'})
                if col == home_col_name:
                    # this one doesn't matter anyway
                    field.queryset = None
                else:
                    field.queryset = remote_qs
            if self.instance is not None and self.instance.pk is not None:
                # pin choices to current value and disable field
                for col in columns:
                    self.fields[col].disabled = True
                    self.fields[col].widget.choices = [
                        ITSFormChoiceIterator(self.fields[col]).choice(
                            getattr(self.instance, col)
                        )
                    ]

    def clean(self):
        super().clean()
        if not self.has_changed():
            return
        amount = self.cleaned_data.get('amount')
        if not amount:
            return
        remote_col_model = self.parent_object.__class__.get_other_half_model()
        split_model, remote_col_name = remote_col_model.get_split_model()
        about_to_apply = self.cleaned_data[remote_col_name]
        if about_to_apply.unmatched_balance < amount:
            raise ValidationError(
                about_to_apply.insufficient_unmatched_balance_error % {
                    'amount': amount,
                    'balance': about_to_apply.unmatched_balance
                }
            )


def transaction_split_form_factory(split_model):
    assert issubclass(split_model, accounting_base.BaseTransactionSplit)
    column_a, column_b = split_model.get_double_book_models().keys()

    class Meta:
        model = split_model
        fields = (column_a, column_b, 'amount')

    return type(
        'Inline%sForm' % split_model.__name__,
        (BaseInlineTransactionSplitForm,),
        {'Meta': Meta}
    )
