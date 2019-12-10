import abc
import io
from typing import Optional, List, Tuple

from django import forms
from django.conf import settings
from django.core.exceptions import ValidationError
from django.forms.models import ModelForm
from django.utils.functional import cached_property
from django.utils.translation import ugettext_lazy as _

from .. import models
from double_entry.forms.widgets import DatalistInputWidget

__all__ = ['GnuCashFieldMixin']

class GnuCashFieldMixin(ModelForm):
    require_gnucash = True

    gnucash_field_name = 'gnucash_category'

    gnucash = forms.CharField(
        widget=DatalistInputWidget(
            choices=models.GnuCashCategory.objects.all
        )
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        gnucash_field = self._meta.model._meta.get_field(
            self.gnucash_field_name
        )
        self.fields['gnucash'].label = gnucash_field.verbose_name
        self.fields['gnucash'].help_text = gnucash_field.help_text
        self.fields['gnucash'].required = self.require_gnucash
        instance = kwargs.get('instance')
        existing_gc = getattr(instance, self.gnucash_field_name, None)
        if instance is not None and existing_gc is not None:
            self.fields['gnucash'].initial = existing_gc.name

    def _save(self, commit=True, set_category=True):
        instance = super(GnuCashFieldMixin, self).save(commit=False)
        if commit or set_category:
            # this potentially writes to the db, so we don't want this
            # as a clean_field method
            gnucash_raw = self.cleaned_data['gnucash']
            gc = models.GnuCashCategory.get_category(gnucash_raw.strip())
            setattr(instance, self.gnucash_field_name, gc)
            if commit:
                instance.save()
        return instance


ErrorList = List[Tuple[List[int],str]]

class ErrorMixin(abc.ABC):

    @abc.abstractmethod
    def error_at_line(self, line_no: int, msg: str, params: Optional[dict]=None):
        pass

    @abc.abstractmethod
    def error_at_lines(self, line_nos: List[int], msg: str,
                       params: Optional[dict]=None):
        pass


class ErrorContextWrapper(ErrorMixin):

    def __init__(self, error_context: ErrorMixin):
        self.error_context = error_context

    def error_at_line(self, line_no: int, msg: str,
                      params: Optional[dict] = None):
        self.error_context.error_at_line(line_no, msg, params)

    def error_at_lines(self, line_nos: List[int], msg: str,
                       params: Optional[dict] = None):
        self.error_context.error_at_lines(line_nos, msg, params)


class ParserErrorMixin(ErrorMixin):
    _ready = False

    def __init__(self, parser):
        self.parser = parser
        self._errors: ErrorList = []

    def run(self):
        return

    def _ensure_ready(self):
        if not self._ready:
            self.run()
        _ready = True
        return

    def error_at_line(self, line_no: int, msg: str, params: Optional[dict]=None):
        self.error_at_lines([line_no], msg, params)

    def error_at_lines(self, line_nos: List[int], msg: str,
                       params: Optional[dict]=None):
        if params is not None:
            msg = msg % params
        self._errors.insert(0, (sorted(line_nos), msg))

    @cached_property
    def errors(self) -> ErrorList:
        self._ensure_ready()
        if self.parser is not None:
            parser_errors = [
                ([lno], err) for lno, err in self.parser.errors
            ]
        else:
            parser_errors = []
        return sorted(
            parser_errors + self._errors,
            # sort by line number(s)
            # these are lists of integers, so OK
            key=lambda t: t[0]
        )


class CSVUploadForm(forms.Form):

    def _validate_csv(self, field):
        # TODO: validate these on the client too, for better UX
        # TODO set accept attribute on filefield to something like
        # .csv, text/csv
        f = self.cleaned_data[field]
        if f is not None:
            if f.size > settings.MAX_CSV_UPLOAD:
                raise ValidationError(
                    _('Uploaded .csv file too large (> 1 MiB).')
                )

            if not f.name.endswith('.csv'):
                raise ValidationError(
                    _('Please upload a .csv file.')
                )
            wrapf = io.TextIOWrapper(f, encoding='utf-8-sig', errors='replace')
            parser_factory = getattr(
                self, field + '_parser_class', None
            )
            if parser_factory is None:
                raise ValueError('No parser class specified')
            parser = parser_factory(wrapf)
            self.cleaned_data[field] = parser
            return parser


    # if there's a field named 'csv', this will validate it
    # (reasonable default). If not, nothing happens.
    def clean_csv(self):
        return self._validate_csv('csv')