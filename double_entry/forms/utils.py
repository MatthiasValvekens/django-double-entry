import abc
import io
from typing import Optional, List, Tuple

from django import forms
from django.core.exceptions import ValidationError
from django.forms.models import ModelForm
from django.utils.deconstruct import deconstructible
from django.utils.translation import gettext_lazy as _

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

    def error_at_line(self, line_no: int, msg: str, params: Optional[dict]=None):
        self.error_at_lines([line_no], msg, params)

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


class ParserErrorAggregator(ErrorMixin):
    _ready = False

    def __init__(self, parser):
        self.parser = parser
        self._errors: ErrorList = []

    def error_at_line(self, line_no: int, msg: str, params: Optional[dict]=None):
        self.error_at_lines([line_no], msg, params)

    def error_at_lines(self, line_nos: List[int], msg: str,
                       params: Optional[dict]=None):
        if params is not None:
            msg = msg % params
        self._errors.insert(0, (sorted(line_nos), msg))

    @property
    def errors(self) -> ErrorList:
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


@deconstructible
class FileSizeValidator:

    def __init__(self, mib=0, kib=0, b=0):
        self.mib = mib
        self.kib = kib
        self.b = b
        self.size_limit = ((mib * 1024) + kib) * 1024 + b

    def __eq__(self, other):
        return isinstance(other, FileSizeValidator) \
               and self.mib == other.mib \
               and self.kib == other.kib \
               and self.b == other.b

    def __call__(self, upl_file):
        if upl_file.size > self.size_limit:
            # has to happen here for the gettext calls to work
            fmt_parts = [
                _('%d MiB') % self.mib if self.mib > 0 else '',
                _('%d KiB') % self.kib if self.kib > 0 else '',
                _('%d bytes') % self.b if self.b > 0 else ''
            ]
            fmt_limit = ', '.join(p for p in fmt_parts if p)
            raise ValidationError(
                _(
                    'Uploaded file larger than %(limit)s.' % {
                        'limit': fmt_limit
                    }
                )
            )


class CSVUploadForm(forms.Form):

    def _validate_csv(self, field):
        # TODO: validate these on the client too, for better UX
        # TODO set accept attribute on filefield to something like
        # .csv, text/csv
        f = self.cleaned_data[field]
        if f is not None:
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