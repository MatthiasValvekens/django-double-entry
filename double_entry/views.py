import json
from dataclasses import dataclass
from typing import List, Type, Optional

from django.core.exceptions import SuspiciousOperation
from django.shortcuts import render
from django.views.generic import FormView

from .api import PaymentPipelineAPIEndpoint
from double_entry.forms import bulk_utils
from .forms.csv import FinancialCSVParser


@dataclass
class FinancialCSVUploadFormSetup:
    pipeline_spec: bulk_utils.PipelineSpec
    csv_parser_class: Type[FinancialCSVParser]
    endpoint: Type[PaymentPipelineAPIEndpoint]
    upload_field_label: Optional[str] = None
    review_template_name: str = 'transaction_upload/review.html'

class BaseFinancialCSVUploadFormView(FormView):

    template_name: str = 'transaction_upload/upload_form_view.html'
    form_class = bulk_utils.FinancialCSVUploadForm
    form_setup = None
    extra_review_context = {}

    def get_setup(self) -> Optional[FinancialCSVUploadFormSetup]:
        raise NotImplementedError

    def dispatch(self, request, *args, **kwargs):
        if self.form_setup is None:
            self.form_setup = self.get_setup()
        return super().dispatch(request, *args, **kwargs)

    def get_form_kwargs_for_setup(self, setup):
        kwargs = super().get_form_kwargs()
        del kwargs['prefix']
        kwargs['pipeline_spec'] = setup.pipeline_spec
        kwargs['csv_parser_class'] = setup.csv_parser_class
        if setup.upload_field_label is not None:
            kwargs['upload_field_label'] = setup.upload_field_label
        return kwargs

    def get_endpoint_url(self):
        return self.form_setup.endpoint.url()

    def format_transaction_response(self, transaction, transaction_id):
        return self.form_setup.endpoint.format_transaction_response(
            transaction=transaction, transaction_id=transaction_id,
            include_commit=False
        )

    def get_review_context_data(self):
        return self.extra_review_context

    def form_valid(self, form):
        form.review()

        def format_transaction_id(section_id, count):
            return 'sec-%d-trans-%d' % (section_id, count)

        def transform_resolved_in_section(section_id: int, resolved: bulk_utils.ResolvedSection):
            # transform resolved transactions into the right format for the template
            return [
                (format_transaction_id(section_id, ix), account, rt)
                for ix, (account, rt) in enumerate(resolved)
            ]
        # data to initialise feedback annotations in javascript
        json_initial_data = [
            self.format_transaction_response(
                transaction=transaction,
                transaction_id=format_transaction_id(section_id, count),
            )
            for section_id, resolved in enumerate(form.resolved)
            for count, (account, transaction) in enumerate(resolved)
        ]

        context = {
            'transaction_initial_data': json.dumps(json_initial_data),
            'resolved_by_section': [
                (ix, transform_resolved_in_section(ix, resolved))
                for ix, resolved in enumerate(form.resolved)
            ],
            'pipeline_errors': form.pipeline_errors,
            'endpoint_url': self.get_endpoint_url(),
            'section_count': len(self.form_setup.pipeline_spec)
        }
        # simplification if the pipeline only has one index
        if len(form.resolved) == 1:
            context['resolved_list'] = context['resolved_by_section'][0][1]
        context.update(**self.get_review_context_data())
        return render(self.request, self.form_setup.review_template_name, context=context)

class FinancialCSVUploadFormView(BaseFinancialCSVUploadFormView):
    setup: FinancialCSVUploadFormSetup

    def get_setup(self) -> FinancialCSVUploadFormSetup:
        return self.form_setup

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs.update(self.get_form_kwargs_for_setup(self.form_setup))
        return kwargs

class MultiFinancialCSVUploadFormView(BaseFinancialCSVUploadFormView):
    form_setups: List[FinancialCSVUploadFormSetup] = None
    form_selector_field = 'form-selected'

    def dispatch(self, request, *args, **kwargs):
        if self.form_setups is None:
            self.form_setups = self.get_form_setups()
        return super().dispatch(request, *args, **kwargs)

    def get_form_setups(self):
        return self.form_setups

    def get_selected_index(self) -> Optional[int]:
        selection = self.request.POST.get(self.form_selector_field)
        if not selection:
            return None
        try:
            return int(selection)
        except (ValueError, TypeError):
            raise SuspiciousOperation

    def get_context_data(self, **kwargs):
        selected_index = self.get_selected_index()

        # stay compatible with form_invalid
        try:
            the_form = kwargs['form']
        except KeyError:
            the_form = None

        def form(ix, setup):
            if ix == selected_index and the_form is not None:
                return the_form
            form_class = self.get_form_class()
            return form_class(
                **self.get_form_kwargs_for_setup(setup), prefix=ix
            )
        forms = [
            form(ix, setup) for ix, setup in enumerate(self.form_setups)
        ]
        return {
            'form_selector_field': self.form_selector_field,
            'forms': forms
        }

    def get_setup(self) -> Optional[FinancialCSVUploadFormSetup]:
        ix = self.get_selected_index()
        return self.form_setups[ix] if ix is not None else None

    def get_prefix(self):
        return self.get_selected_index()

    def get_form_kwargs(self):
        if self.get_setup() is not None:
            kwargs = self.get_form_kwargs_for_setup(self.form_setup)
            ix = self.get_selected_index()
            assert ix is not None
            kwargs['prefix'] = ix
            return kwargs
        else:
            return {}
