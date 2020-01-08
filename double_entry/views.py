import json
from collections import OrderedDict
from typing import Any, OrderedDict as TOrderedDict

from django.shortcuts import render
from django.views.generic import FormView

from .api import PaymentPipelineAPIEndpoint
from double_entry.forms import bulk_utils

class FinancialCSVUploadFormView(FormView):

    form_class = bulk_utils.FinancialCSVUploadForm
    template_name = 'transaction_upload/upload_form_view.html'
    review_template_name = 'transaction_upload/review.html'
    named_pipeline_spec: TOrderedDict[Any, bulk_utils.PipelineSectionClass] = OrderedDict()
    csv_parser_class = None
    upload_field_label = None
    endpoint: PaymentPipelineAPIEndpoint = None

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        kwargs['pipeline_spec'] = self.named_pipeline_spec.values()
        kwargs['csv_parser_class'] = self.csv_parser_class
        if self.upload_field_label is not None:
            kwargs['upload_field_label'] = self.upload_field_label
        return kwargs

    def get_endpoint_url(self):
        return self.endpoint.url()

    def format_transaction_response(self, transaction, transaction_id):
        return self.endpoint.format_transaction_response(
            transaction=transaction, transaction_id=transaction_id, commit=False
        )

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

        return render(self.request, self.review_template_name, context={
            'transaction_initial_data': json.dumps(json_initial_data),
            'resolved_by_section': [
                (ix, name, transform_resolved_in_section(ix, resolved))
                for ix, (name, resolved)
                in enumerate(zip(self.named_pipeline_spec, form.resolved))
            ],
            'endpoint_url': self.get_endpoint_url(),
            'section_count': len(self.named_pipeline_spec)
        })
