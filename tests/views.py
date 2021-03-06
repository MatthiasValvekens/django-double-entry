from dataclasses import dataclass

from django.core.files.uploadedfile import SimpleUploadedFile

from double_entry.forms.csv import KBCCSVParser
from double_entry.views import (
    FinancialCSVUploadFormView,
    FinancialCSVUploadFormSetup,
)
from .models import *
from double_entry.api import (
    register_pipeline_endpoint,
    PaymentPipelineAPIEndpoint,
)
from webauth import api_utils

test_transfer_pipeline = [
    (SimpleTransferResolver, SimpleGenericPreparator),
    (ReservationTransferResolver, ReservationPreparator)
]

test_pipeline_api = api_utils.API(
    name='testapi', auth_workflow=[api_utils.DummyAuthMechanism()],
)

pipeline_endpoint = register_pipeline_endpoint(
    test_pipeline_api, test_transfer_pipeline
)


# For testing parsing of extra attributes
@dataclass(frozen=True)
class TestResolvedTransaction(ResolvedTransaction):
    boolean_field_test: bool
    integer_field_test: int

class AltPipelineEndpoint(PaymentPipelineAPIEndpoint):
    api = test_pipeline_api
    endpoint_name = 'alt_pipeline_submission'
    pipeline_spec = [(TestResolvedTransaction, SimpleGenericPreparator)]



class TestTransferFormView(FinancialCSVUploadFormView):
    form_setup = FinancialCSVUploadFormSetup(
        pipeline_spec=test_transfer_pipeline,
        csv_parser_class=KBCCSVParser,
        endpoint=pipeline_endpoint
    )

    @staticmethod
    def hook_simple_lookup_test(request):
        # for quick checks of the transaction review page
        from .test_csv import KBC_SIMPLE_LOOKUP_TEST
        return TestTransferFormView.hook(request, KBC_SIMPLE_LOOKUP_TEST)

    @classmethod
    def hook(cls, request, csvdata):
        csv_file = SimpleUploadedFile(
            'transfers.csv', csvdata.encode('utf-8')
        )
        request.FILES._mutable = True
        request.FILES['csv'] = csv_file
        request.method = 'POST'
        return TestTransferFormView.as_view()(request)
