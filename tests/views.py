from .models import *
from double_entry.api import register_pipeline_endpoint
from webauth import api_utils

test_transfer_pipeline = [
    (SimpleTransferResolver, SimpleTransferPreparator),
    (ReservationTransferResolver, ReservationTransferPreparator)
]

test_pipeline_api = api_utils.API(
    name='testapi', auth_workflow=[api_utils.DummyAuthMechanism()],
)

pipeline_endpoint = register_pipeline_endpoint(
    test_pipeline_api, test_transfer_pipeline
)
