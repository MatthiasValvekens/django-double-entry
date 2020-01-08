import dataclasses
from datetime import datetime
from decimal import Decimal
from typing import Type

import pytz
from django.http import JsonResponse
from djmoney.money import Money

from webauth import api_utils
from double_entry.forms import bulk_utils

__all__ = ['register_pipeline_endpoint']


class APIErrorContext(bulk_utils.ResolvedTransactionMessageContext):

    def __init__(self, transaction_id: str):
        super().__init__()
        self.transaction_id = transaction_id


class PaymentPipelineAPIEndpoint(api_utils.APIEndpoint, abstract=True):
    pipeline_spec: bulk_utils.PipelineSpec = None
    endpoint_name = 'pipeline_submit'

    def __init_subclass__(cls, abstract=False, **kwargs):
        super().__init_subclass__(abstract=abstract)
        if not abstract and cls.pipeline_spec is None:
            raise TypeError

    def shape_resolved_transaction(self, raw: dict):
        transaction_id = raw['transaction_id']
        del raw['transaction_id']
        raw['amount'] = Money(
            amount=Decimal(raw['amount']), currency=raw['currency']
        )
        del raw['currency']
        try:
            pipeline_section_id = int(raw['pipeline_section_id'])
            del raw['pipeline_section_id']
        except KeyError:
            if len(self.pipeline_spec) == 1:
                pipeline_section_id = 0
            else:
                raise api_utils.APIError(
                    'pipeline_section_id is required on all transactions'
                )
        resolver, preparator = self.pipeline_spec[pipeline_section_id]
        rt_class = resolver.resolved_transaction_class
        if 'do_not_skip' not in raw:
            raw['do_not_skip'] = False
        timestamp_fields = {
            f.name for f in dataclasses.fields(rt_class)
            if f.type is datetime
        }
        for ts_field in timestamp_fields:
            ts = datetime.fromisoformat(raw[ts_field])
            if ts.tzinfo is None:
                # naive datetime - treat as UTC
                raw[ts_field] = pytz.utc.localize(ts)
            else:
                # replace timezone by UTC
                raw[ts_field] = ts.astimezone(pytz.utc)
        return pipeline_section_id, rt_class(
            **raw,
            message_context=APIErrorContext(transaction_id=transaction_id)
        )

    def post(self, request, *, transactions: list, commit: bool=True):
        by_section = [
            [] for _i in range(len(self.pipeline_spec))
        ]
        transaction_list = []
        def shape_all():
            for ix, tr in enumerate(transactions):
                # TODO: more granular error reporting, maybe allow non-faulty
                #  transactions to proceed?
                error_obj = api_utils.APIError(
                    'Illegally formatted transaction'
                )
                if not isinstance(tr, dict):
                    raise error_obj
                try:
                    section, resolved_tr = self.shape_resolved_transaction(tr)
                except (TypeError, ValueError, KeyError):
                    raise error_obj
                try:
                    by_section[section].append(resolved_tr)
                    yield resolved_tr
                except IndexError:
                    raise api_utils.APIError(
                        'Invalid pipeline section %d', section
                    )

        transaction_list = list(shape_all())

        pipeline = bulk_utils.PaymentPipeline(
            self.pipeline_spec, resolved=by_section
        )
        if commit:
            pipeline.commit()
        else:
            pipeline.review()

        def pipeline_responses():
            # transaction_list now carries all the error data from
            #  the pipeline, if applicable
            for tr in transaction_list:
                message_context = tr.message_context
                assert isinstance(message_context, APIErrorContext)
                res = {
                    'transaction_id': message_context.transaction_id,
                    'errors': message_context.transaction_errors,
                    'warnings': message_context.transaction_warnings,
                    'verdict': message_context.verdict,
                }
                if commit:
                    res['committed'] = tr.to_commit
                yield res

        return JsonResponse(
            { 'pipeline_responses': list(pipeline_responses()) },
            status=201 if commit else 200,
        )

def register_pipeline_endpoint(api: api_utils.API,
                               pipeline_spec: bulk_utils.PipelineSpec) -> Type['PaymentPipelineAPIEndpoint']:
    endpoint_class = type(
        'PipelineEndpointFor' + api.__class__.__name__,
        (PaymentPipelineAPIEndpoint,),
        { 'api': api, 'pipeline_spec': pipeline_spec }
    )
    assert issubclass(endpoint_class, PaymentPipelineAPIEndpoint)
    return endpoint_class