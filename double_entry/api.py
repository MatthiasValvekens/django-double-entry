import dataclasses
from datetime import datetime
from decimal import Decimal, DecimalException
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

class TransactionShapingError(api_utils.APIError):
    pass

class PaymentPipelineAPIEndpoint(api_utils.APIEndpoint, abstract=True):
    pipeline_spec: bulk_utils.SubmissionSpec = None

    def __init_subclass__(cls, abstract=False, **kwargs):
        super().__init_subclass__(abstract=abstract)
        if not abstract and cls.pipeline_spec is None:
            raise TypeError

    def get_queryset(self, pipeline_section_id):
        rt_class, preparator_class = self.pipeline_spec[pipeline_section_id]
        return bulk_utils.LedgerQuerySetBuilder.default_ledger_query_set(
            preparator_class.transaction_party_model
        )

    def shape_resolved_transaction(self, transaction_id, raw: dict):
        try:
            raw['amount'] = Money(
                amount=Decimal(raw['amount']), currency=raw['currency']
            )
            del raw['currency']
        except (DecimalException, ValueError, KeyError):
            raise TransactionShapingError(
                "Transaction amount must be specified in decimal \'amount\' and "
                "\'currency\' fields."
            )
        except ValueError:
            raise TransactionShapingError(
                'Invalid transaction amount \"%(amount)s %(currency)s\"', {
                    'amount': raw['amount'], 'currency': raw['currency']
                }
            )
        try:
            pipeline_section_id = int(raw['pipeline_section_id'])
            del raw['pipeline_section_id']
        except KeyError:
            if len(self.pipeline_spec) == 1:
                pipeline_section_id = 0
            else:
                raise TransactionShapingError(
                    'pipeline_section_id is required on all transactions'
                )
        try:
            rt_class, preparator = self.pipeline_spec[pipeline_section_id]
        except IndexError:
            raise TransactionShapingError(
                'Invalid pipeline section \'%d\'.', pipeline_section_id
            )

        # attempt to reprocess fields in dict
        # we allow coercions of string values, for easier interoperability
        #  with html attributes
        for f in dataclasses.fields(rt_class):
            try:
                raw_field = raw[f.name]
            except KeyError:
                # will be dealt with later, if necessary
                continue
            if f.name == 'amount':  # we already dealt with this
                continue
            if f.type is datetime:
                try:
                    ts = datetime.fromisoformat(raw_field)
                except (KeyError, ValueError, TypeError):
                    raise TransactionShapingError(
                        'Could not parse ISO datetime \'%s\'.',
                        raw_field
                    )
                if ts.tzinfo is None:
                    # naive datetime - treat as UTC
                    raw[f.name] = pytz.utc.localize(ts)
                else:
                    # replace timezone by UTC
                    raw[f.name] = ts.astimezone(pytz.utc)
            elif f.type is bool:
                if isinstance(raw_field, bool):
                    continue
                elif isinstance(raw_field, str):
                    if raw_field.casefold() == 'true':
                        raw[f.name] = True
                    elif raw_field.casefold() == 'false':
                        raw[f.name] = False
                    else:
                        raise TransactionShapingError(
                            'Invalid boolean string \'%s\'.', raw_field
                        )
                else:
                    raise TransactionShapingError(
                        'Boolean fields must be represented as booleans '
                        'or \'true\'/\'false\' strings.'
                    )
            else:
                if type(raw_field) is f.type:
                    continue
                try:
                    raw[f.name] = f.type(raw_field)
                except:
                    raise TransactionShapingError(
                        'Failed to coerce \'%(value)s\' of type \'%(value_type)s\' '
                        'to value of type \'%(field_type)s\' in field '
                        '\'%(field_name)s\'.', {
                            'value': raw_field, 'value_type': type(raw_field),
                            'field_type': f.type, 'field_name': f.name
                        }
                    )

        raw.setdefault('do_not_skip', False)

        try:
            resolved_transaction = rt_class(
                **raw,
                message_context=APIErrorContext(transaction_id=transaction_id)
            )
        except (TypeError, ValueError):
            keys_specified = frozenset(raw.keys())
            all_fields = {
                f.name for f in dataclasses.fields(rt_class)
                if f.name != 'message_context'
            }
            over_defined = keys_specified - all_fields
            required_fields = {
                f.name for f in dataclasses.fields(rt_class)
                if f.default_factory is dataclasses.MISSING
                    and f.default is dataclasses.MISSING
                    and f.name != 'message_context'
            }
            under_defined = required_fields - keys_specified
            if over_defined and under_defined:
                raise TransactionShapingError(
                    'The fields \'%s\' are required, and '
                    'the fields \'%s\' are undefined.' % (
                        ', '.join(under_defined),
                        ', '.join(over_defined)
                    )
                )
            elif over_defined:
                raise TransactionShapingError(
                    'The fields \'%s\' are undefined.' % ', '.join(over_defined)
                )
            elif under_defined:
                raise TransactionShapingError(
                    'The fields \'%s\' are required.' % ', '.join(under_defined)
                )
            else:  # pragma: no cover
                raise TransactionShapingError(
                    'Failed to instantiate \'resolved_transaction\'.'
                )
        return pipeline_section_id, resolved_transaction

    @classmethod
    def format_transaction_response(cls, transaction: bulk_utils.ResolvedTransaction,
                                    include_commit=False, transaction_id=None):
        message_context = transaction.message_context
        if transaction_id is None:
            assert isinstance(message_context, APIErrorContext)
            transaction_id = message_context.transaction_id
        res = {
            'transaction_id': transaction_id,
            'errors': message_context.transaction_errors,
            'warnings': message_context.transaction_warnings,
            'verdict': message_context.verdict,
        }
        if include_commit:
            # FIXME In retrospect this is a bit of a misnomer, since
            #  calling the API in no-commit mode obviously makes it so
            #  that transactions are never saved to the DB.
            #  Also calling this one 'to_commit' would have made more sense.
            #  Need to document this properly!
            res['committed'] = transaction.to_commit
        return res

    # allow subclasses to add more information if necessary
    # noinspection PyUnusedLocal
    def format_total_response(self, *, pipeline, transaction_list,
                              faulty_transaction_responses, commit):
        def pipeline_responses():
            # transaction_list now carries all the error data from
            #  the pipeline, if applicable
            for tr in transaction_list:
                res = self.format_transaction_response(tr, include_commit=True)
                yield res
            yield from faulty_transaction_responses
        pl_responses = list(pipeline_responses())
        response = { 'pipeline_responses': pl_responses }
        if commit:
            response['all_committed'] = all(
                res.get('committed', False) for res in pl_responses
            )
        return response

    def faulty_transaction(self, transaction_id, error, include_commit):
        res = {
            'transaction_id': transaction_id,
            'errors': [error], 'warnings': [],
            'verdict': bulk_utils.ResolvedTransactionVerdict.DISCARD
        }
        if include_commit:
            res['committed'] = False
        return res

    def post(self, request, *, transactions: list, commit: bool=True):
        by_section = [[] for _i in range(len(self.pipeline_spec))]
        faulty_transactions = []
        def shape_all():
            for tr in transactions:
                if not isinstance(tr, dict):
                    raise api_utils.APIError('Transactions must be JSON objects')
                try:
                    transaction_id = tr['transaction_id']
                    del tr['transaction_id']
                except KeyError:
                    raise api_utils.APIError(
                        'All transactions must have a transaction_id'
                    )
                try:
                    section, resolved_tr = self.shape_resolved_transaction(
                        transaction_id, tr
                    )
                except TransactionShapingError as e:
                    faulty_transactions.append(
                        self.faulty_transaction(
                            transaction_id=transaction_id,
                            error=str(e), include_commit=commit
                        )
                    )
                    continue
                try:
                    by_section[section].append(resolved_tr)
                    yield resolved_tr
                except IndexError:  # pragma: no cover
                    # this will have generated an error earlier
                    pass

        transaction_list = list(shape_all())

        pipeline = bulk_utils.PaymentSubmissionPipeline(self.pipeline_spec)
        pipeline.submit_resolved(
            [(self.get_queryset(ix), trs) for ix, trs in enumerate(by_section)]
        )

        if commit:
            pipeline.commit()
        else:
            pipeline.review()
        response = self.format_total_response(
            pipeline=pipeline, faulty_transaction_responses=faulty_transactions,
            transaction_list=transaction_list, commit=commit
        )
        return JsonResponse(response, status=201 if commit else 200)


def register_pipeline_endpoint(api: api_utils.API,
                               pipeline_spec: bulk_utils.PipelineSpec,
                               endpoint_name=None) -> Type['PaymentPipelineAPIEndpoint']:

    class _PipelineEndpoint(PaymentPipelineAPIEndpoint, abstract=True):
        def get_queryset(self, pipeline_section_id):
            resolver_cls, preparator_cls = pipeline_spec[pipeline_section_id]
            return resolver_cls.base_query_set()

    endpoint_class = type(
        'PipelineEndpointFor' + api.__class__.__name__,
        (_PipelineEndpoint,), {
            'api': api,
            'pipeline_spec': bulk_utils.as_submission_spec(pipeline_spec),
            'endpoint_name': endpoint_name or 'pipeline_submit'
        }
    )
    assert issubclass(endpoint_class, PaymentPipelineAPIEndpoint)
    return endpoint_class