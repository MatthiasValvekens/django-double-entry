import logging

from django.http import HttpResponse
from djmoney.money import Money
from double_entry.utils import epc_qr_code_response as _epc_qr_response

logger = logging.getLogger(__name__)

__all__ = [
    'PAYMENT_NATURE_CASH', 'PAYMENT_NATURE_OTHER', 'PAYMENT_NATURE_TRANSFER',
    'OGM_RESERVATION_PREFIX', 'OGM_INTERNAL_DEBT_PREFIX',
    'VALID_OGM_PREFIXES', 'check_payment_change_permissions', 'any_payment_access',
    'epc_qr_code_response'
]

PAYMENT_NATURE_CASH = 1
PAYMENT_NATURE_TRANSFER = 2
PAYMENT_NATURE_OTHER = 3

OGM_RESERVATION_PREFIX = '1'
OGM_INTERNAL_DEBT_PREFIX = '2'

VALID_OGM_PREFIXES = [
    OGM_RESERVATION_PREFIX, 
    OGM_INTERNAL_DEBT_PREFIX,
]


def check_payment_change_permissions(user):
    res = []
    if user.has_perm('lukweb.add_internalpayment'):
        res += [OGM_INTERNAL_DEBT_PREFIX]
    if user.has_perm('lukweb.change_reservation'):
        res += [OGM_RESERVATION_PREFIX]
    return res


def any_payment_access(user):
    return user.has_perm('lukweb.add_internalpayment') \
        or user.has_perm('lukweb.change_reservation')


def epc_qr_code_response(*, transaction_amount: Money, remittance_info,
                         sepa_purpose):
    from ..models import FinancialGlobals
    fin_globals: FinancialGlobals = FinancialGlobals.load()
    if not all([fin_globals.sepa_bic, fin_globals.sepa_beneficiary,
                fin_globals.choir_iban]):
        logger.warning(
            'Financial globals incomplete -- could not dispatch EPC QR code.'
        )
        return HttpResponse('Improperly configured', status=503)
    else:
        return _epc_qr_response(
            transaction_amount=transaction_amount,
            remittance_info=remittance_info,
            sepa_purpose=sepa_purpose,
            sepa_bic=fin_globals.sepa_bic,
            sepa_beneficiary=fin_globals.sepa_beneficiary,
            sepa_iban=fin_globals.choir_iban
        )
