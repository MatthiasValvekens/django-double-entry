import re
from decimal import Decimal

from django.http import HttpResponse
from django.conf import settings
from django.forms import ValidationError
from django.utils.translation import ugettext_lazy as _
from djmoney.money import Money
from moneyed import EUR

import logging
logger = logging.getLogger(__name__)

__all__ = [
    'PAYMENT_NATURE_CASH', 'PAYMENT_NATURE_OTHER', 'PAYMENT_NATURE_TRANSFER',
    'OGM_RESERVATION_PREFIX', 'OGM_INTERNAL_DEBT_PREFIX',
    'VALID_OGM_PREFIXES', 'OGM_REGEX',
    'decimal_to_money', 'parse_ogm', 'valid_ogm',
    'ogm_from_prefix', 'check_payment_change_permissions', 'any_payment_access',
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

OGM_PRE_POST = '(\+\+\+|\*\*\*)?'
OGM_REGEX = '%s%s%s' % (    
    OGM_PRE_POST,
    r'(?P<fst>\d{3})/?(?P<snd>\d{4})/?(?P<trd>\d{3})(?P<mod>\d\d)',
    OGM_PRE_POST
)
SEARCH_PATTERN = re.compile(OGM_REGEX)


def decimal_to_money(d, currency=None):
    if currency is None:
        currency = settings.BOOKKEEPING_CURRENCY
    return Money(
        amount=d.quantize(Decimal('.01')),
        currency=currency
    )


def parse_ogm(ogm_str, match=None, validate=True):
    m = match or SEARCH_PATTERN.match(ogm_str.strip())

    if m is None:
        raise ValueError('Invalid OGM string: %s' % ogm_str)

    prefix = int(m.group('fst') + m.group('snd') + m.group('trd'))
    modulus = int(m.group('mod'))
    remainder = prefix % 97

    if validate and \
            (modulus != remainder and not (remainder == 0 and modulus == 97)):
        raise ValueError('Modulus of %s does not validate.' % ogm_str)

    return prefix, modulus


def ogm_from_prefix(prefix, formatted=True):
    if isinstance(prefix, int):
        prefix_str = '%010d' % prefix
    else:
        prefix_str = str(prefix)

    if len(prefix_str) != 10:
        raise ValueError()
    modulo = int(prefix_str) % 97
    if modulo == 0:
        modulo = 97

    ogm = prefix_str + ('%02d' % modulo)

    if formatted:
        return '+++%s/%s/%s+++' % (ogm[:3], ogm[3:7], ogm[7:12])
    else:
        return ogm


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
    

# validate a raw ogm (i.e. just 12 digits in a string)
def valid_ogm(ogm):
    malformed = ValidationError(
            _('Malformed OGM: %(ogm)s'),
            code='invalid',
            params={'ogm': ogm}
        )

    if not len(ogm) == 12:
        raise malformed
    try:
        prefix = int(ogm[:10])
        modulus = int(ogm[10:12])
    except ValueError:
        raise malformed

    remainder = prefix % 97
    if modulus != remainder and not (remainder == 0 and modulus == 97):
        raise ValidationError(
            _('OGM %(ogm)s failed modulus check; expected %(modulus)s'),
            code='invalid',
            params={'modulus': remainder}
        )

def epc_qr_code_response(*, transaction_amount: Money,
                         remittance_info, sepa_purpose):
    from ..models import FinancialGlobals

    if len(sepa_purpose) != 4:
        raise ValueError('SEPA AT-44 Purpose must consist of 4 characters.')
    if transaction_amount.currency != EUR:
        raise ValueError(
            'Can only use EPC codes with amounts in EUR, not %s.',
            transaction_amount.currency
        )

    fin_globals: FinancialGlobals = FinancialGlobals.load()
    if not all([fin_globals.sepa_bic, fin_globals.sepa_beneficiary,
               fin_globals.choir_iban]):
        logger.warning(
            'Financial globals incomplete -- could not dispatch EPC QR code.'
        )
        return HttpResponse('Improperly configured', status=503)
    payload = (
        'BCD\n' # service identifier
        '001\n' # version number
        '1\n'   # charset (1 = UTF-8)
        'SCT\n' # ident code (SCT = SEPA Credit Transfer)
        '%(bic)s\n'
        '%(beneficiary)s\n'
        '%(iban)s\n'
        'EUR%(amount).2f\n'
        '%(purpose)s\n'
        '%(remittance_info)s\n'
    ) % {
        'bic': fin_globals.sepa_bic,
        'beneficiary': fin_globals.sepa_beneficiary,
        'iban': fin_globals.choir_iban.replace(' ', ''),
        'amount': transaction_amount.amount,
        'purpose': sepa_purpose,
        'remittance_info': remittance_info
    }

    try:
        import qrcode
        import qrcode.image.svg

        img = qrcode.make(
            payload, image_factory=qrcode.image.svg.SvgImage
        )
        response = HttpResponse(content_type='image/svg+xml')
        img.save(response)
        return response
    except ImportError:
        return HttpResponse('QR code not available', status=503)
