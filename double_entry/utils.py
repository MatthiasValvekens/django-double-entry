import logging
import csv
import datetime
import io
import re
import secrets
from decimal import Decimal, DecimalException
from collections import defaultdict, OrderedDict
from typing import Generator, TypeVar, Any, List, Tuple

from django.conf import settings
from django.core.exceptions import ValidationError

from django.http import HttpResponse
from django.utils import timezone
from django.utils.text import slugify
from django.utils.translation import ugettext_lazy as _
from djmoney.money import Money
from moneyed import EUR

logger = logging.getLogger(__name__)

T = TypeVar('T')
S = TypeVar('S')
# TODO: think of a more pythonic way to do this, this feels off
def consume_with_result(generator: Generator[T, Any, S]) -> Tuple[List[T], S]:
    result: S = None
    def _wrapper():
        nonlocal result
        result = yield from generator
    iter_result = list(_wrapper())
    assert result is not None
    return iter_result, result

def validated_bulk_query(get_search_param, ignorecase=False):
    """
    Decorator that adds an optional (opt-in) validation step to a bulk fetch,
    that tells you which of the searched objects were actually found.

    simple usage example:

    @validated_bulk_query(lambda x: x.name)
    def by_names(self, names):
        return self.filter(name__in=names)

    Then call qs.by_names(names, validate_unseen=True) to get a filtered qs
    back, and a frozenset with all unseen search params.
    It goes without saying that this forces the queryset to be evaluated.
    """
    def dec(f):
        def wrapf(self, search_scope, validate_unseen=False,
                  validate_nodups=False):
            result = f(self, search_scope)

            if not (validate_unseen or validate_nodups):
                return result

            # set up seen / scope iters
            if ignorecase:
                seen_iter = (get_search_param(x).casefold() for x in result)
                scope_set = frozenset(map(str.casefold, search_scope))
            else:
                seen_iter = map(get_search_param, result)
                scope_set = frozenset(search_scope)

            if validate_nodups:
                # frozenset() not good enough
                multi_seen = defaultdict(int)
                # assume search params are strings
                for x in seen_iter:
                    multi_seen[x] += 1
                duplicates = frozenset(
                    x for (x, y) in multi_seen.items() if y > 1
                )
                if validate_unseen:
                    unseen = frozenset(
                        x for x in scope_set if x not in multi_seen
                    )
                    return result, unseen, duplicates
                else:
                    return result, duplicates
            else:  # just validate_unseen
                return result, (scope_set - frozenset(seen_iter))
        return wrapf
    return dec


def csv_response(rows, headers, download_name):
    buf = io.StringIO()
    # don't use dictwriter, since we want to be able to translate the headers
    w = csv.writer(
        buf, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL
    )
    columns = tuple(headers)
    w.writerow([slugify(headers[c]) for c in columns])
    w.writerows(rows)
    cd = 'attachment; filename="%s"' % download_name
    response = HttpResponse(buf.getvalue(), content_type='text/csv')
    response['Content-Disposition'] = cd
    return response


def qif_response(content, download_name='transactions.qif'):
    response = HttpResponse(content, content_type='application/qif')
    cd = 'attachment; filename="%s"' % download_name
    response['Content-Disposition'] = cd
    return response


def _dt_fallback(timestamp, use_max=False):
    if isinstance(timestamp, datetime.datetime):
        if timezone.is_aware(timestamp):
            return timestamp
        else:
            return timezone.make_aware(timestamp)

    if use_max:
        time_pad = datetime.datetime.max.time()
    else:
        time_pad = datetime.datetime.min.time()

    return timezone.make_aware(
        datetime.datetime.combine(
            timestamp, time_pad
        )
    )


def make_token():
    return secrets.token_bytes(8)


OGM_PRE_POST = '(\+\+\+|\*\*\*)?'
OGM_REGEX = '%s%s%s' % (
    OGM_PRE_POST,
    r'(?P<fst>\d{3})/?(?P<snd>\d{4})/?(?P<trd>\d{3})(?P<mod>\d\d)',
    OGM_PRE_POST
)
SEARCH_PATTERN = re.compile(OGM_REGEX)


def decimal_to_money(d, currency=None):
    if currency is None:
        currency = settings.DEFAULT_CURRENCY
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


def normalise_ogm(ogm_str, validate=True):
    prefix, modulus = parse_ogm(ogm_str, validate=validate)
    return ogm_from_prefix(prefix)


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
                         remittance_info, sepa_purpose, sepa_iban,
                         sepa_bic, sepa_beneficiary):

    if len(sepa_purpose) != 4:
        raise ValueError('SEPA AT-44 Purpose must consist of 4 characters.')
    if transaction_amount.currency != EUR:
        raise ValueError(
            'Can only use EPC codes with amounts in EUR, not %s.',
            transaction_amount.currency
        )

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
        'bic': sepa_bic,
        'beneficiary': sepa_beneficiary,
        'iban': sepa_iban.replace(' ', ''),
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


class NegativeAmountError(ValueError):
    pass


def parse_amount(amount_str: str, raw_decimal=False):
    if amount_str is None:
        raise ValueError
    # ugly, but Decimal doesn't really support formatting parameters
    # (unless we involve the locale module)
    amt_str = amount_str.replace(',', '.')

    try:
        rd = Decimal(amt_str).quantize(Decimal('.01'))
    except (ValueError, IndexError, DecimalException):
        raise ValueError

    if raw_decimal:
        return rd
    currency = settings.DEFAULT_CURRENCY
    if rd <= 0:
        raise NegativeAmountError
    return Money(rd, currency)


class CIDictReader(csv.DictReader):

    def __next__(self):
        row = super().__next__()
        # minuscule computational overhead
        return CIOrderedDict(row)


class CIStr(str):

    def __eq__(self, other):
        if not isinstance(other, str):
            return False
        return self.casefold() == other.casefold()

    def __hash__(self):
        return hash(self.casefold())


def as_cistr(key):
    return key if isinstance(key, CIStr) else CIStr(key)


class CIOrderedDict(OrderedDict):
    # inspired by https://stackoverflow.com/a/32888599/4355619

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._convert_keys()

    def __getitem__(self, key):
        return super().__getitem__(
            as_cistr(key))

    def __setitem__(self, key, value):
        super().__setitem__(as_cistr(key), value)

    def __delitem__(self, key):
        return super().__delitem__(
            as_cistr(key))

    def __contains__(self, key):
        return super().__contains__(as_cistr(key))

    def pop(self, key, *args, **kwargs):
        return super().pop(as_cistr(key), *args, **kwargs)

    def get(self, key, *args, **kwargs):
        return super().get(as_cistr(key), *args, **kwargs)

    def setdefault(self, key, *args, **kwargs):
        return super().setdefault(
            as_cistr(key), *args, **kwargs)

    def update(self, E=None, **F):
        super().update(self.__class__(E or {}))
        super().update(self.__class__(**F))

    def _convert_keys(self):
        for k in list(self.keys()):
            v = super().pop(k)
            self.__setitem__(k, v)