import re
from collections import defaultdict
from decimal import Decimal

from django.conf import settings
from django.forms import ValidationError
from django.utils import timezone
from django.utils.translation import (
    ugettext_lazy as _, activate, get_language,
    pgettext,
)
from djmoney.money import Money

from ..utils import _dt_fallback

__all__ = [
    'PAYMENT_NATURE_CASH', 'PAYMENT_NATURE_OTHER', 'PAYMENT_NATURE_TRANSFER',
    'OGM_RESERVATION_PREFIX', 'OGM_INTERNAL_DEBT_PREFIX',
    'generate_qif', 'VALID_OGM_PREFIXES', 'OGM_REGEX', 'decimal_to_money',
    'parse_ogm', 'valid_ogm',
    'ogm_from_prefix', 'check_payment_change_permissions', 'any_payment_access',
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

def generate_qif(start, end, by_processed_ts=True):
    from ..models import InternalPaymentSplit, FinancialGlobals
    overpaid_category = pgettext('.qif export', 'OVERPAID')
    overpaid_memo = pgettext('.qif export', 'ERROR: OVERPAID')
    multiple_debts_memo = pgettext('.qif export', '[multiple debts]')
    gsettings = FinancialGlobals.load()
    old_lang = get_language()
    activate(gsettings.gnucash_language)
    categories_seen = set()

    accounts = {
        PAYMENT_NATURE_TRANSFER: (
            gsettings.gnucash_checking_account_name, 'Bank'
        ),
        PAYMENT_NATURE_CASH: (
            gsettings.gnucash_cash_account_name, 'Cash'
        )
    }

    # This generates better SQL than prefetch_related
    # on the InternalPayment table (gets all data in 1 query)
    # TODO optimise by deferring unnecessary fields
    # (probably requires something like django-seal to test properly)
    ts_range = (
        _dt_fallback(start), _dt_fallback(end, use_max=True)
    )
    if by_processed_ts:
        qs = InternalPaymentSplit.objects.filter(
            payment__processed__range=ts_range
        )
    else:
        qs = InternalPaymentSplit.objects.filter(
            payment__timestamp__range=ts_range
        )
    splits_to_process = qs.select_related(
        'debt', 'payment', 'payment__member',
        'debt__activity_participation__activity',
        'debt__activity_participation__activity__gnucash_category'
    )

    payments_by_nature = defaultdict(list)
    splits_by_payment = defaultdict(list)

    for s in splits_to_process:
        splits_by_payment[s.payment].append(s)

    for p in splits_by_payment.keys():
        payments_by_nature[p.nature].append(p)

    def format_transaction(payment, splits):
        yield 'D' + timezone.localdate(payment.timestamp).strftime('%d/%m/%y')
        yield 'T' + str(payment.total_amount.amount)
        yield 'M{last_name} {first_name}: {split_memos}'.format(
            last_name=payment.member.last_name.upper(),
            first_name=payment.member.first_name,
            split_memos=(
                multiple_debts_memo if len(splits) > 3 else
                ', '.join(split.debt.gnucash_memo for split in splits)
            )
        )

        total_amt = Decimal('0.00')
        for split in splits:
            debt = split.debt
            gnucash_category = debt.gnucash_category_string
            categories_seen.add(gnucash_category)
            amt = split.amount.amount
            total_amt += amt
            yield 'S' + gnucash_category
            yield 'E' + debt.gnucash_memo
            yield '$' + str(amt)

        # This should happen only rarely, but in any case
        # the treasurer needs to know about it,
        # so we add a split in the OVERPAID category
        # (at least this should be more predicable and easier to document
        # than explaining/figuring out how unbalanced transactions are handled
        # in GnuCash on .qif imports)
        remainder = payment.total_amount.amount - total_amt
        if remainder > 0:
            categories_seen.add(overpaid_category)
            yield 'S' + overpaid_category
            yield 'E' + overpaid_memo
            yield '$' + str(remainder)

        yield '^'
        yield ''

    def format_qif():
        # declare categories
        yield '!Type:Cat'
        for category in categories_seen:
            yield 'N' + category
            yield 'I'
            yield '^'
            yield ''

        # declare accounts + associated payments according to their natures
        for nature, (account, transaction_type) in accounts.items():
            yield '!Account'
            yield 'N' + account
            yield '^'
            yield '!Type:' + transaction_type
            for payment in payments_by_nature[nature]:
                yield from format_transaction(
                    payment, splits_by_payment[payment]
                )

    result = '\n'.join(format_qif())
    activate(old_lang)
    return result
