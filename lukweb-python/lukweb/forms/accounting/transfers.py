import logging
import re
from collections import defaultdict
from decimal import Decimal
from itertools import chain

from django.utils.translation import (
    ugettext_lazy as _,
)
from djmoney.money import Money

from ... import payments, models
from . import internal, ticketing
from .bulk_utils import PaymentCSVParser
from ..utils import ParserErrorMixin
from django.conf import settings

logger = logging.getLogger(__name__)

__all__ = [
    'ElectronicPaymentPopulator', 'BankCSVParser'
]


# TODO: implement KBC parser
# TODO: implement parser switching in globals
# TODO: clearly document parsers
# TODO: delimiter autodetection

class BankCSVParser(PaymentCSVParser):
    
    class TransactionInfo(PaymentCSVParser.TransactionInfo): 
        def __init__(self, *, ogm, **kwargs):
            super().__init__(**kwargs)
            self.ogm = ogm

    def get_nature(self, line_no, row):
        return payments.PAYMENT_NATURE_TRANSFER

    def get_ogm(self, line_no, row):
        raise NotImplementedError

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        ogm = self.get_ogm(line_no, row)
        if ogm is None:
            return None
        parsed['ogm'] = ogm
        return parsed

# lookbehind doesn't work, since we don't want to constrain the
# prefix to a fixed length
FORTIS_FIND_OGM = r'MEDEDELING\s*:\s+' + payments.OGM_REGEX
FORTIS_SEARCH_PATTERN = re.compile(FORTIS_FIND_OGM)

class FortisCSVParser(BankCSVParser):
    delimiter = ';'

    # TODO: force all relevant columns to be present here
    amount_column_name = 'Bedrag'
    date_column_name = 'Uitvoeringsdatum'

    def get_ogm(self, line_no, row):
        m = FORTIS_SEARCH_PATTERN.search(row['Details'])
        if m is None:
            return None
        ogm_str = m.group(0)
        try:
            prefix, modulus = payments.parse_ogm(ogm_str, match=m)
        except (ValueError, TypeError):
            self.error(
                line_no, 
                _('Illegal OGM string %(ogm)s.') % {
                    'ogm': ogm_str
                }
            )
            return None

        ogm_canonical = payments.ogm_from_prefix(prefix)
        return ogm_canonical

class ElectronicPaymentPopulator(ParserErrorMixin):
    DEBT_TRANSFER_PREFIX = 'bulk-debt-transfers'
    RESERVATION_TRANSFER_PREFIX = 'reservation-transfers'

    def __init__(self, user, transfer_parser):
        super().__init__(transfer_parser)
        self.allowed_prefixes \
            = payments.check_payment_change_permissions(user)

        if transfer_parser is not None:
            transfer_data = transfer_parser.parsed_data
        else:
            transfer_data = []
        buckets = defaultdict(list)
        for t in transfer_data:
            prefix, modulus = payments.parse_ogm(
                # already validated
                t.ogm, validate=False
            )

            kind_prefix = str(prefix)[0]
            if not (kind_prefix in payments.VALID_OGM_PREFIXES):
                self.unknown_ogm(t.ogm, [t.line_no])
            elif not (kind_prefix in self.allowed_prefixes):
                self.permission_denied(t)
            else:
                buckets[kind_prefix].append(t) 

        reserv_qs, reserv_ix = self._prepare_reservations(
            buckets[payments.OGM_RESERVATION_PREFIX]
        )

        self.reservation_formset = ticketing.ReservationPaymentFormSet(
            queryset=reserv_qs,
            prefix=self.RESERVATION_TRANSFER_PREFIX,
        )
        
        member_qs, internaldebt_ix = self._prepare_internal(
            buckets[payments.OGM_INTERNAL_DEBT_PREFIX]
        )

        # prepare duplicate checking
        historical_buckets = internal.bucket_transaction_history(
            payments.PAYMENT_NATURE_TRANSFER,
            buckets[payments.OGM_INTERNAL_DEBT_PREFIX]
        )

        # also compute total contribution to 
        # check for overpayments later
        self.debt_contributions = {}

        def populate_debt_formset(member):
            tinfos = internaldebt_ix[member.payment_tracking_no]
            dupcheck = defaultdict(list)
            total_contribution = Money(
                Decimal('0.00'), settings.BOOKKEEPING_CURRENCY
            )
            for tinfo in tinfos:
                k = internal.bucket_key(member, tinfo)
                occ_so_far = dupcheck[k]
                occ_so_far.append(tinfo)
                # ok, we've DEFINITELY not seen this one before
                if len(occ_so_far) > historical_buckets[k]:
                    total_contribution += tinfo.amount
                    yield {
                        'nature':
                            payments.PAYMENT_NATURE_TRANSFER,
                        'email': member.user.email,
                        'member_id': member.pk,
                        'ogm': tinfo.ogm,
                        'name': member.full_name,
                        'total_amount': tinfo.amount,
                        'timestamp': tinfo.timestamp
                    }

            # save to debt_contributions
            self.debt_contributions[member] = total_contribution
            # finally report on possible duplicates
            internal.do_dupcheck(
                self.error_at_lines,
                member, historical_buckets, dupcheck
            )

        initial_data = list(chain(*map(populate_debt_formset, member_qs)))
        self.debt_formset = internal.BulkPaymentFormSet(
            queryset=models.InternalPayment.objects.none(),
            initial=initial_data,
            prefix=self.DEBT_TRANSFER_PREFIX
        )
        self.debt_formset.extra = len(initial_data)

    def _prepare_reservations(self, reserv_payments):
        # eliminate duplicates
        reservation_ogm_index_dupes = defaultdict(list)
        for t in reserv_payments:
            # pure laziness, since we could extract obfuscated_id to avoid 
            # some double work, but this kind of string manipulation
            # is not worth optimising (Amdahl)
            reservation_ogm_index_dupes[t.ogm].append(t)

        # build a transaction index indexed by ogm
        reservation_ogm_index = {}
        for ogm, lst in reservation_ogm_index_dupes.items():
            if len(lst) > 1:
                self.duplicate(ogm, (t.line_no for t in lst))
            else:
                reservation_ogm_index[ogm] = lst[0]

        # fetch database records in bulk 
        reserv_qs, unseen = models.Reservation.objects.by_payment_tracking_nos(
            reservation_ogm_index.keys(), validate_unseen=True
        )

        for ogm in unseen:
            self.unknown_ogm(ogm, [reservation_ogm_index[ogm].line_no])

        # verify transaction amounts
        def correct_transactions():
            for r in reserv_qs:
                tinfo = reservation_ogm_index[r.payment_tracking_no]
                if r.total_price != tinfo.amount:
                    self.wrong_amount(tinfo, r)
                yield r.pk

        reserv_qs = reserv_qs.filter(pk__in=list(correct_transactions()))
        # how do we deal with those?
        # might require overriding the @forms property on the formset
        return reserv_qs, reservation_ogm_index

    def _prepare_internal(self, internal_transfers):
        internal_debt_ogm_index = defaultdict(list)
        for t in internal_transfers:
            if t.amount.amount < 0:
                self.negative_amount(t)
                continue
            internal_debt_ogm_index[t.ogm].append(t)

        member_qs, unseen = models.ChoirMember.objects.with_debt_balances()\
            .by_payment_tracking_nos(
                internal_debt_ogm_index.keys(), validate_unseen=True
            )

        for ogm in unseen:
            ts = internal_debt_ogm_index[ogm]
            self.unknown_ogm(ogm, [t.line_no for t in ts])

        return member_qs, internal_debt_ogm_index

    def error_at_line(self, line_no, ogm, msg, params=None):
        self.error_at_lines([line_no], ogm, msg, params)

    def error_at_lines(self, line_nos, ogm, msg, params=None):
        if params is None:
            params = {'ogm': ogm}
        else:
            params['ogm'] = ogm

        fmtd_msg = msg % params
        self._errors.insert(0, (sorted(line_nos), fmtd_msg))

    def duplicate(self, ogm, line_nos):
        msg = _(
            'Transaction %(ogm)s occurs multiple times. Skipped processing.'
        )
        self.error_at_lines(
            line_nos, ogm, msg
        )

    def wrong_amount(self, t, reservation):
        msg = _(
            'Reservation payment %(ogm)s has the wrong amount. '
            'Expected %(actual_price)s but got %(amt)s. '
            'Skipped processing; please follow up manually.'
        )

        self.error_at_line(
            t.line_no, t.ogm, msg, params={
                'amt': t.amount,
                'actual_price': reservation.total_price
            }
        )

    def negative_amount(self, t):
        msg = _(
            'Transfer %(ogm)s is negative: %(amt)s.'
        )

        self.error_at_line(
            t.line_no, t.ogm, msg, params={
                'amt': t.amount,
            }
        )

    @staticmethod
    def unknown_ogm(ogm, line_nos):
        # We don't show this error in the interface, since
        # if the OGM validates properly AND is not found in our system,
        # it probably simply corresponds to a transaction that we don't
        # care about
        logger.debug(
            'Unknown ogm prefix in %s at line(s) %s', ogm, str(line_nos)
        )

    def permission_denied(self, t):
        self.error_at_line(
            t.line_no, t.ogm,
            _(
                'You do not have sufficient permissions to access %(ogm)s.'
            ),
        )

    def verify_amount(self, t, expected):
        if expected == t.amount:
            return True

        self.error_at_line(
            t.line_no, t.ogm,
            _(
                'Transaction %(ogm)s has amount %(trans_amt)s, '
                'expected %(expected_amt)s.'
            ),
            params={
                'expected_amt': expected,
                'trans_amt': t.amount
            }
        )
        return False
