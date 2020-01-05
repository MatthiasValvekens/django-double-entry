import datetime
from unittest import mock

import pytz

from io import StringIO
from typing import List, Optional, Set

from django.test import TestCase
from djmoney.money import Money

from double_entry.forms.bulk_utils import (
    ResolvedTransaction,
    ResolvedTransactionMessageContext,
)
from double_entry.forms.csv import BankTransactionInfo
from double_entry.forms.utils import ErrorMixin
from . import models
from double_entry.forms import csv as forms_csv

# OGMs for all our test users in the database
SIMPLE_OGMS = ['+++190/5063/21290+++', '+++102/6984/10597+++', '+++191/8961/50327+++', '+++153/4339/48985+++', '+++192/0586/45737+++', '+++180/1915/47216+++', '+++160/9604/46545+++']
TICKET_OGMS = ['+++290/5063/21227+++', '+++202/6984/10534+++', '+++291/8961/50361+++', '+++253/4339/48922+++', '+++292/0586/45771+++', '+++280/1915/47250+++', '+++260/9604/46579+++']

# Quick descriptions (all real users have id 1)
#  2 Happy use case: real user, correct token (OK)
#  3 nonexistent pk (parse OK, lookup not OK)
#  4 real user, wrong token (parse OK, lookup not OK)
#  5 wrong check digits (both not OK)
#  6 totally nonsensical tracking number (both not OK)
#  7 in free-form column (OK)
KBC_SIMPLE_LOOKUP_TEST = """Rekeningnummer;Rubrieknaam;Naam;Munt;Afschriftnummer;Datum;Omschrijving;Valuta;Bedrag;Saldo;credit;debet;rekeningnummer tegenpartij;BIC tegenpartij;Naam tegenpartij;Adres tegenpartij;gestructureerde mededeling;Vrije mededeling
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***190/5063/21290***; 
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***154/5988/69980***;
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***176/0220/59911***;
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***190/5063/21291***;
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;;adlkfjasdlkj
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;;***190/5063/21290***
"""

# Results of the above parsing test (parsing failures eliminated)
# After lookup, we expect the following:
#  2 Happy use case: real user, correct token (OK)
#  3 nonexistent pk (not OK)
#  4 real user, wrong token (not OK)
#  7 (OK), result should coincide with line 2
PARSE_TEST_DATETIME = pytz.timezone('Europe/Brussels').localize(
    datetime.datetime(2019, 8, 8, 23, 59, 59, 999999)
)

SIMPLE_LOOKUP_TEST_POSTPARSE = [
    BankTransactionInfo(
        ln, Money(32, 'EUR'), PARSE_TEST_DATETIME, lookup_str,
    ) for ln, lookup_str in (
        (2, '+++190/5063/21290+++'), (3, '+++154/5988/69980+++'),
        (4, '+++176/0220/59911+++'), (7, '+++190/5063/21290+++')
    )
]

SIMPLE_LOOKUP_TEST_RESULT_DATA = {
    'transaction_party_id': 1, 'amount': Money(32, 'EUR'),
    'timestamp': PARSE_TEST_DATETIME,
}

class TestErrorMixin(ErrorMixin):

    def __init__(self, test_case: TestCase, expected_error_lines: Set[int]):
        self.test_case = test_case
        self.lines_with_errors: Set[int] = set()
        self.expected_errors = expected_error_lines

    def assert_errors(self):
        self.test_case.assertEqual(
            self.lines_with_errors, self.expected_errors
        )

    def error_at_line(self, line_no: int, msg: str,
                      params: Optional[dict] = None):
        self.lines_with_errors.add(line_no)

    def error_at_lines(self, line_nos: List[int], msg: str,
                       params: Optional[dict]=None):
        self.lines_with_errors.update(line_nos)


class TestBankCSVs(TestCase):
    fixtures = ['reservations.json', 'simple.json']

    def test_tracking_no_permanence(self):
        # I set the hidden tokens to be the same in both sets of fixtures
        # hence, the payment tracking numbers should be the same up to the
        #  first digit, and the check digits
        c1 = models.SimpleCustomer.objects.get(pk=1)
        c2 = models.TicketCustomer.objects.get(pk=1)
        self.assertEquals(c1.payment_tracking_no, '+++190/5063/21290+++')
        self.assertEquals(c2.payment_tracking_no, '+++290/5063/21227+++')
        c2.hidden_token = bytes.fromhex('deadbeefcafebabe')
        self.assertNotEqual(c2.hidden_token, '+++290/5063/21290+++')

        # these values are tightly coupled to the CSV testing data
        # so we should test that they stay the same
        simple_ogms = [c.payment_tracking_no for c in models.SimpleCustomer.objects.all().order_by('pk')]
        ticket_ogms = [c.payment_tracking_no for c in models.TicketCustomer.objects.all().order_by('pk')]
        self.assertEqual(len(simple_ogms), len(SIMPLE_OGMS))
        self.assertEqual(len(ticket_ogms), len(TICKET_OGMS))
        for ix, (val, exp) in enumerate(zip(simple_ogms, SIMPLE_OGMS)):
            with self.subTest(pk=ix + 1, scheme='simple'):
                self.assertEqual(val, exp)
        for ix, (val, exp) in enumerate(zip(ticket_ogms, TICKET_OGMS)):
            with self.subTest(pk=ix + 1, scheme='ticket'):
                self.assertEqual(val, exp)

    def test_simple_kbc_parse(self):
        parser = forms_csv.KBCCSVParser(StringIO(KBC_SIMPLE_LOOKUP_TEST))
        row: BankTransactionInfo
        rows: List[BankTransactionInfo] = parser.parsed_data
        self.assertEqual(
            {r.line_no for r in rows}, {2,3,4,7},
            msg='\n'.join(map(str, parser.parsed_data))
                + '\n'.join(map(str, parser.errors))
        )
        self.assertEqual(rows[0].account_id, (1, 1))
        self.assertEqual(rows[1].account_id, (1, 9000000))
        self.assertEqual(rows[2].account_id, (1, 1))
        self.assertEqual(rows[3].account_id, (1, 1))
        for row, exp_row in zip(rows, SIMPLE_LOOKUP_TEST_POSTPARSE):
            with self.subTest(line_no=row.line_no):
                self.assertEqual(row, exp_row)

    def test_simple_transfer_lookup(self):
        # the errors of the kind we expect here are not reported to the error
        # mixin, since the user shouldn't care about them anyway
        error_feedback = TestErrorMixin(
            test_case=self, expected_error_lines=set()
        )
        # instead, we hook into TransferTransactionIndexBuilder to
        # get feedback on the errors
        unseen_ogms = None
        def unseen_callback(_self, unseen):
            nonlocal unseen_ogms
            unseen_ogms = unseen

        resolver = models.SimpleTransferResolver(
            error_feedback, pipeline_section_id=-1
        )
        from double_entry.forms.transfers import TransferTransactionIndexBuilder
        mockery = mock.patch.object(
            TransferTransactionIndexBuilder, 'report_invalid_ogms',
            new=unseen_callback
        )
        with mockery:
            results = list(resolver(SIMPLE_LOOKUP_TEST_POSTPARSE))
        self.assertEqual(
            unseen_ogms, {'+++154/5988/69980+++', '+++176/0220/59911+++'}
        )
        self.assertEqual(len(results), 2)
        error_feedback.assert_errors()
        cust = models.SimpleCustomer.objects.get(pk=1)
        exp_result = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA, pipeline_section_id=-1,
            message_context=ResolvedTransactionMessageContext(),
            do_not_skip=False
        )
        self.assertEqual(results[0], (cust, exp_result))
        self.assertEqual(results[1], (cust, exp_result))

    def test_review_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA, pipeline_section_id=-1,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleTransferPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 0)
        self.assertEqual(len(error_context.transaction_errors), 0)
        self.assertEqual(len(prep.valid_transactions), 1)
        pt, = prep.valid_transactions
        le: models.SimpleCustomerPayment = pt.ledger_entry
        self.assertEqual(le.total_amount, resolved_transaction.amount)
        self.assertEqual(le.credit_remaining, Money(0, 'EUR'))

    def test_commit_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA, pipeline_section_id=-1,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleTransferPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects\
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)
