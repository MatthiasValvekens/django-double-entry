import json
from copy import deepcopy

from django.test import TestCase
from djmoney.money import Money

from double_entry.forms import bulk_utils
from double_entry.forms.bulk_utils import (
    ResolvedTransactionMessageContext,
    ResolvedTransaction,
    ResolvedTransactionVerdict,
)
from . import models, views as test_views
from .test_csv import PARSE_TEST_DATETIME, SIMPLE_LOOKUP_TEST_RESULT_DATA

SIMPLE_OVERPAID_CHECK = {
    'transaction_party_id': 1, 'amount': Money(40, 'EUR'),
    'timestamp': PARSE_TEST_DATETIME,
}

PIPELINE_SIMPLE_SECTION = 0
PIPELINE_TICKET_SECTION = 1


# TODO: figure out a way to create skippable tests for the postgres only stuff
# TODO: test these with queryset sealing
# noinspection DuplicatedCode
class TestSimplePreparator(TestCase):

    fixtures = ['simple.json']

    def test_refund_nocredit(self):
        pmt = models.SimpleCustomerPayment(
            creditor_id=1, total_amount=Money(8, 'EUR')
        )
        pmt.spoof_matched_balance(Money(8, 'EUR'))
        self.assertIsNone(bulk_utils.refund_overpayment([pmt]))

    def test_review_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
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
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.COMMIT
        )

    def test_review_simple_resolved_transaction_exactonly(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.exact_amount_match_only = True
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 0)
        self.assertEqual(len(error_context.transaction_errors), 0)
        self.assertEqual(len(prep.valid_transactions), 1)
        pt, = prep.valid_transactions
        le: models.SimpleCustomerPayment = pt.ledger_entry
        self.assertEqual(le.total_amount, resolved_transaction.amount)
        self.assertEqual(le.credit_remaining, Money(0, 'EUR'))
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.COMMIT
        )

    def test_review_simple_resolved_transaction_underpay(self):
        error_context = ResolvedTransactionMessageContext()
        data = deepcopy(SIMPLE_LOOKUP_TEST_RESULT_DATA)
        data['amount'] = Money(8, 'EUR')
        resolved_transaction = ResolvedTransaction(
            **data, message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
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
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.COMMIT
        )

    def test_review_simple_resolved_transaction_underpay_exactonly(self):
        error_context = ResolvedTransactionMessageContext()
        data = deepcopy(SIMPLE_LOOKUP_TEST_RESULT_DATA)
        data['amount'] = Money(8, 'EUR')
        resolved_transaction = ResolvedTransaction(
            **data, message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.exact_amount_match_only = True
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 1)
        self.assertTrue('only €0.00' in error_context.transaction_warnings[0])
        self.assertEqual(len(prep.valid_transactions), 1)
        pt, = prep.valid_transactions
        le: models.SimpleCustomerPayment = pt.ledger_entry
        self.assertEqual(le.total_amount, resolved_transaction.amount)
        self.assertEqual(le.credit_remaining, Money(8, 'EUR'))
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.COMMIT
        )

    def test_commit_simple_resolved_transaction_underpay(self):
        error_context = ResolvedTransactionMessageContext()
        data = deepcopy(SIMPLE_LOOKUP_TEST_RESULT_DATA)
        data['amount'] = Money(8, 'EUR')
        resolved_transaction = ResolvedTransaction(
            **data, message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertEqual(debt.balance, Money('24', 'EUR'))
        le: models.SimpleCustomerPayment = models.SimpleCustomerPayment.objects \
            .with_debts().get(creditor_id=1)
        self.assertEqual(le.total_amount, resolved_transaction.amount)
        self.assertEqual(le.credit_remaining, Money(0, 'EUR'))

    def test_commit_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects\
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

    def test_review_simple_resolved_transaction_paid_too_much(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_OVERPAID_CHECK,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 1)
        self.assertTrue('only €32.00' in error_context.transaction_warnings[0])
        self.assertEqual(len(error_context.transaction_errors), 0)
        self.assertEqual(len(prep.valid_transactions), 1)
        pt, = prep.valid_transactions
        le: models.SimpleCustomerPayment = pt.ledger_entry
        self.assertEqual(le.total_amount, resolved_transaction.amount)
        self.assertEqual(le.credit_remaining, Money(8, 'EUR'))

    def test_commit_simple_resolved_transaction_paid_too_much_norefund(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_OVERPAID_CHECK,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.no_refunds = True
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1, is_refund=False)
        self.assertTrue(debt.paid)
        refund_exists = models.SimpleCustomerDebt.objects \
            .filter(debtor_id=1, is_refund=True).exists()
        self.assertFalse(refund_exists)
        pmt: models.SimpleCustomerPayment = models.SimpleCustomerPayment.objects \
            .with_debts().get(creditor_id=1)
        self.assertFalse(pmt.fully_matched)
        self.assertEqual(pmt.credit_remaining, Money(8, 'EUR'))

    def test_commit_simple_resolved_transaction_paid_too_much_refund(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_OVERPAID_CHECK,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1, is_refund=False)
        self.assertTrue(debt.paid)
        refund: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1, is_refund=True)
        self.assertEquals(refund.gnucash_category.name, 'refund')
        self.assertTrue(refund.fully_matched)
        pmt: models.SimpleCustomerPayment = models.SimpleCustomerPayment.objects \
            .with_debts().get(creditor_id=1)
        self.assertTrue(pmt.fully_matched)

    def test_commit_twice_review(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.SUGGEST_DISCARD
        )

    def test_commit_twice_noforce(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.SUGGEST_DISCARD
        )

        pmt_count = models.SimpleCustomerPayment.objects.filter(
            creditor_id=1
        ).count()
        self.assertEqual(pmt_count, 1)

    def test_commit_twice_withforce(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.SimpleCustomer.objects.get(pk=1)
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=True,
        )
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.SUGGEST_DISCARD
        )

        pmt_count = models.SimpleCustomerPayment.objects.filter(
            creditor_id=1
        ).count()
        self.assertEqual(pmt_count, 2)

    def test_duplicate_review_with_prehist(self):
        pmt = models.SimpleCustomerPayment(
            creditor_id=1, total_amount=Money(32, 'EUR'),
            timestamp=PARSE_TEST_DATETIME
        )
        pmt.save()
        split = models.SimpleCustomerPaymentSplit(
            payment=pmt, debt_id=1, amount=pmt.total_amount
        )
        split.save()
        error_context1 = ResolvedTransactionMessageContext()
        error_context2 = ResolvedTransactionMessageContext()
        resolved_transaction1 = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context1, do_not_skip=False
        )
        resolved_transaction2 = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context2, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[
                (cust, resolved_transaction1),
                (cust, resolved_transaction2)
            ]
        )
        prep.review()
        self.assertEqual(
            {error_context1.verdict, error_context2.verdict},
            {ResolvedTransactionVerdict.SUGGEST_DISCARD, ResolvedTransactionVerdict.COMMIT}
        )

    def test_duplicate_commit_with_prehist(self):
        pmt = models.SimpleCustomerPayment(
            creditor_id=1, total_amount=Money(32, 'EUR'),
            timestamp=PARSE_TEST_DATETIME
        )
        pmt.save()
        split = models.SimpleCustomerPaymentSplit(
            payment=pmt, debt_id=1, amount=pmt.total_amount
        )
        split.save()
        error_context1 = ResolvedTransactionMessageContext()
        error_context2 = ResolvedTransactionMessageContext()
        resolved_transaction1 = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context1, do_not_skip=False
        )
        resolved_transaction2 = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context2, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[
                (cust, resolved_transaction1),
                (cust, resolved_transaction2)
            ]
        )
        prep.commit()
        self.assertEqual(
            {error_context1.verdict, error_context2.verdict},
            {ResolvedTransactionVerdict.SUGGEST_DISCARD, ResolvedTransactionVerdict.COMMIT}
        )
        pmt_count = models.SimpleCustomerPayment.objects.filter(creditor_id=1).count()
        self.assertEqual(pmt_count, 2)

    def test_negative_amount(self):
        # includes an irrelevant field
        error_context = ResolvedTransactionMessageContext()
        data = deepcopy(SIMPLE_LOOKUP_TEST_RESULT_DATA)
        data['amount'].amount *= -1
        resolved_transaction = models.ResolvedTransaction(
            **data, message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleGenericPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 0)
        self.assertEqual(len(error_context.transaction_errors), 1)
        self.assertTrue('negative' in error_context.transaction_errors[0])
        self.assertEqual(len(prep.valid_transactions), 0)
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.DISCARD
        )


# noinspection DuplicatedCode
class TestReservationPreparator(TestCase):

    fixtures = ['reservations.json']

    def test_review_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 0)
        self.assertEqual(len(error_context.transaction_errors), 0)
        self.assertEqual(len(prep.valid_transactions), 1)
        pt, = prep.valid_transactions
        le: models.ReservationPayment = pt.ledger_entry
        self.assertEqual(le.total_amount, resolved_transaction.amount)
        self.assertEqual(le.credit_remaining, Money(0, 'EUR'))
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.COMMIT
        )

    def test_commit_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.ReservationDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1)
        self.assertTrue(debt.paid)

    def test_review_simple_resolved_transaction_paid_too_much(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_OVERPAID_CHECK,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 1)
        self.assertTrue('only €32.00' in error_context.transaction_warnings[0])
        self.assertEqual(len(error_context.transaction_errors), 0)
        self.assertEqual(len(prep.valid_transactions), 1)
        pt, = prep.valid_transactions
        le: models.ReservationPayment = pt.ledger_entry
        self.assertEqual(le.total_amount, resolved_transaction.amount)
        self.assertEqual(le.credit_remaining, Money(8, 'EUR'))

    def test_commit_simple_resolved_transaction_paid_too_much_norefund(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_OVERPAID_CHECK,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.no_refunds = True
        prep.commit()
        debt: models.ReservationDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1, is_refund=False)
        self.assertTrue(debt.paid)
        refund_exists = models.ReservationDebt.objects \
            .filter(owner_id=1, is_refund=True).exists()
        self.assertFalse(refund_exists)
        pmt: models.ReservationPayment = models.ReservationPayment.objects \
            .with_debts().get(customer_id=1)
        self.assertFalse(pmt.fully_matched)
        self.assertEqual(pmt.credit_remaining, Money(8, 'EUR'))

    def test_commit_simple_resolved_transaction_paid_too_much_refund(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_OVERPAID_CHECK,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.ReservationDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1, is_refund=False)
        self.assertTrue(debt.paid)
        refund: models.ReservationDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1, is_refund=True)
        self.assertEquals(refund.gnucash_category.name, 'refund')
        self.assertTrue(refund.fully_matched)
        pmt: models.ReservationPayment = models.ReservationPayment.objects \
            .with_debts().get(customer_id=1)
        self.assertTrue(pmt.fully_matched)

    def test_commit_twice_review(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.ReservationDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.SUGGEST_DISCARD
        )

    def test_commit_twice_noforce(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.ReservationDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.SUGGEST_DISCARD
        )

        pmt_count = models.ReservationPayment.objects.filter(
            customer_id=1
        ).count()
        self.assertEqual(pmt_count, 1)

    def test_commit_twice_withforce(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=False
        )
        cust = models.TicketCustomer.objects.get(pk=1)
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.ReservationDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.TicketCustomer.objects.get(pk=1)
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
            message_context=error_context, do_not_skip=True,
        )
        prep = models.ReservationPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.SUGGEST_DISCARD
        )

        pmt_count = models.ReservationPayment.objects.filter(
            customer_id=1
        ).count()
        self.assertEqual(pmt_count, 2)

# noinspection DuplicatedCode
class TestSubmissionAPI(TestCase):

    fixtures = ['reservations.json', 'simple.json']

    @classmethod
    def setUpTestData(cls):
        cls.endpoint = test_views.pipeline_endpoint.url()
        cls.alt_endpoint = test_views.AltPipelineEndpoint.url()

    def test_simple_submission(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '32.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertTrue(response_payload['all_committed'])
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )
        self.assertTrue(res['committed'])
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

    def test_simple_submission_alt_2(self):
        response = self.client.post(
            self.alt_endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '32.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'boolean_field_test': False,
                        'integer_field_test': 1,
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertTrue(response_payload['all_committed'])
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )
        self.assertTrue(res['committed'])
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

    def test_simple_submission_alt(self):
        response = self.client.post(
            self.alt_endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '32.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'boolean_field_test': 'fAlSe',
                        'integer_field_test': '1',
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertTrue(response_payload['all_committed'])
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )
        self.assertTrue(res['committed'])
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

    def test_simple_submission_review(self):
        response = self.client.post(
            self.endpoint, data={
                'commit': False,
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '32.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 200)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertFalse(debt.paid)
        self.assertFalse(
            models.SimpleCustomerPayment.objects.filter(creditor_id=1).exists()
        )

    def test_simple_submission_dateonly(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': '2019-08-08',
                        'amount': '32.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertTrue(response_payload['all_committed'])
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )
        self.assertTrue(res['committed'])
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

    def test_simple_ticket_submission(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-1-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '32.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_TICKET_SECTION,
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertTrue(response_payload['all_committed'])
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )
        self.assertTrue(res['committed'])
        debt: models.SimpleCustomerDebt = models.ReservationDebt.objects \
            .with_payments().get(owner_id=1)
        self.assertTrue(debt.paid)

    def test_simple_submission_overpay(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '50.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertTrue(response_payload['all_committed'])
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )
        self.assertTrue(res['committed'])
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1, is_refund=False)
        self.assertTrue(debt.paid)
        refund: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1, is_refund=True)
        self.assertEqual(refund.total_amount, Money(18, 'EUR'))
        self.assertTrue(refund.paid)

    def test_simple_submission_negative(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '-50.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_alt_insuff(self):
        response = self.client.post(
            self.alt_endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '50.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_alt_insuff_2(self):
        response = self.client.post(
            self.alt_endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'amount': '32.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'boolean_field_test': False,
                        'integer_field_test': 1
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_no_currency(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '50.00',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_bad_amount(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '50.00 EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_no_section_id(self):
        # TODO: add test for single-section pipeline, which should work
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '50.00',
                        'currency': 'EUR',
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_no_timestamp(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'amount': '50.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_no_timestamp_typo(self):
        # test underdefined/overdefined scenario
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'amount': '50.00',
                        'timestapm': PARSE_TEST_DATETIME,
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_too_many_fields(self):
        # test underdefined/overdefined scenario
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'amount': '50.00',
                        'timestamp': PARSE_TEST_DATETIME,
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True,  # this should not work
                        'nonsense_field': 0
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_simple_submission_illegal_timestamp(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'transaction_party_id': 1,
                        'timestamp': '2018/08/08',
                        'amount': '50.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_submission_bad_transaction_list(self):
        formally_ok = {
            'transaction_id': 'sec-0-trans-0',
            'transaction_party_id': 1,
            'timestamp': '2018/08/08',
            'amount': '50.00',
            'currency': 'EUR',
            'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
            'do_not_skip': True  # this should not work
        }
        response = self.client.post(
            self.endpoint, data={
                'transactions': [ 0, 'abc', formally_ok]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 400)

    def test_simple_submission_no_trans_id(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_party_id': 1,
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '50.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 400)
        # this fails with an API error, because without transaction IDs
        #  there's no meaningful way to inform the user about what went wrong
        #  with a specific transaction.

    def test_simple_submission_no_account_id(self):
        response = self.client.post(
            self.endpoint, data={
                'transactions': [
                    {
                        'transaction_id': 'sec-0-trans-0',
                        'timestamp': PARSE_TEST_DATETIME,
                        'amount': '50.00',
                        'currency': 'EUR',
                        'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
                        'do_not_skip': True  # this should not work
                    }
                ]
            }, content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.DISCARD
        )

    def test_commit_twice_noforce(self):
        base_data = {
            'transaction_party_id': 1,
            'timestamp': PARSE_TEST_DATETIME,
            'amount': '32.00',
            'currency': 'EUR',
            'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
        }
        # change up the transaction id for good measure, but it shouldn't matter
        trans1 = {
            'transaction_id': 'adsflkajsd',  **base_data
        }
        trans2 = {
            'transaction_id': 'blalalal',  **base_data
        }
        response = self.client.post(
            self.endpoint, data={ 'transactions': [ trans1 ] },
            content_type='application/json'
        )

        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )

        response = self.client.post(
            self.endpoint, data={ 'transactions': [ trans2 ] },
            content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.SUGGEST_DISCARD
        )
        self.assertFalse(res['committed'])
        pmt_count = models.SimpleCustomerPayment.objects.filter(
            creditor_id=1
        ).count()
        self.assertEqual(pmt_count, 1)

    def test_commit_twice_with_force(self):
        base_data = {
            'transaction_party_id': 1,
            'timestamp': PARSE_TEST_DATETIME,
            'amount': '32.00',
            'currency': 'EUR',
            'pipeline_section_id': PIPELINE_SIMPLE_SECTION,
        }
        # change up the transaction id for good measure, but it shouldn't matter
        trans1 = {
            'transaction_id': 'adsflkajsd',  **base_data
        }
        trans2 = {
            'transaction_id': 'blalalal',  'do_not_skip': True,
            **base_data
        }
        response = self.client.post(
            self.endpoint, data={ 'transactions': [ trans1 ] },
            content_type='application/json'
        )

        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.COMMIT
        )

        response = self.client.post(
            self.endpoint, data={ 'transactions': [ trans2 ] },
            content_type='application/json'
        )
        self.assertEquals(response.status_code, 201)
        response_payload = json.loads(response.content)
        self.assertEqual(len(response_payload['pipeline_responses']), 1)
        res = response_payload['pipeline_responses'][0]
        self.assertEqual(
            res['verdict'], bulk_utils.ResolvedTransactionVerdict.SUGGEST_DISCARD
        )
        self.assertTrue(res['committed'])
        pmt_count = models.SimpleCustomerPayment.objects.filter(
            creditor_id=1
        ).count()
        self.assertEqual(pmt_count, 2)

