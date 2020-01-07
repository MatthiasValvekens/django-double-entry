from django.test import TestCase
from djmoney.money import Money
from double_entry.forms.bulk_utils import (
    ResolvedTransactionMessageContext,
    ResolvedTransaction,
    ResolvedTransactionVerdict,
)
from double_entry.models import GnuCashCategory
from tests import models
from tests.test_csv import PARSE_TEST_DATETIME, SIMPLE_LOOKUP_TEST_RESULT_DATA

SIMPLE_OVERPAID_CHECK = {
    'transaction_party_id': 1, 'amount': Money(40, 'EUR'),
    'timestamp': PARSE_TEST_DATETIME,
}


# TODO: test these with queryset sealing
# noinspection DuplicatedCode
class TestPreparators(TestCase):

    fixtures = ['reservations.json', 'simple.json']

    def test_review_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
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
        self.assertEqual(
            error_context.verdict, ResolvedTransactionVerdict.COMMIT
        )

    def test_commit_simple_resolved_transaction(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_LOOKUP_TEST_RESULT_DATA,
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

    def test_review_simple_resolved_transaction_paid_too_much(self):
        error_context = ResolvedTransactionMessageContext()
        resolved_transaction = ResolvedTransaction(
            **SIMPLE_OVERPAID_CHECK,
            message_context=error_context, do_not_skip=False
        )
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleTransferPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.review()
        self.assertEqual(len(error_context.transaction_warnings), 1)
        self.assertTrue('only 32.00' in error_context.transaction_warnings[0])
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
        prep = models.SimpleTransferPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
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
        prep = models.SimpleTransferPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        refund_cat = GnuCashCategory.get_category('refund')
        prep.refund_credit_gnucash_account = refund_cat
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1, is_refund=False)
        self.assertTrue(debt.paid)
        refund: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1, is_refund=True)
        self.assertTrue(refund.gnucash_category == refund_cat)
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
        prep = models.SimpleTransferPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleTransferPreparator(
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
        prep = models.SimpleTransferPreparator(
            resolved_transactions=[(cust, resolved_transaction)]
        )
        prep.commit()
        debt: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_payments().get(debtor_id=1)
        self.assertTrue(debt.paid)

        # reload to make sure
        cust = models.SimpleCustomer.objects.get(pk=1)
        prep = models.SimpleTransferPreparator(
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
        prep = models.SimpleTransferPreparator(
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
        prep = models.SimpleTransferPreparator(
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