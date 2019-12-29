from django.test import TestCase

from tests import models

class TestSimpleMagic(TestCase):

    def test_double_ledger_link(self):
        self.assertEqual(
            models.SimpleCustomerDebt.get_other_half_model(),
            models.SimpleCustomerPayment
        )
        self.assertEqual(
            models.SimpleCustomerPayment.get_other_half_model(),
            models.SimpleCustomerDebt
        )

    def test_ledger_split_link(self):
        self.assertEqual(
            models.SimpleCustomerDebt.get_split_model(),
            (models.SimpleCustomerPaymentSplit, 'debt')
        )
        self.assertEqual(
            models.SimpleCustomerPayment.get_split_model(),
            (models.SimpleCustomerPaymentSplit, 'payment')
        )

    def test_split_manager_link(self):
        # TODO: need instances to test public API
        self.assertEqual(
            models.SimpleCustomerDebt._split_manager_name, 'debt_splits'
        )

        self.assertEqual(
            models.SimpleCustomerPayment._split_manager_name, 'payment_splits'
        )

    def test_customer_ledger_links(self):
        self.assertEqual(
            models.SimpleCustomer.get_debt_model(),
            models.SimpleCustomerDebt
        )
        self.assertEqual(
            models.SimpleCustomer.get_payment_model(),
            models.SimpleCustomerPayment
        )
        self.assertEqual(
            models.SimpleCustomer.get_split_model(),
            models.SimpleCustomerPaymentSplit
        )
        self.assertEqual(
            models.SimpleCustomer.get_debt_remote_fk(), 'debtor'
        )
        self.assertEqual(
            models.SimpleCustomer.get_payment_remote_fk(), 'creditor'
        )
        self.assertEqual(
            models.SimpleCustomer.get_debt_remote_fk_column(), 'debtor_id'
        )
        self.assertEqual(
            models.SimpleCustomer.get_payment_remote_fk_column(), 'creditor_id'
        )
