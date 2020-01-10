from django.test import TestCase
from djmoney.money import Money

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
            models.SimpleCustomerDebt.split_manager_name, 'debt_splits'
        )

        self.assertEqual(
            models.SimpleCustomerPayment.split_manager_name, 'payment_splits'
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


class TestComplexMagic(TestCase):

    def test_double_ledger_link(self):
        self.assertEqual(
            models.ReservationDebt.get_other_half_model(),
            models.ReservationPayment
        )
        self.assertEqual(
            models.ReservationPayment.get_other_half_model(),
            models.ReservationDebt
        )
        self.assertEqual(
            models.Reservation.get_other_half_model(),
            models.ReservationPayment
        )

    def test_ledger_split_link(self):
        self.assertEqual(
            models.ReservationDebt.get_split_model(),
            (models.ReservationPaymentSplit, 'reservation')
        )
        self.assertEqual(
            models.ReservationPayment.get_split_model(),
            (models.ReservationPaymentSplit, 'payment')
        )

    def test_split_manager_link(self):
        # TODO: need instances to test public API
        self.assertEqual(
            models.ReservationDebt.split_manager_name, 'splits'
        )

        self.assertEqual(
            models.ReservationPayment.split_manager_name, 'splits'
        )

    def test_customer_ledger_links(self):
        self.assertEqual(
            models.TicketCustomer.get_debt_model(),
            models.ReservationDebt
        )
        self.assertEqual(
            models.TicketCustomer.get_payment_model(),
            models.ReservationPayment
        )
        self.assertEqual(
            models.TicketCustomer.get_split_model(),
            models.ReservationPaymentSplit
        )
        self.assertEqual(
            models.TicketCustomer.get_debt_remote_fk(), 'owner'
        )
        self.assertEqual(
            models.TicketCustomer.get_payment_remote_fk(), 'customer'
        )
        self.assertEqual(
            models.TicketCustomer.get_debt_remote_fk_column(), 'owner_id'
        )
        self.assertEqual(
            models.TicketCustomer.get_payment_remote_fk_column(), 'customer_id'
        )
