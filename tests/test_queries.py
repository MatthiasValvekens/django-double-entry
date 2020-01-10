import datetime
from decimal import Decimal

import pytz
from django.test import TestCase
from djmoney.money import Money

from tests import models

FIXTURE_EVENT_PK = 1
FIXTURE_UNPAID_PK = 1
FIXTURE_PERFECT_PK = 2
FIXTURE_STATIC_PRICE_PK = 3
FIXTURE_COMPLEX_PARTIALLY_PAID_PK = 4
# situation:
#  reservation 5: 3x cat 1 (12 eur)
#  reservation 4: 10x cat 1 + 2x cat 2 (60 eur)
#  payment 4: 44 eur, split 12 / 32 accross 5 and 4
#  payment 5: 18 eur, all towards reservation 4
#  effect: reservation 5 is fully paid off, while reservation 4 is not
FIXTURE_COMPLEX_FULLY_PAID_PK = 5
FIXTURE_COMPLEX_BIG_PAYMENT_PK = 4
FIXTURE_COMPLEX_SMALL_PAYMENT_PK = 5
FIXTURE_PAID_TOO_MUCH_PK = 6
FIXTURE_PAID_TOO_MUCH_AND_TOO_LITTLE = 7


class TestSimplePaymentQueries(TestCase):
    fixtures = ['simple.json']

    def test_simple_unpaid(self):
        r: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_fully_matched_date().get(
            pk=FIXTURE_UNPAID_PK
        )
        self.assertTrue(r.balance)
        self.assertEquals(r.balance, r.total_amount)
        self.assertEquals(r.fully_matched_date, None)


    def test_perfect_payment(self):
        r: models.SimpleCustomerDebt = models.SimpleCustomerDebt.objects \
            .with_fully_matched_date().get(
            pk=FIXTURE_PERFECT_PK
        )
        self.assertFalse(r.balance)
        self.assertEquals(
            r.fully_matched_date, datetime.datetime(
                2019, 8, 8, 0, 15, 0, tzinfo=pytz.utc
            )
        )
        r: models.SimpleCustomerPayment = models.SimpleCustomerPayment.objects \
            .with_fully_matched_date().get(pk=FIXTURE_PERFECT_PK)
        self.assertFalse(r.credit_remaining)


    def test_fully_paid_easy(self):
        fully_paid_bare = models.SimpleCustomerDebt.objects.get(pk=2)
        lazy_result = fully_paid_bare.matched_balance
        fully_paid_prepped = models.SimpleCustomerDebt.objects.with_remote_accounts().get(pk=2)
        eager_result = fully_paid_prepped.matched_balance
        self.assertEqual(lazy_result, Money(24, 'EUR'))
        self.assertEqual(eager_result, Money(24, 'EUR'))
        self.assertTrue(fully_paid_bare.paid)
        self.assertTrue(fully_paid_prepped.paid)

    def test_fully_paid_complex(self):
        fully_paid_bare = models.SimpleCustomerDebt.objects.get(pk=5)
        lazy_result = fully_paid_bare.matched_balance
        fully_paid_prepped = models.SimpleCustomerDebt.objects.with_remote_accounts().get(pk=5)
        eager_result = fully_paid_prepped.matched_balance
        self.assertEqual(lazy_result, Money(12, 'EUR'))
        self.assertEqual(eager_result, Money(12, 'EUR'))
        self.assertTrue(fully_paid_bare.paid)
        self.assertTrue(fully_paid_prepped.paid)

    def test_spoof(self):
        fully_paid_prepped = models.SimpleCustomerDebt.objects.with_remote_accounts().get(pk=2)
        eager_result = fully_paid_prepped.matched_balance
        eager_result_unm = fully_paid_prepped.unmatched_balance
        self.assertEqual(eager_result, Money(24, 'EUR'))
        self.assertEqual(eager_result_unm, Money(0, 'EUR'))
        fully_paid_prepped.spoof_matched_balance(11)
        self.assertEqual(fully_paid_prepped.matched_balance, Money(11, 'EUR'))
        self.assertEqual(fully_paid_prepped.unmatched_balance, Money(13, 'EUR'))


class TestReservationPaymentQueries(TestCase):
    fixtures = ['reservations.json']

    def test_simple_unpaid_reservation(self):
        r: models.ReservationDebt = models.ReservationDebt.objects \
            .with_total_price().with_fully_matched_date().get(
            pk=FIXTURE_UNPAID_PK
        )
        self.assertTrue(r.balance)
        self.assertEquals(r.balance, r.total_amount)
        self.assertEquals(r.fully_matched_date, None)
        r: models.Reservation = models.Reservation.objects \
            .with_total_price().with_fully_matched_date().get(
            pk=FIXTURE_UNPAID_PK
        )
        self.assertTrue(r.balance)
        self.assertEquals(r.balance, r.total_amount)
        self.assertEquals(r.fully_matched_date, None)


    def test_perfect_reservation(self):
        r: models.ReservationDebt = models.ReservationDebt.objects\
            .with_total_price().with_remote_accounts().get(
                pk=FIXTURE_PERFECT_PK
            )
        self.assertFalse(r.balance)
        self.assertEquals(
            r.fully_matched_date, datetime.datetime(
                2019, 8, 8, 0, 15, 0, tzinfo=pytz.utc
            )
        )
        r: models.Reservation = models.Reservation.objects \
            .with_total_price().with_remote_accounts().get(
                pk=FIXTURE_PERFECT_PK
            )
        self.assertFalse(r.balance)
        r: models.ReservationPayment = models.ReservationPayment.objects \
            .with_remote_accounts().get(pk=FIXTURE_PERFECT_PK)
        self.assertFalse(r.credit_remaining)


    def test_static_price(self):
        r: models.ReservationDebt = models.ReservationDebt.objects \
            .with_total_price().with_remote_accounts().get(
            pk=FIXTURE_STATIC_PRICE_PK
        )
        self.assertEquals(r.total_amount.amount, Decimal('7.00'))
        self.assertFalse(r.balance)
        r: models.Reservation = models.Reservation.objects \
            .with_total_price().with_remote_accounts().get(
            pk=FIXTURE_STATIC_PRICE_PK
        )
        self.assertEquals(r.total_amount.amount, Decimal('7.00'))
        self.assertFalse(r.balance)
        self.assertEquals(r.ticket_face_value.amount, Decimal('32.00'))
