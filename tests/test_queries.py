from django.test import TestCase
from tests import models

FIXTURE_EVENT_PK = 1
FIXTURE_UNPAID_RESERVATION_PK = 1
FIXTURE_PERFECT_RESERVATION_PK = 2
FIXTURE_TOO_MANY_SCANS_PK = 3
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

# TODO: write tests for simpler test models
# TODO: test matched dates

class TestReservationPaymentQueries(TestCase):
    fixtures = ['reservations.json']

    def test_simple_unpaid_reservation(self):
        r: models.ReservationDebt = models.ReservationDebt.objects \
            .with_total_price().with_remote_accounts().get(
            pk=FIXTURE_UNPAID_RESERVATION_PK
        )
        self.assertTrue(r.balance)
        self.assertEquals(r.balance, r.total_amount)
        r: models.Reservation = models.Reservation.objects \
            .with_total_price().with_remote_accounts().get(
            pk=FIXTURE_UNPAID_RESERVATION_PK
        )
        self.assertTrue(r.balance)
        self.assertEquals(r.balance, r.total_amount)


    def test_perfect_reservation(self):
        r: models.ReservationDebt = models.ReservationDebt.objects\
            .with_total_price().with_remote_accounts().get(
                pk=FIXTURE_PERFECT_RESERVATION_PK
            )
        self.assertFalse(r.balance)
        r: models.Reservation = models.Reservation.objects \
            .with_total_price().with_remote_accounts().get(
                pk=FIXTURE_PERFECT_RESERVATION_PK
            )
        self.assertFalse(r.balance)
        r: models.ReservationPayment = models.ReservationPayment.objects \
            .with_remote_accounts().get(pk=FIXTURE_PERFECT_RESERVATION_PK)
        self.assertFalse(r.credit_remaining)



