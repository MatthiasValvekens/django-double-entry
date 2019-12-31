from django.db import models
from django.db.models import OuterRef, Sum, ExpressionWrapper, F, Subquery
from django.db.models.functions import Coalesce
from djmoney.models.fields import MoneyField

from double_entry import models as base


class SimpleCustomer(base.TransactionPartyMixin):
    name = models.CharField(max_length=100)

class SimpleCustomerDebt(base.BaseDebtRecord, base.ConcreteAmountMixin):
    # give different names for more meaningful testing
    debtor = models.ForeignKey(
        SimpleCustomer, on_delete=models.CASCADE,
        related_name='debts'
    )

class SimpleCustomerPayment(base.BasePaymentRecord, base.ConcreteAmountMixin):
    creditor = models.ForeignKey(
        SimpleCustomer, on_delete=models.CASCADE,
        related_name='payments'
    )


class SimpleCustomerPaymentSplit(base.BaseTransactionSplit):
    payment = models.ForeignKey(
        SimpleCustomerPayment, on_delete=models.CASCADE,
        related_name='payment_splits'
    )
    debt = models.ForeignKey(
        SimpleCustomerDebt, on_delete=models.CASCADE,
        related_name='debt_splits'
    )


class Event(models.Model):
    name = models.CharField(max_length=100)
    start = models.DateTimeField()

class TicketCustomer(base.TransactionPartyMixin):
    name = models.CharField(max_length=100)


class ReservationDebtQuerySet(base.BaseDebtQuerySet):
    TOTAL_PRICE_FIELD = 'total_price_fromdb'
    FACE_VALUE_FIELD = 'face_value_fromdb'

    def with_total_price(self):
        cls = self.__class__
        actual_reservation_query = Ticket.objects.filter(
            reservation__debt_id=OuterRef('pk')
        ).order_by().values('reservation_id').annotate(
            _total_ticket_price=Sum(
                ExpressionWrapper(
                    F('category__price') * F('count'),
                    output_field=models.DecimalField()
                )
            )
        ).values('_total_ticket_price')

        return self.annotate(**{
            cls.FACE_VALUE_FIELD: ExpressionWrapper(
                Subquery(actual_reservation_query),
                output_field=models.DecimalField()
            ),
            # static price takes precedence
            cls.TOTAL_PRICE_FIELD: ExpressionWrapper(
                Coalesce(F('static_price'), F(cls.FACE_VALUE_FIELD), 0),
                output_field=models.DecimalField()
            )
        })


class ReservationDebt(base.BaseDebtRecord):
    owner = models.ForeignKey(
        TicketCustomer, on_delete=models.CASCADE, related_name='reservations'
    )

    static_price = models.DecimalField(
        decimal_places=4, max_digits=19, null=True, blank=True
    )

class Reservation(ReservationDebt):
    debt = models.OneToOneField(
        ReservationDebt, on_delete=models.CASCADE,
        related_name='reservation', parent_link=True
    )

    event = models.ForeignKey(
        Event, on_delete=models.CASCADE,
        related_name='reservations'
    )

    # see if the magic is smart enough to recognise that this FK is NOT
    # the one needed by the ledger management code
    referred_by = models.ForeignKey(
        SimpleCustomer, on_delete=models.CASCADE,
        related_name='referrer', null=True
    )


class TicketCategory(models.Model):
    price = MoneyField(decimal_places=2, max_digits=6)

class Ticket(models.Model):
    reservation = models.ForeignKey(
        Reservation, on_delete=models.CASCADE, related_name='tickets'
    )
    category = models.ForeignKey(
        TicketCategory, models.CASCADE, related_name='tickets'
    )
    count = models.PositiveSmallIntegerField()


class ReservationPayment(base.BasePaymentRecord, base.ConcreteAmountMixin):
    customer = models.ForeignKey(
        TicketCustomer, on_delete=models.CASCADE, related_name='payments'
    )

class ReservationPaymentSplit(base.BaseDebtPaymentSplit):

    reservation = models.ForeignKey(
        ReservationDebt, related_name='splits', on_delete=models.CASCADE
    )

    payment = models.ForeignKey(
        ReservationPayment, related_name='splits', on_delete=models.CASCADE
    )
