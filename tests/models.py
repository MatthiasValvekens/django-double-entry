from decimal import Decimal

from django.conf import settings
from django.db import models
from django.db.models import (
    OuterRef, Sum, ExpressionWrapper, F, Subquery, Value
)
from django.db.models.functions import Coalesce
from djmoney.models.fields import MoneyField
from djmoney.money import Money

from double_entry import models as base
from double_entry.forms.bulk_utils import ResolvedTransaction
from double_entry.forms.csv import BankTransactionInfo
from double_entry.forms.transfers import (
    TransferResolver, TransferPaymentPreparator,
)
from double_entry.utils import decimal_to_money


class SimpleCustomer(base.TransactionPartyMixin):
    payment_tracking_prefix = 1
    name = models.CharField(max_length=100)

    def __str__(self):
        return '%s (id %d)' % (self.name, self.pk)

class SimpleCustomerDebt(base.BaseDebtRecord, base.ConcreteAmountMixin):
    # give different names for more meaningful testing
    debtor = models.ForeignKey(
        SimpleCustomer, on_delete=models.CASCADE,
        related_name='debts'
    )


class SimpleCustomerPaymentQuerySet(base.BasePaymentQuerySet, base.DuplicationProtectedQuerySet):
    pass


class PaymentQuerySet(base.DuplicationProtectedQuerySet, base.BasePaymentQuerySet):
    pass

class SimpleCustomerPayment(base.BasePaymentRecord, base.ConcreteAmountMixin, base.DuplicationProtectionMixin):
    dupcheck_signature_fields = ('creditor',)
    creditor = models.ForeignKey(
        SimpleCustomer, on_delete=models.CASCADE,
        related_name='payments'
    )

    objects = PaymentQuerySet.as_manager()


class SimpleCustomerPaymentSplit(base.BaseDebtPaymentSplit):
    payment = models.ForeignKey(
        SimpleCustomerPayment, on_delete=models.CASCADE,
        related_name='payment_splits'
    )
    debt = models.ForeignKey(
        SimpleCustomerDebt, on_delete=models.CASCADE,
        related_name='debt_splits'
    )


# TODO: add factory methods to double_entry to build these guys

class SimpleTransferResolver(TransferResolver[SimpleCustomer,
                                              BankTransactionInfo,
                                              ResolvedTransaction]):
    transaction_party_model = SimpleCustomer

class SimpleTransferPreparator(TransferPaymentPreparator[
                                        SimpleCustomerPayment,
                                        SimpleCustomer,
                                        ResolvedTransaction
                                    ]):
    transaction_party_model = SimpleCustomer

class Event(models.Model):
    name = models.CharField(max_length=100)
    start = models.DateTimeField()

class TicketCustomerQuerySet(base.TransactionPartyQuerySet):

    # noinspection DuplicatedCode
    def with_debt_balances(self):
        # FIXME: If/when Django decides to drop the ridiculous ban on nested
        #  aggregate functions, we can just use the superclass implementation.

        cls = self.__class__
        if cls.DEBT_BALANCE_FIELD in self.query.annotations:
            return self

        total_ticket_cost = Ticket.objects.filter(
            reservation__owner_id=OuterRef('pk')
        ).order_by().values('reservation__owner_id').annotate(
            _total_ticket_price=Sum(
                ExpressionWrapper(
                    F('category__price') * F('count'),
                    output_field=models.DecimalField()
                )
            )
        ).values('_total_ticket_price')

        total_static_debt = ReservationDebt.objects.filter(
            owner_id=OuterRef('pk')
        ).order_by().values('owner_id').annotate(
            _total_static_debt=Sum('static_price')
        ).values('_total_static_debt')

        total_paid = ReservationPaymentSplit.objects.filter(
            reservation__owner_id=OuterRef('pk')
        ).order_by().values('reservation__owner_id').annotate(
            _total_paid=Sum('amount')
        ).values('_total_paid')

        return self.annotate(
            total_ticket_cost=Coalesce(
                Subquery(total_ticket_cost),
                Value(Decimal('0.00')),
                output_field=models.DecimalField()
            ),
            total_static_debt=Coalesce(
                Subquery(total_static_debt),
                Value(Decimal('0.00')),
                output_field=models.DecimalField()
            ),
            total_paid=Coalesce(
                Subquery(total_paid),
                Value(Decimal('0.00')),
                output_field=models.DecimalField()
            )
        ).annotate(**{
            cls.DEBT_BALANCE_FIELD: (
                    F('total_ticket_cost') + F('total_static_debt')
                    - F('total_paid')
            )
        })

class TicketCustomer(base.TransactionPartyMixin):
    payment_tracking_prefix = 2
    name = models.CharField(max_length=100)

    objects = TicketCustomerQuerySet.as_manager()

    def __str__(self):
        return '%s (id %d)' % (self.name, self.pk)


class ReservationDebtQuerySet(base.BaseDebtQuerySet):
    TOTAL_PRICE_FIELD = 'total_price_fromdb'
    FACE_VALUE_FIELD = 'face_value_fromdb'

    # noinspection DuplicatedCode
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

BaseDebtReservationManager = models.Manager.from_queryset(
    ReservationDebtQuerySet
)
class ReservationDebtManager(BaseDebtReservationManager):
    def get_queryset(self):
        return super().get_queryset().with_total_price()

class ReservationDebt(base.BaseDebtRecord):
    TOTAL_AMOUNT_FIELD_COLUMN = ReservationDebtQuerySet.TOTAL_PRICE_FIELD

    owner = models.ForeignKey(
        TicketCustomer, on_delete=models.CASCADE, related_name='reservations'
    )

    static_price = models.DecimalField(
        decimal_places=4, max_digits=19, null=True, blank=True
    )

    objects = ReservationDebtManager()

    # noinspection DuplicatedCode
    @property
    def total_amount(self):
        if self.static_price is not None:
            return decimal_to_money(self.static_price)
        try:
            return decimal_to_money(
                getattr(self, ReservationDebtQuerySet.TOTAL_PRICE_FIELD)
            )
        except AttributeError:
            # no prefetch or annotation => database deluge :(
            # TODO: detect prefetch and warn if not present
            return sum(
                (ticket.count * ticket.category.price
                 for ticket in self.reservation.tickets.all()),
                Money(Decimal('0.00'), settings.DEFAULT_CURRENCY)
            )

    @total_amount.setter
    def total_amount(self, value):
        self.static_price = value.amount

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

    @property
    def ticket_face_value(self):
        # this cannot be None for an actual reservation, since
        # there must be tickets associated to the reservation
        # it could be zero, though (in theory)
        # Nevertheless, we program defensively.
        face_value = getattr(self, ReservationDebtQuerySet.FACE_VALUE_FIELD)
        return decimal_to_money(
            face_value or Decimal('0.00')
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


class ReservationPayment(base.BasePaymentRecord, base.ConcreteAmountMixin, base.DuplicationProtectionMixin):
    dupcheck_signature_fields = ('customer',)
    customer = models.ForeignKey(
        TicketCustomer, on_delete=models.CASCADE, related_name='payments'
    )
    objects = PaymentQuerySet.as_manager()

class ReservationPaymentSplit(base.BaseDebtPaymentSplit):

    reservation = models.ForeignKey(
        ReservationDebt, related_name='splits', on_delete=models.CASCADE
    )

    payment = models.ForeignKey(
        ReservationPayment, related_name='splits', on_delete=models.CASCADE
    )

class ReservationTransferResolver(TransferResolver[TicketCustomer,
                                              BankTransactionInfo,
                                              ResolvedTransaction]):
    transaction_party_model = TicketCustomer

class ReservationTransferPreparator(TransferPaymentPreparator[
                                   ReservationPayment,
                                   TicketCustomer,
                                   ResolvedTransaction]):
    transaction_party_model = TicketCustomer
