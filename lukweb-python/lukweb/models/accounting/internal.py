import logging
from decimal import Decimal

from django.db import models
from django.db.models import (
    F, Sum, Case, When, Subquery, OuterRef,
    Value, ExpressionWrapper,
)
from django.db.models.functions import Coalesce
from django.forms import ValidationError
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, pgettext_lazy,
)
from djmoney.models.fields import MoneyField
from djmoney.models.validators import MinMoneyValidator
from djmoney.money import Money

from ... import payments
from ...fields import (
    ChoirMemberField
)
from ...payments import decimal_to_money

logger = logging.getLogger(__name__)

__all__ = [
    'GnuCashCategory', 'InternalDebtItem', 'InternalPayment',
    'InternalPaymentSplit'
]


class GnuCashCategory(models.Model):
    # TODO: can we branch on whether citext is available or not?
    # TODO: what kind of validation do we want here?
    name = models.CharField(
        max_length=255,
        verbose_name=_('GnuCash category name'),
        unique=True,
    )

    class Meta:
        verbose_name = _('GnuCash category')
        verbose_name_plural = _('GnuCash categories')
        ordering = ('name',)

    @classmethod
    def get_category(cls, name):
        if not name:
            return None
        obj, created = cls.objects.get_or_create(
            name__iexact=name,
            # need to set defaults when using __iexact
            defaults={'name': name}
        )
        return obj

    def __str__(self):
        return self.name


class IDIQuerySet(models.QuerySet):
    def with_payments(self):
        # have to use subqueries, joins don't work for multiple
        # annotations. The pattern used here is from
        # https://docs.djangoproject.com/en/2.1/ref/models/expressions/#using-aggregates-within-a-subquery-expression
        amount_paid_subq = InternalPaymentSplit.objects.filter(
            debt=OuterRef('pk')
        ).order_by().values('debt').annotate(
            total_amount_paid=Sum('amount')
        ).values('total_amount_paid')

        return self.annotate(
            amount_paid_fromdb=Coalesce(
                Subquery(amount_paid_subq),
                Value(Decimal('0.00')),
                output_field=models.DecimalField()
            ),
            balance_fromdb=ExpressionWrapper(
                F('total_amount') - F('amount_paid_fromdb'),
                output_field=models.DecimalField()
            ),
            # For some extremely bizarre reason,
            # When(balance_fromdb__lte=Decimal(0), then=V(1)),
            # doesn't work. It returns the right result when I run
            # the generated SQL in sqlite3, but not through the ORM
            # This should probably be reported to upstream if I can 
            # find a minimal repro example somewhere.
            # This can fail to be correct on sqlite3 due to rounding errors
            # but postgres should compute it in fixed-point arithmetic
            # (I'd add in a rounding function, but it's kind of hard to do
            # that in a database-agnostic way)
            paid_fromdb=Case(
                When(
                    total_amount__lte=F('amount_paid_fromdb'), then=Value(True)
                ),
                default=Value(False),
                output_field=models.BooleanField()
            )
        )

    def unpaid(self):
        # assume with_payments
        # TODO does it hurt to call with_payments twice?
        return self.filter(paid_fromdb=False)

    def balances_by_filter_slug(self, filter_slugs=None, skip_zeroes=False):
        # again, assume with_payments
        # returns a per-filter tally of all debt balances
        qs = self.unpaid().values_list('filter_slug').order_by()
        if filter_slugs:
            qs = qs.filter(filter_slug__in=filter_slugs)
        qs = qs.annotate(
            total_balance=Coalesce(
                Sum('balance_fromdb'), Value(Decimal('0.00')),
            )
        )
        return {
            row[0]: payments.decimal_to_money(row[1]) for row in qs
            if not skip_zeroes or row[1]
        }


class InternalDebtItem(models.Model):
    member = ChoirMemberField(
        on_delete=models.PROTECT,
        verbose_name=_('involved member'),
        require_active=False,
        related_name='debts'
    )

    comment = models.CharField(
        verbose_name=_('comment'),
        max_length=255,
        blank=False,
    )

    # TODO: we probably want to add validation to the admin
    # that requires this field to be set unless activity_participation
    # is null. This is nontrivial, since activity_participation
    # is never included in admin forms.
    gnucash_category = models.ForeignKey(
        GnuCashCategory,
        verbose_name=_('GnuCash category'),
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    total_amount = MoneyField(
        verbose_name=_('amount owed'),
        decimal_places=2,
        max_digits=6,
        default_currency='EUR',
        # TODO this is a bit crufty, we should implement
        # a proper StrictMinValueValidator and mix
        # BaseMoneyValidator into that.
        validators=[MinMoneyValidator(Money(0.01, 'EUR'))]
    )

    timestamp = models.DateTimeField(
        verbose_name=_('timestamp'),
        default=timezone.now
    )

    # TODO: we should enforce participation_allowed on object creation
    # in the admin. This is not completely trivial
    activity_participation = models.ForeignKey(
        'ActivityParticipation',
        on_delete=models.CASCADE,
        editable=False,
        null=True
    )

    filter_slug = models.SlugField(
        verbose_name=_('filter code'),
        help_text=_('Filter code for use in targeted payment processing'),
        null=True,
        blank=True
    )

    objects = IDIQuerySet.as_manager()

    class Meta:
        verbose_name = _('internal debt')
        verbose_name_plural = _('internal debts')
        ordering = ('timestamp',)

    def clean(self):
        if not self.filter_slug:
            self.filter_slug = None

    @cached_property
    def amount_paid(self):
        if hasattr(self, 'amount_paid_fromdb'):
            return decimal_to_money(self.amount_paid_fromdb)
        # a debt that is not in the DB yet is by definition unpaid
        elif self.pk is None:
            return decimal_to_money(Decimal('0.00'))
        return decimal_to_money(
            self.splits.aggregate(
                a=Coalesce(
                    Sum('amount'), Decimal('0.00')
                )
            )['a']
        )

    @property
    def amount(self):
        import warnings
        warnings.warn(
            'Deprecated. Use total_amount instead', DeprecationWarning
        )
        return self.total_amount

        

    @cached_property
    def balance(self):
        if hasattr(self, 'balance_fromdb'):
            return decimal_to_money(self.balance_fromdb)
        return self.total_amount - self.amount_paid

    @cached_property
    def paid(self):
        if hasattr(self, 'paid_fromdb'):
            return self.paid_fromdb
        return self.amount_paid >= self.total_amount

    @cached_property
    def gnucash_category_string(self):
        if self.gnucash_category is not None:
            return_value = self.gnucash_category.name
        elif self.activity_participation is not None:
            # gnucash_category_string is never None
            return_value = self.activity_participation. \
                activity.gnucash_category_string
        else:
            logger.error("Could not find a gnucash category for payment")
            return_value = "WEBSITE_ERROR"
        return return_value

    @cached_property
    def gnucash_memo(self):
        if self.comment:
            return_val = self.comment
        elif self.activity_participation is not None:
            return_val = str(self.activity_participation.activity)
        else:
            logger.error("Could not find a memo name for payment")
            return_val = "WEBSITE ERROR: NO MEMO FOUND"
        return return_val

    def __str__(self):
        if self.comment:
            return '[%s]<%s>:%s' % (
                self.total_amount, self.member, self.comment
            )
        else:
            return '[%s]<%s>' % (self.total_amount, self.member)


class IPQuerySet(models.QuerySet):

    def with_debts(self):
        # for symmetry with IDIQuerySet.with_payments
        credit_used_subq = InternalPaymentSplit.objects.filter(
            payment=OuterRef('pk')
        ).order_by().values('payment').annotate(
            total_credit_used=Sum('amount')
        ).values('total_credit_used')

        return self.annotate(
            credit_used_fromdb=Coalesce(
                Subquery(credit_used_subq),
                Value(Decimal('0.00')),
                output_field=models.DecimalField()
            ),
            credit_remaining_fromdb=ExpressionWrapper(
                F('total_amount') - F('credit_used_fromdb'),
                output_field=models.DecimalField()
            ),
            fully_used_fromdb=Case(
                When(
                    total_amount__lte=F('credit_used_fromdb'),
                    then=Value(True)
                ),
                default=Value(False),
                output_field=models.BooleanField()
            )
        )

    def unused_credit(self):
        # assume with_debts
        return self.filter(fully_used_fromdb=False)


class InternalPayment(models.Model):
    member = ChoirMemberField(
        on_delete=models.PROTECT,
        verbose_name=_('involved member'),
        require_active=False,
        related_name='payments'
    )

    total_amount = MoneyField(
        verbose_name=_('amount paid'),
        decimal_places=2,
        max_digits=6,
        default_currency='EUR',
        # TODO see InternalDebtItem.amount
        validators=[MinMoneyValidator(Money(0.01, 'EUR'))]
    )

    PAYMENT_NATURE_CHOICES = (
        (
            payments.PAYMENT_NATURE_CASH, pgettext_lazy(
                'internal payment class', 'cash'
            )
        ),
        (
            payments.PAYMENT_NATURE_TRANSFER, pgettext_lazy(
                'internal payment class', 'transfer'
            )
        ),
        (
            payments.PAYMENT_NATURE_OTHER, pgettext_lazy(
                'internal payment class', 'other'
            )
        ),
    )

    # gnucash metadata
    nature = models.PositiveSmallIntegerField(
        verbose_name=_('payment nature'),
        choices=PAYMENT_NATURE_CHOICES
    )

    processed = models.DateTimeField(
        verbose_name=_('processing timestamp'),
        default=timezone.now,
        editable=False
    )

    timestamp = models.DateTimeField(
        verbose_name=_('payment timestamp'),
    )

    # invariant: sum(splits__amount) <= total_amount
    applied_to = models.ManyToManyField(
        InternalDebtItem,
        # this behaves as expected, since the sum of all transactions
        # minus the sum of all applied_to amounts is the total credit
        # in a member's account
        verbose_name=_('debts applied to'),
        related_name='payments',
        through='InternalPaymentSplit',
        blank=False,
    )

    objects = IPQuerySet.as_manager()

    class Meta:
        verbose_name = _('internal payment')
        verbose_name_plural = _('internal payments')

    @cached_property
    def credit_used(self):
        if hasattr(self, 'credit_used_fromdb'):
            return decimal_to_money(self.credit_used_fromdb)
        # a payment that is not in the DB yet is by definition unused
        elif self.pk is None:
            return decimal_to_money(Decimal('0.00'))
        return decimal_to_money(
            self.splits.aggregate(
                a=Coalesce(
                    Sum('amount'), Decimal('0.00')
                )
            )['a']
        )

    @cached_property
    def credit_remaining(self):
        if hasattr(self, 'credit_remaining_fromdb'):
            return decimal_to_money(self.credit_remaining_fromdb)
        return self.total_amount - self.credit_used

    @cached_property
    def fully_used(self):
        if hasattr(self, 'fully_used_fromdb'):
            return self.fully_used_fromdb
        return self.credit_used >= self.total_amount

    def save(self, **kwargs):
        # if timestamp not set, set it to the processing time timestamp
        if not self.timestamp:
            self.timestamp = self.processed
        return super(InternalPayment, self).save(**kwargs)

    def __str__(self):
        return '%s (%s)' % (self.total_amount, self.member)


class InternalPaymentSplit(models.Model):
    payment = models.ForeignKey(
        InternalPayment,
        on_delete=models.CASCADE,
        verbose_name=_('payment'),
        related_name='splits',
    )

    debt = models.ForeignKey(
        InternalDebtItem,
        on_delete=models.CASCADE,
        verbose_name=_('debt'),
        related_name='splits'
    )

    amount = MoneyField(
        verbose_name=_('amount paid'),
        decimal_places=2,
        max_digits=6,
        default_currency='EUR',
        # TODO see InternalDebtItem.amount
        validators=[MinMoneyValidator(Money(0.01, 'EUR'))]
    )

    class Meta:
        verbose_name = _('internal payment split')
        verbose_name_plural = _('internal payment splits')
        unique_together = ('payment', 'debt')

    # TODO: sane __str__

    def clean(self):
        try:
            if self.payment.timestamp < self.debt.timestamp:
                raise ValidationError(
                    _('Payment cannot be applied to future debt.')
                )
        except (
            InternalPayment.DoesNotExist, InternalDebtItem.DoesNotExist
        ):
            pass
