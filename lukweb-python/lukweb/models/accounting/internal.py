import logging
from decimal import Decimal

from django.db import models
from django.db.models import (
    Sum, Value,
)
from django.db.models.functions import Coalesce
from django.forms import ValidationError
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, pgettext_lazy,
)

from . import base as accounting_base
from ... import payments
from ...fields import (
    ChoirMemberField
)

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


class IDIQuerySet(accounting_base.BaseDebtQuerySet):

    def balances_by_filter_slug(self, filter_slugs=None, skip_zeroes=False):
        # again, assume with_payments
        # returns a per-filter tally of all debt balances
        qs = self.unpaid().values_list('filter_slug').order_by()
        if filter_slugs:
            qs = qs.filter(filter_slug__in=filter_slugs)
        qs = qs.annotate(
            total_balance=Coalesce(
                Sum(IDIQuerySet.MATCHED_BALANCE_FIELD), Value(Decimal('0.00')),
            )
        )
        return {
            slug: payments.decimal_to_money(v) for slug, v in qs
            if not skip_zeroes or v
        }


class InternalDebtItem(accounting_base.BaseDebtRecord):
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

    @property
    def amount(self):
        import warnings
        warnings.warn(
            'Deprecated. Use total_amount instead', DeprecationWarning
        )
        return self.total_amount

        
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


class InternalPayment(accounting_base.BasePaymentRecord):

    member = ChoirMemberField(
        on_delete=models.PROTECT,
        verbose_name=_('involved member'),
        require_active=False,
        related_name='payments'
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

    class Meta:
        verbose_name = _('internal payment')
        verbose_name_plural = _('internal payments')

    def save(self, **kwargs):
        # if timestamp not set, set it to the processing time timestamp
        if not self.timestamp:
            self.timestamp = self.processed
        return super(InternalPayment, self).save(**kwargs)

    def __str__(self):
        return '%s (%s)' % (self.total_amount, self.member)


class InternalPaymentSplit(accounting_base.BaseTransactionSplit):
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
