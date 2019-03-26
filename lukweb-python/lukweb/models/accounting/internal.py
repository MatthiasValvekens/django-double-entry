import logging
from collections import defaultdict
from decimal import Decimal

from django.db import models
from django.db.models import (
    Sum, Value,
    Index,
)
from django.db.models.functions import Coalesce
from django.forms import ValidationError
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, pgettext_lazy, ugettext
)

from . import base as accounting_base
from ... import payments
from ...fields import (
    ChoirMemberField
)

logger = logging.getLogger(__name__)

__all__ = [
    'InternalDebtItem', 'InternalPayment', 'InternalPaymentSplit'
]


class IDIQuerySet(accounting_base.BaseDebtQuerySet):

    def balances_by_filter_slug(self, filter_slugs=None, skip_zeroes=False):
        # returns a per-filter tally of all debt balances
        qs = self.with_payments().unpaid().values_list('filter_slug').order_by()
        if filter_slugs:
            qs = qs.filter(filter_slug__in=filter_slugs)
        qs = qs.annotate(
            total_balance=Coalesce(
                Sum(IDIQuerySet.UNMATCHED_BALANCE_FIELD), 
                Value(Decimal('0.00')),
            )
        )
        zero_money = payments.decimal_to_money(Decimal('0.00'))
        return defaultdict(lambda: zero_money, {
            slug: payments.decimal_to_money(v) for slug, v in qs
            if not skip_zeroes or v
        })


class InternalDebtItem(accounting_base.BaseDebtRecord,
                       accounting_base.ConcreteAmountMixin):

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

    insufficient_unmatched_balance_error = _(
        'The balance of the selected debt is lower than the '
        'amount supplied: '
        'balance is %(balance)s, but attempted to credit '
        '%(amount)s.'
    )

    class Meta:
        verbose_name = _('internal debt')
        verbose_name_plural = _('internal debts')
        ordering = ('timestamp',)
        indexes = (
            Index(fields=['member', 'filter_slug']),
            Index(fields=['filter_slug'])
        )

    def clean(self):
        if not self.filter_slug:
            self.filter_slug = None
        if not self.comment:
            self.comment = ''

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

    def get_comment_display(self):
        if self.comment:
            return self.comment
        elif self.is_refund:
            return ugettext('<refund/unmanaged debt>')
        elif self.activity_participation is not None:
            base = str(self.activity_participation.activity)
            if self.activity_participation.participant_count > 1:
                return '%s [%s]' % (
                    base, pgettext_lazy('registrant count', '%(count)d p') % {
                        'count': self.activity_participation.participant_count
                    }
                )
            else:
                return base
        else:
            return ''

    @cached_property
    def gnucash_memo(self):
        return_val = self.get_comment_display()
        if not return_val:
            logger.error(
                "Could not find a memo name for payment with id %s." % (
                    self.pk
                )
            )
            return_val = "WEBSITE ERROR: NO MEMO FOUND"
        return return_val

    def form_select_str(self):
        return _(
            '%(comment)s (total: %(total)s, balance: %(balance)s) '
            '[%(date)s]'
        ) % {
            'date': timezone.localdate(self.timestamp),
            'balance': self.balance,
            'total': self.total_amount,
            'comment': self.get_comment_display()
        }

    def __str__(self):
        if self.comment:
            return '[%s]<%s>:%s' % (
                self.total_amount, self.member, self.comment
            )
        else:
            return '[%s]<%s>' % (self.total_amount, self.member)


class IPQuerySet(accounting_base.BasePaymentQuerySet, 
                 accounting_base.DuplicationProtectedQuerySet):
    pass

class InternalPayment(accounting_base.BasePaymentRecord,
                      accounting_base.ConcreteAmountMixin,
                      accounting_base.DuplicationProtectionMixin):

    dupcheck_signature_fields = ('nature', 'member')

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

    insufficient_unmatched_balance_error = _(
        'That payment does not have enough funds left: '
        'requested %(amount)s, but only %(balance)s available.'
    )

    objects = IPQuerySet.as_manager()

    class Meta:
        verbose_name = _('internal payment')
        verbose_name_plural = _('internal payments')

    def save(self, **kwargs):
        # if timestamp not set, set it to the processing time timestamp
        if not self.timestamp:
            self.timestamp = self.processed
        return super(InternalPayment, self).save(**kwargs)

    def form_select_str(self):
        return _('%(date)s (total: %(total)s, credit rem.: %(credit)s)') % {
            'date': timezone.localdate(self.timestamp),
            'total': self.total_amount,
            'credit': self.credit_remaining,
        }

    def __str__(self):
        return '%s (%s)' % (self.total_amount, self.member)


class InternalPaymentSplit(accounting_base.BaseDebtPaymentSplit):
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
        super().clean()
        if self.payment.member_id != self.debt.member_id:
            raise ValidationError(
                _('Payment and debt must belong to the same member.')
            )
