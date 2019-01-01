import logging
from decimal import Decimal

from django.db import models
from django.db.models import (
    F, Sum, Case, When, Subquery, OuterRef,
    Value, ExpressionWrapper
)
from django.db.models.functions import Coalesce
from django.forms import ValidationError
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, pgettext_lazy,
)
from djmoney.models.fields import MoneyField
from djmoney.money import Money
from django.db.models.fields.reverse_related import ManyToOneRel
from django.conf import settings

from ...payments import decimal_to_money

__all__ = [
    'DoubleBookModel', 'BaseFinancialRecord', 'BaseDebtRecord',
    'BasePaymentRecord', 'BaseDebtQuerySet', 'BasePaymentQuerySet',
    'BaseTransactionSplit', 'DoubleBookQuerySet', 'nonzero_money_validator'
]

logger = logging.getLogger(__name__)


def nonzero_money_validator(money):
    if money.amount <= 0:
        raise ValidationError(
            pgettext_lazy(
                'accounting', 'Amount should be strictly greater than zero.'
            )
        )

class DoubleBookModel(models.Model):
    _split_manager_name = None
    _split_model = None
    _remote_target_field = None
    _other_half_model = None

    timestamp = models.DateTimeField(
        verbose_name=pgettext_lazy(
            'accounting', 'transaction timestamp'
        ),
        default=timezone.now
    )

    processed = models.DateTimeField(
        verbose_name=pgettext_lazy(
            'accounting', 'processing timestamp'
        ),
        default=timezone.now,
        editable=False
    )

    class Meta:
        abstract = True

    @classmethod
    def _prepare_split_metadata(cls):
        """
        Find the split relation to use through reflection.
        """ 
        def get_fks_on_split(split_model):
            return [
                f for f in split_model._meta.get_fields()
                if isinstance(f, models.ForeignKey)
                and issubclass(f.related_model, DoubleBookModel)
            ]
            
        def is_candidate(field):
            # we're only interested in many2one fields
            if not isinstance(field, ManyToOneRel):
                return False
            remote_model = field.remote_field.model
            if not issubclass(remote_model, BaseTransactionSplit):
                return False
            # count the number of DoubleBookModel fk's
            # on the remote model. It should be exactly 2,
            # and they should point to different models
            remote_fks = get_fks_on_split(remote_model)
            doublebook_fk_count = len(remote_fks)
            doublebook_fk_model_count = len(
                set(f.related_model for f in remote_fks)
            )
            return doublebook_fk_count == doublebook_fk_model_count == 2
           
        candidates = [
            f for f in cls._meta.get_fields() if is_candidate(f)
        ]
        if not candidates:
            raise TypeError(
                'There are no possible split fields on this '
                'DoubleBookModel.'
            )
        elif len(candidates) > 1:
            raise TypeError(
                'There are too many possible split fields on this '
                'DoubleBookModel: %s.' % (
                    ', '.join(f.name for f in candidates)
                )
            )

        split_rel = candidates[0]
        cls._split_model = split_rel.related_model
        cls._split_manager_name = split_rel.name
        cls._remote_target_field = split_rel.remote_field.name
        
        # the is_candidate condition guarantees that this works
        split_fk_1, split_fk_2 = get_fks_on_split(cls._split_model)
        if split_fk_1.related_model == cls:
            cls._other_half_model = split_fk_2.related_model
        else:
            cls._other_half_model = split_fk_1.related_model

    @classmethod
    def get_split_model(cls):
        if cls._split_model is None:
            cls._prepare_split_metadata()
        return cls._split_model, cls._remote_target_field

    @classmethod
    def get_other_half_model(cls):
        if cls._other_half_model is None:
            cls._prepare_split_metadata()
        return cls._other_half_model

    @property
    def split_manager(self):
        cls = self.__class__
        if cls._split_manager_name is None:
            cls._prepare_split_metadata()
        return getattr(self, cls._split_manager_name)

    @cached_property
    def matched_balance(self):
        try:
            return decimal_to_money(
                getattr(self, DoubleBookQuerySet.MATCHED_BALANCE_FIELD)
            )
        except AttributeError as e:
            # a record that is not in the DB yet is by definition 
            # completely unmatched
            if self.pk is None:
                return decimal_to_money(Decimal('0.00'))
            logger.debug(
                'PERFORMANCE WARNING: '
                'falling back to database deluge '
                'for matched_balance computation. '
                'Please review queryset usage.', e
            )
            splits = self.split_manager
            return decimal_to_money(
                splits.aggregate(
                    a=Coalesce(
                        Sum('amount'), Decimal('0.00')
                    )
                )['a']
            )

    @cached_property
    def unmatched_balance(self):
        try:
            return decimal_to_money(
                getattr(self, DoubleBookQuerySet.UNMATCHED_BALANCE_FIELD)
            )
        except AttributeError:
            # total_amount needs to be a valid attribute, but it doesn't make
            # sense to define it as None at the class level,
            # since it might be supplied by an annotation.
            # (TODO: is this necessarily a problem?)
            return self.total_amount - self.matched_balance

    @cached_property
    def fully_matched(self):
        # ignore the direct database result here, since
        # it might have rounding errors (and it's intended for filtering
        # anyway)
        return not self.unmatched_balance


class BaseFinancialRecord(DoubleBookModel):

    total_amount = MoneyField(
        verbose_name=_('total amount'),
        decimal_places=2,
        max_digits=6,
        default_currency=settings.BOOKKEEPING_CURRENCY,
        validators=[nonzero_money_validator]
    )

    class Meta:
        abstract = True


# Conventions: 
#  matched balance: sum of all splits
#  unmatched balance: whatever remains 
#  (i.e. money that doesn't appear in any transactions so far)
class DoubleBookQuerySet(models.QuerySet):

    MATCHED_BALANCE_FIELD = 'matched_balance_fromdb'
    UNMATCHED_BALANCE_FIELD = 'unmatched_balance_fromdb' 
    FULLY_MATCHED_FIELD = 'fully_matched_fromdb'
    TOTAL_AMOUNT_FIELD = 'total_amount'

    def _split_sum_subquery(self):
        """
        Compute the sum over all transaction splits for each row
        via a subquery (no joins, so suitable for multiple qs annotations).
        The final output will be a DecimalField.
        """
        # The pattern used here is from
        # https://docs.djangoproject.com/en/2.1/ref/models/expressions/
        split_model, join_on = self.model.get_split_model()
        subq = split_model._default_manager.filter(**{
            join_on: OuterRef('pk')
        }).order_by().values(join_on).annotate(
            _split_total=Sum('amount')
        ).values('_split_total')
        return Coalesce(
            Subquery(subq),
            Value(Decimal('0.00')),
            output_field=models.DecimalField()
        )
    
    def with_remote_accounts(self):
        cls = self.__class__
        # TODO: figure out if this is even necessary
        if cls.FULLY_MATCHED_FIELD in self.query.annotations:
            return self
        # joins don't work for multiple annotations, so 
        # we have to use a subquery
        return self.annotate(**{
            cls.MATCHED_BALANCE_FIELD: self._split_sum_subquery(),
            cls.UNMATCHED_BALANCE_FIELD: ExpressionWrapper(
                F(cls.TOTAL_AMOUNT_FIELD) - F(cls.MATCHED_BALANCE_FIELD),
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
            # TODO: write said rounding function
            cls.FULLY_MATCHED_FIELD: Case(
                When(
                    total_amount__lte=F(cls.MATCHED_BALANCE_FIELD), 
                    then=Value(True)
                ),
                default=Value(False),
                output_field=models.BooleanField()
            )
        })

    def unmatched(self):
        return self.with_remote_accounts().filter(**{
            self.__class__.FULLY_MATCHED_FIELD: False
        })

    def fully_matched(self):
        return self.with_remote_accounts().filter(**{
            self.__class__.FULLY_MATCHED_FIELD: True
        })
        

# for semantic consistency and backwards compatibility
class BaseDebtQuerySet(DoubleBookQuerySet):
    
    def with_payments(self):
        return self.with_remote_accounts()

    def unpaid(self):
        return self.unmatched()

    def paid(self):
        return self.fully_matched()


class BasePaymentQuerySet(DoubleBookQuerySet):

    def with_debts(self):
        return self.with_remote_accounts()

    def credit_remaining(self):
        return self.unmatched()

    def fully_used(self):
        return self.fully_matched()


class BaseDebtRecord(BaseFinancialRecord):

    objects = BaseDebtQuerySet.as_manager()

    class Meta:
        abstract = True

    @property
    def amount_paid(self):
        return self.matched_balance

    @property
    def balance(self):
        return self.unmatched_balance

    @property
    def paid(self):
        return self.fully_matched


class BasePaymentRecord(BaseFinancialRecord):

    objects = BasePaymentQuerySet.as_manager()

    class Meta:
        abstract = True

    @cached_property
    def credit_used(self):
        return self.matched_balance

    @cached_property
    def credit_remaining(self):
        return self.unmatched_balance

    @cached_property
    def fully_used(self):
        return self.fully_matched
    

class BaseTransactionSplit(models.Model):

    amount = MoneyField(
        verbose_name=_('amount'),
        decimal_places=2,
        max_digits=6,
        default_currency=settings.BOOKKEEPING_CURRENCY,
        validators=[nonzero_money_validator]
    )
    
    class Meta:
        abstract = True
