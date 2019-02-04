import logging
import datetime
from decimal import Decimal
from collections import defaultdict
from typing import Type, Tuple, cast

from django.db import models
from django.db.models import (
    F, Sum, Case, When, Subquery, OuterRef,
    Value, ExpressionWrapper,
    Max,
)
from django.db.models.functions import Coalesce
from django.forms import ValidationError
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, pgettext_lazy,
)
from djmoney.models.fields import MoneyField
from django.db.models.fields.reverse_related import ManyToOneRel
from django.conf import settings

from ...payments import decimal_to_money
from ...utils import _dt_fallback

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


class DoubleBookInterface(models.Model):
    """
    One half of a ledger in a double-entry accounting system.
    """

    """
    Name of the field or property representing the total amount.
    Possibly different from the actual db column / annotation.
    """
    TOTAL_AMOUNT_FIELD_NAME = 'total_amount'
    TOTAL_AMOUNT_FIELD_COLUMN = 'total_amount'

    # This error message is vague, so subclasses should override it with
    # something that makes more sense.
    insufficient_unmatched_balance_error = _(
        'This account does not have enough unmatched balance to '
        'apply the requested split: unmatched balance is %(balance)s, '
        'but attempted to match %(amount)s.'
    )

    _split_manager_name = None
    _split_model = None
    _remote_target_field = None
    _other_half_model = None

    timestamp = None
    processed = None

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
    def get_split_model(cls) -> Tuple[Type['BaseTransactionSplit'], str]:
        if cls._split_model is None:
            cls._prepare_split_metadata()
        return cls._split_model, cls._remote_target_field

    @classmethod
    def get_other_half_model(cls) -> Type['DoubleBookInterface']:
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
        except AttributeError:
            # a record that is not in the DB yet is by definition 
            # completely unmatched. If it is freshly saved, the 
            # same should apply.
            if self.pk is None:
                return decimal_to_money(Decimal('0.00'))
            logger.debug(
                'PERFORMANCE WARNING: '
                'falling back to database deluge '
                'for matched_balance computation. '
                'Please review queryset usage. '
                'Object of type %(model)s with id %(pk)s', 
                {'model': self.__class__, 'pk': self.pk}
            )
            import traceback
            logger.debug(''.join(traceback.format_stack()))
            splits = self.split_manager
            return decimal_to_money(
                splits.aggregate(
                    a=Coalesce(
                        Sum('amount'), Decimal('0.00')
                    )
                )['a']
            )

    @cached_property
    def fully_matched_date(self):
        try:
            return getattr(self, DoubleBookQuerySet.FULLY_MATCHED_DATE_FIELD)
        except AttributeError:
            if not self.fully_matched:
                return None
            logger.debug(
                'PERFORMANCE WARNING: '
                'falling back to database deluge '
                'for fully_matched_date computation. '
                'Please review queryset usage. '
                'Object of type %(model)s with id %(pk)s',
                {'model': self.__class__, 'pk': self.pk}
            )
            import traceback
            logger.debug(''.join(traceback.format_stack()))
            other_half = self.get_other_half_model()
            split_model, other_half_fk = other_half.get_split_model()
            return self.split_manager.aggregate(
                a=Max(other_half_fk + '__timestamp')
            )['a']

    def spoof_matched_balance(self, amount):
        setattr(
            self, DoubleBookQuerySet.MATCHED_BALANCE_FIELD, amount
        )
        try:
            # invalidate cache
            del self.__dict__['matched_balance']
        except KeyError:
            pass

    @property
    def unmatched_balance(self):
        try:
            return decimal_to_money(
                getattr(self, DoubleBookQuerySet.UNMATCHED_BALANCE_FIELD)
            )
        except AttributeError:
            total_amount = getattr(self, self.TOTAL_AMOUNT_FIELD_NAME)
            return  total_amount - self.matched_balance

    @property
    def fully_matched(self):
        # ignore the direct database result here, since
        # it might have rounding errors (and it's intended for filtering
        # anyway)
        return not self.unmatched_balance

    # string value that will be used in select fields in admin forms
    def form_select_str(self):
        return str(self)

    def save(self, **kwargs):
        # 'remember' when saving a new object
        balance_set = hasattr(self, DoubleBookQuerySet.MATCHED_BALANCE_FIELD)
        if self.pk is None and not balance_set:
            self.spoof_matched_balance(Decimal('0.00'))
        super().save(**kwargs)


class DoubleBookModel(DoubleBookInterface):

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


class DuplicationProtectionMixin(DoubleBookInterface):
    """
    Specify fields to be used in the duplicate checker on bulk imports.
    The fields `timestamp` and `total_amount` are implicit.
    """
    dupcheck_signature_fields = None

    class Meta:
        abstract = True

    @property
    def dupcheck_signature(self):
        cls = self.__class__
        if cls.dupcheck_signature_fields is None:
            return None
        # translates foreign keys to the fieldname_id format,
        # which is better for comparisons
        sig_fields = list(
            cls._meta.get_field(fname).column
            for fname in cls.dupcheck_signature_fields
        )
        # Problem: the resolution of most banks' reporting is a day.
        # Hence, we cannot use an exact timestamp as a cutoff point between
        # imports, which would eliminate the need for duplicate 
        # checking in practice.
        date = timezone.localdate(self.timestamp)
        amt = getattr(self, cls.TOTAL_AMOUNT_FIELD_NAME).amount
        return (date, amt) + tuple(
            getattr(self, field) for field in sig_fields
        )


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

    def clean(self): 
        if self.total_amount.amount < 0:
            raise ValidationError(
                _('Ledger entry amount is negative: %(amount)s') % {
                    'amount': self.total_amount
                }
            )


# Conventions: 
#  matched balance: sum of all splits
#  unmatched balance: whatever remains 
#  (i.e. money that doesn't appear in any transactions so far)
class DoubleBookQuerySet(models.QuerySet):

    model: Type[DoubleBookModel]

    MATCHED_BALANCE_FIELD = 'matched_balance_fromdb'
    UNMATCHED_BALANCE_FIELD = 'unmatched_balance_fromdb' 
    FULLY_MATCHED_FIELD = 'fully_matched_fromdb'
    FULLY_MATCHED_DATE_FIELD = 'fully_matched_date_fromdb'

    def _split_sum_subquery(self):
        """
        Compute the sum over all transaction splits for each row
        via a subquery (no joins, so suitable for multiple qs annotations).
        The final output will be a DecimalField.
        """
        # The pattern used here is from
        # https://docs.djangoproject.com/en/2.1ref/models/expressions/
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
        total_amount_field_name = self.model.TOTAL_AMOUNT_FIELD_COLUMN
        annotation_kwargs = {
            cls.MATCHED_BALANCE_FIELD: self._split_sum_subquery(),
            cls.UNMATCHED_BALANCE_FIELD: ExpressionWrapper(
                F(total_amount_field_name) - F(cls.MATCHED_BALANCE_FIELD),
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
                When(**{
                    total_amount_field_name + '__lte':
                        F(cls.MATCHED_BALANCE_FIELD),
                    'then': Value(True)
                }),
                default=Value(False),
                output_field=models.BooleanField()
            ),
        }
        return self.annotate(**annotation_kwargs)

    def with_fully_matched_date(self):
        # Useful e.g. to compute the effective date of payment on a debt
        # TODO: I assume the db is smart enough to do this more or less
        #  in tandem with _split_sum_subquery, but is it really that simple?
        #  Ideally, it would probably be best to do both in the same subquery
        #  but Django doesn't seem to deal with multi-column subqueries that
        #  well.
        cls = self.__class__
        if cls.FULLY_MATCHED_DATE_FIELD in self.query.annotations:
            return self
        qs = self.with_remote_accounts()
        other_half = self.model.get_other_half_model()
        split_model, join_on = self.model.get_split_model()
        __, other_half_fk = other_half.get_split_model()
        # query string to get the timestamp field on the other half of the
        # ledger.
        remote_date_field = other_half_fk + '__timestamp'
        subq = split_model._default_manager.filter(**{
            join_on: OuterRef('pk'),
        }).order_by().values(join_on).annotate(
            _max_date=Max(remote_date_field)
        ).values('_max_date')

        return qs.annotate(**{
            cls.FULLY_MATCHED_DATE_FIELD: Case(
                When(**{
                    cls.FULLY_MATCHED_FIELD: True,
                    'then': Subquery(subq),
                }),
                default=Value(None),
                output_field=models.DateTimeField()
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


class DuplicationProtectedQuerySet(DoubleBookQuerySet):

    model: Type[DuplicationProtectionMixin]

    # Prepare buckets for duplication check
    def dupcheck_buckets(self, date_bounds=None):
        if self.model.dupcheck_signature_fields is None:
            raise TypeError(
                'Duplicate checking is not supported on this model.'
            )
        historical_buckets = defaultdict(int)
        if date_bounds is not None:
            # replace min/max timestamps by min/max time on the same day
            # (in the local timezone) and filter
            min_date, max_date = date_bounds
            if isinstance(min_date, datetime.datetime):
                min_date = timezone.localdate(min_date)
            if isinstance(max_date, datetime.datetime):
                max_date = timezone.localdate(max_date)
            # assume that we have a raw date pair now
            qs = self.filter(
                timestamp__gte=_dt_fallback(min_date),
                timestamp__lte=_dt_fallback(max_date, use_max=True)
            )
        else:
            qs = self

        for entry in qs:
            historical_buckets[entry.dupcheck_signature] += 1

        return historical_buckets 

# mainly for semantic consistency and backwards compatibility
class BaseDebtQuerySet(DoubleBookQuerySet):
    
    def with_payments(self, include_timestamps=False):
        if include_timestamps:
            return self.with_fully_matched_date()
        else:
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

    @classmethod
    def get_split_model(cls) -> Tuple[Type['BaseDebtPaymentSplit'], str]:
        return cast(
            Tuple[Type['BaseDebtPaymentSplit'], str], super().get_split_model()
        )

    @classmethod
    def get_other_half_model(cls) -> Type['BasePaymentRecord']:
        return cast(
            Type['BasePaymentRecord'], super().get_other_half_model()
        )

    is_refund = models.BooleanField(
        verbose_name=_('Refund/unmanaged'),
        help_text=_(
            'Flag indicating whether this debt record represents an '
            'overpayment refund or an unmanaged debt, rather than '
            'an actual debt record.'
        ),
        default=False,
        editable=False
    )

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

    @property
    def payment_timestamp(self):
        return self.fully_matched_date


class BasePaymentRecord(BaseFinancialRecord):

    @classmethod
    def get_split_model(cls) -> Tuple[Type['BaseDebtPaymentSplit'], str]:
        return cast(
            Tuple[Type['BaseDebtPaymentSplit'], str], super().get_split_model()
        )

    @classmethod
    def get_other_half_model(cls) -> Type['BaseDebtRecord']:
        return cast(
            Type['BaseDebtRecord'], super().get_other_half_model()
        )

    objects = BasePaymentQuerySet.as_manager()

    class Meta:
        abstract = True

    @property
    def credit_used(self):
        return self.matched_balance

    @property
    def credit_remaining(self):
        return self.unmatched_balance

    @property
    def fully_used(self):
        return self.fully_matched
    

# TODO: can we auto-enforce unique_together?
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

    @classmethod
    def get_double_book_models(cls):
        res = {
            f.name: f.related_model for f in cls._meta.get_fields()
            if isinstance(f, models.ForeignKey) 
            and issubclass(f.related_model, DoubleBookModel)
        }
        assert len(res) == 2
        return res

    def clean(self):
        if self.amount.amount < 0:
            raise ValidationError(
                _('Split amount is negative: %(amount)s') % {
                    'amount': self.amount
                }
            )


class BaseDebtPaymentSplit(BaseTransactionSplit):
    _pmt_column_name = None
    _debt_column_name = None

    class Meta:
        abstract = True

    @classmethod
    def get_payment_column(cls):
        if cls._pmt_column_name is None:
            res = [
                f.name for f in cls._meta.get_fields()
                if isinstance(f, models.ForeignKey) 
                and issubclass(f.related_model, BasePaymentRecord)
            ]
            if len(res) == 0:
                raise TypeError('No payment column present.')
            elif len(res) == 2:
                raise TypeError('Multiple payment columns present.')
            cls._pmt_column_name = res[0]
        return cls._pmt_column_name

    @classmethod
    def get_debt_column(cls):
        if cls._debt_column_name is None:
            res = [
                f.name for f in cls._meta.get_fields()
                if isinstance(f, models.ForeignKey) 
                and issubclass(f.related_model, BaseDebtRecord)
            ]
            if len(res) == 0:
                raise TypeError('No debt column present.')
            elif len(res) == 2:
                raise TypeError('Multiple debt columns present.')
            cls._debt_column_name = res[0]
        return cls._debt_column_name
    
    def clean(self):
        super().clean()
        cls = self.__class__
        payment = getattr(self, cls.get_payment_column())
        debt = getattr(self, cls.get_debt_column())

        if payment.timestamp < debt.timestamp and not debt.is_refund:
            def loctimefmt(ts):
                return timezone.localtime(ts).strftime(
                    '%Y-%m-%d %H:%M:%S'
                )
            raise ValidationError(
                _(
                    'Payment cannot be applied to future debt. '
                    'Payment is dated %(payment_ts)s, while '
                    'debt is dated %(debt_ts)s.'
                ) % {
                    'payment_ts': loctimefmt(payment.timestamp),
                    'debt_ts': loctimefmt(debt.timestamp)
                }
            )
