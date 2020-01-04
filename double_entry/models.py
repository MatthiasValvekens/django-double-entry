import logging
import datetime
from decimal import Decimal
from collections import defaultdict, namedtuple
from typing import Type, Tuple, cast, Optional

from django.db import models
from django.db.models import (
    F, Sum, Case, When, Subquery, OuterRef,
    Value, ExpressionWrapper,
    Max, Prefetch,
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
from djmoney.money import Money

from double_entry.utils import (
    validated_bulk_query, _dt_fallback, make_token,
    decimal_to_money,
    parse_ogm,
    ogm_from_prefix,
)

__all__ = [
    'DoubleBookModel', 'ConcreteAmountMixin', 'BaseDebtRecord',
    'BasePaymentRecord', 'BaseDebtQuerySet', 'BasePaymentQuerySet',
    'BaseTransactionSplit', 'DoubleBookQuerySet', 'nonzero_money_validator',
    'GnuCashCategory'
]

logger = logging.getLogger(__name__)


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
    def get_category(cls, name, create=True):
        if not name:
            return None
        if create:
            obj, created = cls.objects.get_or_create(
                name__iexact=name,
                # need to set defaults when using __iexact
                defaults={'name': name}
            )
        else:
            try:
                obj = cls.objects.get(name__iexact=name)
            except cls.DoesNotExist:
                obj = cls(name=name)
        return obj

    def __str__(self):
        return self.name


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
    TOTAL_AMOUNT_FIELD_COLUMN = 'total_amount'

    # This error message is vague, so subclasses should override it with
    # something that makes more sense.
    insufficient_unmatched_balance_error = _(
        'This account does not have enough unmatched balance to '
        'apply the requested split: unmatched balance is %(balance)s, '
        'but attempted to match %(amount)s.'
    )

    split_manager_name = None
    _split_model = None
    _remote_target_field = None
    _other_half_model = None

    timestamp: datetime
    processed: datetime
    total_amount: Money

    class Meta:
        abstract = True

    # We cannot use __init_subclas__ since we need access to the app's model
    #  registry, which isn't avaiable at that point
    # TODO: figure out if we can hook into the app registry preparation to
    #  call this method at some fixed point in the process, so we don't have
    #  to deal with ensuring it gets called in all accessor methods
    #  same with TransactionPartyMixin
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

        if cls.split_manager_name is None:
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
                    'DoubleBookModel: %s. Please set split_manager_name' % (
                        ', '.join(f.name for f in candidates)
                    )
                )
            split_rel = candidates[0]
            cls.split_manager_name = split_rel.name
        else:
            split_rel = cls._meta.get_field(cls.split_manager_name)

        cls._split_model = split_rel.related_model
        cls._remote_target_field = split_rel.remote_field.name
        
        # the is_candidate condition guarantees that this works
        split_fk_1, split_fk_2 = get_fks_on_split(cls._split_model)
        if issubclass(cls, split_fk_1.related_model):
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
        if cls.split_manager_name is None:
            cls._prepare_split_metadata()
        return getattr(self, cls.split_manager_name)

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
            return  self.total_amount - self.matched_balance

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
    __dupcheck_signature_nt = None
    __dupcheck_sig_fields = None

    class Meta:
        abstract = True

    @property
    def dupcheck_signature(self):
        cls = self.__class__
        if cls.dupcheck_signature_fields is None:
            return None

        if cls.__dupcheck_signature_nt is None:
            # translates foreign keys to the fieldname_id format,
            # which is better for comparisons
            sig_fields = list(
                cls._meta.get_field(fname).column
                for fname in cls.dupcheck_signature_fields
            )
            cls.__dupcheck_signature_nt = namedtuple(
                self.__class__.__name__ + 'DuplicationSignature',
                ['date', 'amount'] + sig_fields
            )
            cls.__dupcheck_sig_fields = sig_fields

        sig_kwargs = {
            field: getattr(self, field) for field in cls.__dupcheck_sig_fields
        }
        # Problem: the resolution of most banks' reporting is a day.
        # Hence, we cannot use an exact timestamp as a cutoff point between
        # imports, which would eliminate the need for duplicate
        # checking in practice.
        sig_kwargs['date'] = timezone.localdate(self.timestamp)
        sig_kwargs['amount'] = self.total_amount.amount
        return cls.__dupcheck_signature_nt(**sig_kwargs)


class ConcreteAmountMixin(models.Model):

    total_amount = MoneyField(
        verbose_name=_('total amount'),
        decimal_places=getattr(settings, 'CURRENCY_DECIMAL_PLACES', 4),
        max_digits=getattr(settings, 'CURRENCY_MAX_DIGITS', 19),
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


class BaseDebtRecord(DoubleBookModel):


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

    gnucash_category = models.ForeignKey(
        GnuCashCategory,
        verbose_name=_('GnuCash category'),
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    objects = BaseDebtQuerySet.as_manager()

    class Meta:
        abstract = True

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


class BasePaymentRecord(DoubleBookModel):

    objects = BasePaymentQuerySet.as_manager()

    class Meta:
        abstract = True

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
        decimal_places=getattr(settings, 'CURRENCY_DECIMAL_PLACES', 4),
        max_digits=getattr(settings, 'CURRENCY_MAX_DIGITS', 19),
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
    strictly_enforce_timestamps = False

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
        strict = self.strictly_enforce_timestamps
        if strict and payment.timestamp < debt.timestamp and not debt.is_refund:
            raise ValidationError(
                _(
                    'Payment cannot be applied to future debt. '
                    'Payment is dated %(payment_ts)s, while '
                    'debt is dated %(debt_ts)s.'
                ) % {
                    'payment_ts': payment.timestamp.isoformat(),
                    'debt_ts': debt.timestamp.isoformat()
                }
            )

NINE_DIGIT_MODPAIR = (783142319, 289747279)

def parse_transaction_no(ogm, prefix_digit: Optional[int]=None, match=None):
    prefix, _ = parse_ogm(ogm, match)

    prefix_str = str(prefix)
    rd_prefix_digit = int(prefix_str[0])
    if prefix_digit is not None and rd_prefix_digit != prefix_digit:
        raise ValueError
    unpack = (int(prefix_str[1:]) * NINE_DIGIT_MODPAIR[1]) % 10**9
    # ignore token digest, it already served its purpose
    return rd_prefix_digit, unpack // 100

class TransactionPartyQuerySet(models.QuerySet):
    model: 'TransactionPartyMixin'
    DEBT_BALANCE_FIELD = 'debt_balance_fromdb'

    def by_payment_tracking_no(self, ogm):
        try:
            prefix_digit, pk = parse_transaction_no(
                ogm, prefix_digit=self.model.payment_tracking_prefix
            )
        except ValueError:
            raise self.model.DoesNotExist()
        return self.get(pk=pk)

    @validated_bulk_query(lambda x: x.payment_tracking_no)
    def by_payment_tracking_nos(self, ogms):
        def compute_pks():
            for ogm in ogms:
                try:
                    yield parse_transaction_no(
                        ogm, prefix_digit=self.model.payment_tracking_prefix
                    )[1]
                except ValueError:
                    continue

        pks = set(x for x in compute_pks())
        if not pks:
            return self.none()
        return self.filter(pk__in=pks)

    def with_debt_annotations(self):
        # annotate debts relation
        # this does NOT compute the debt balance/member annotation
        return self.prefetch_related(
            Prefetch(
                self.model.get_debts_manager_name(),
                queryset=self.model.get_debt_model().objects.with_payments()
            )
        )

    def with_payment_annotations(self):
        # annotate payments relation (for symmetry)
        return self.prefetch_related(
            Prefetch(
                self.model.get_payments_manager_name(),
                queryset=self.model.get_payment_model().objects.with_debts()
            )
        )

    def with_debts_and_payments(self):
        return self.with_debt_annotations().with_payment_annotations()

    def with_debt_balances(self):
        # TODO: figure out if this is even necessary
        cls = self.__class__
        if cls.DEBT_BALANCE_FIELD in self.query.annotations:
            return self

        # prefetch_related doesn't work and leads to
        # confusing but nonetheless absolutely hilarious bugs.
        # prefetching debts as InternalDebtItem.objects.with_payments(),
        # and then annotating Sum(DEBT_BALANCE_FIELD) will cause
        # Django to sum the primary keys of every debt object.

        # hence, we have to make the following sacrifice to the
        # Flying Spaghetti Monster.
        base_debt_qs = self.model.get_debt_model().objects.unpaid()
        tp_remote_fk = self.model.get_debt_remote_fk()
        debt_balance_subq = base_debt_qs.filter(**{
            tp_remote_fk: OuterRef('pk'),
        }).order_by().values(tp_remote_fk).annotate(
            total_balance=Coalesce(
                Sum(
                    DoubleBookQuerySet.UNMATCHED_BALANCE_FIELD,
                    output_field=models.DecimalField()
                ),
                Value(Decimal('0.00')),
                output_field=models.DecimalField()
            )
        ).values('total_balance')

        return self.annotate(**{
            cls.DEBT_BALANCE_FIELD: Coalesce(
                Subquery(debt_balance_subq),
                Value(Decimal('0.00')),
                output_field=models.DecimalField()
            )
        })
        # R'amen

# TODO: auto-enforce equality of transaction parties accross debt/payment splits
#  through reflection
class TransactionPartyMixin(models.Model):

    payment_tracking_prefix: int = None
    _debt_model: Type[BaseDebtRecord] = None
    _payment_model: Type[BasePaymentRecord] = None
    _split_model: Type[BaseDebtPaymentSplit] = None
    _debts_manager_name: str = None
    _payments_manager_name: str = None
    _debt_remote_fk: str = None
    _payment_remote_fk: str = None
    _debt_remote_fk_column: str = None
    _payment_remote_fk_column: str = None

    hidden_token = models.BinaryField(
        max_length=8,
        verbose_name=_('hidden token'),
        help_text=_(
            'Hidden unchanging token, for use in low-security '
            'cryptographic operations. In principle never '
            'displayed to any users.'
        ),
        editable=False,
        default=make_token
    )

    objects = TransactionPartyQuerySet.as_manager()

    class Meta:
        abstract = True

    @classmethod
    def _annotate_model_metadata(cls):
        if cls._debt_model is not None:
            return

        fields = cls._meta.get_fields()
        m2one_fields = [f for f in fields if isinstance(f, ManyToOneRel)]
        m2one_models = set(f.remote_field.model for f in m2one_fields)
        def is_candidate(field, *, is_payment: bool):
            base_class = BasePaymentRecord if is_payment else BaseDebtRecord
            remote_model = field.remote_field.model
            if not issubclass(remote_model, base_class):
                return False
            other_half = remote_model.get_other_half_model()
            return other_half in m2one_models

        if cls._debts_manager_name is None:
            debt_fields = [
                f for f in m2one_fields if is_candidate(f, is_payment=False)
            ]
            if not debt_fields:
                raise TypeError('No candidate for debts relation')
            elif len(debt_fields) > 1:
                raise TypeError(
                    'Too many candidates for debts relation. '
                    'Please set debts_manager_name'
                )
            debts_f = debt_fields[0]
            cls._debts_manager_name = debts_f.name
        else:
            debts_f = cls._meta.get_field(cls._debts_manager_name)
        if cls._payments_manager_name is None:
            payment_fields = [
                f for f in m2one_fields if is_candidate(f, is_payment=True)
            ]
            if not payment_fields:
                raise TypeError('No candidate for payments relation')
            elif len(payment_fields) > 1:
                raise TypeError(
                    'Too many candidates for payments relation. '
                    'Please set payments_manager_name'
                )
            payments_f = payment_fields[0]
            cls._payments_manager_name = payments_f.name
        else:
            payments_f = cls._meta.get_field(cls._payments_manager_name)
        cls._debt_model = debts_f.related_model
        cls._payment_model = payments_f.related_model
        if not issubclass(cls._debt_model, BaseDebtRecord):
            raise TypeError('Debts relation does not point to a debt model')
        if not issubclass(cls._payment_model, BasePaymentRecord):
            raise TypeError(
                'Payments relation does not point to a payment model'
            )
        cls._debt_remote_fk = debts_f.remote_field.name
        cls._payment_remote_fk = payments_f.remote_field.name
        cls._debt_remote_fk_column = debts_f.remote_field.column
        cls._payment_remote_fk_column = payments_f.remote_field.column
        models_consistent = (
            cls._debt_model.get_other_half_model() == cls._payment_model
            and cls._payment_model.get_other_half_model() == cls._debt_model
        )
        if not models_consistent:
            raise TypeError(
                'Payment and debt ledger classes are inconsistent.'
            )
        cls._split_model = cls._debt_model.get_split_model()[0]

    @classmethod
    def get_debts_manager_name(cls):
        cls._annotate_model_metadata()
        return cls._debts_manager_name

    @classmethod
    def get_payments_manager_name(cls):
        cls._annotate_model_metadata()
        return cls._payments_manager_name

    @classmethod
    def get_debt_model(cls):
        cls._annotate_model_metadata()
        return cls._debt_model

    @classmethod
    def get_payment_model(cls):
        cls._annotate_model_metadata()
        return cls._payment_model

    @classmethod
    def get_split_model(cls):
        cls._annotate_model_metadata()
        return cls._split_model

    @classmethod
    def get_debt_remote_fk(cls):
        cls._annotate_model_metadata()
        return cls._debt_remote_fk

    @classmethod
    def get_payment_remote_fk(cls):
        cls._annotate_model_metadata()
        return cls._payment_remote_fk

    @classmethod
    def get_debt_remote_fk_column(cls):
        cls._annotate_model_metadata()
        return cls._debt_remote_fk_column

    @classmethod
    def get_payment_remote_fk_column(cls):
        cls._annotate_model_metadata()
        return cls._payment_remote_fk_column

    @classmethod
    def parse_transaction_no(cls, ogm):
        return parse_transaction_no(ogm, cls.payment_tracking_prefix)[1]

    def _payment_tracking_no(self, formatted):
        type_prefix = self.__class__.payment_tracking_prefix
        if type_prefix is None:
            raise TypeError(
                'Payment tracking prefix not set'
            )
        # memoryview weirdness forces this
        token_seed = bytes(self.hidden_token)[1]
        raw = int('%07d%02d' % (self.pk % 10 ** 7, token_seed % 100))
        obf = (raw * NINE_DIGIT_MODPAIR[0]) % 10**9
        prefix_str = '%s%09d' % (type_prefix, obf)
        return ogm_from_prefix(prefix_str, formatted)

    @cached_property
    def payment_tracking_no(self):
        return self._payment_tracking_no(True)

    @cached_property
    def raw_payment_tracking_no(self):
        return self._payment_tracking_no(False)

    @cached_property
    def debt_balance(self):
        try:
            return decimal_to_money(
                getattr(self, TransactionPartyQuerySet.DEBT_BALANCE_FIELD)
            )
        except AttributeError:
            # let's hope that you called this method with
            # with_debt_annotations, otherwise RIP DB
            # TODO: can we detect prefetched relations easily?
            cls = self.__class__
            # TODO: can we do better than falling back on settings.DEFAULT_CURRENCY?
            return sum(
                (d.balance for d in getattr(self, cls.get_debts_manager_name()).all()),
                Money(0, settings.DEFAULT_CURRENCY)
            )

    @cached_property
    def debt_paid(self):
        # see above
        cls = self.__class__
        return sum(
            (d.amount_paid for d in getattr(self, cls.get_debts_manager_name()).all()),
            Money(0, settings.DEFAULT_CURRENCY)
        )

    @cached_property
    def payment_total(self):
        # this one needs with_payment_annotations to be efficient
        cls = self.__class__
        return sum(
            (d.total_amount for d in getattr(self, cls.get_payments_manager_name()).all()),
            Money(0, settings.DEFAULT_CURRENCY)
        )

    @cached_property
    def to_refund(self):
        return self.payment_total - self.debt_paid
