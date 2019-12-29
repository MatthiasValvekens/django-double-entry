from django.db import models
from double_entry import models as base


class SimpleCustomer(base.TransactionPartyMixin):
    name = models.CharField(max_length=100)

class SimpleCustomerDebt(base.BaseDebtRecord):
    # give different names for more meaningful testing
    debtor = models.ForeignKey(
        SimpleCustomer, on_delete=models.CASCADE,
        related_name='debts'
    )

class SimpleCustomerPayment(base.BasePaymentRecord):
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



# TODO: make more complicated models