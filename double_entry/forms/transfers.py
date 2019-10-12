import logging
from typing import Iterable

from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, ugettext,
)

import double_entry.utils
from double_entry.models import TransactionPartyMixin
from double_entry.forms import bulk_utils

logger = logging.getLogger(__name__)

# TODO: case-insensitive column names
# TODO: clearly document parsers
# TODO: delimiter autodetection


# lookbehind doesn't work, since we don't want to constrain the
# prefix to a fixed length

class TransferTransactionIndexBuilder(bulk_utils.TransactionPartyIndexBuilder):

    prefix_digit = None

    def __init__(self, ledger_preparator, prefix_digit):
        super().__init__(ledger_preparator)
        self.prefix_digit = prefix_digit

    @classmethod
    def lookup_key_for_account(cls, account):
        return account.payment_tracking_no

    def ogm_applies(self, ogm):
        try:
            prefix, modulus = double_entry.utils.parse_ogm(ogm)
            return self.prefix_digit == str(prefix)[0]
        except ValueError:
            return False

    def append(self, tinfo):
        string = tinfo.account_lookup_str
        if not self.ogm_applies(string):
            return False
        else:
            self.transaction_index[string].append(tinfo)
            return True

    def base_query_set(self):
        return self.ledger_preparator.transaction_party_model \
            ._default_manager.with_debts_and_payments()

    def execute_query(self) -> Iterable[TransactionPartyMixin]:
        account_qs, unseen = self.base_query_set().by_payment_tracking_nos(
            self.transaction_index.keys(), validate_unseen=True
        )

        # We don't show this error in the interface, since
        # if the OGM validates properly AND is not found in our system,
        # it probably simply corresponds to a transaction that we don't
        # care about
        if unseen:
            logger.debug(
                'OGMs not corresponding to valid user records: %s.',
                ', '.join(unseen)
            )
        return account_qs


class TransferPaymentPreparator(bulk_utils.StandardCreditApportionmentMixin,
                                bulk_utils.DuplicationProtectedPreparator):

    prefix_digit = None

    def get_lookup_builders(self):
        return [
            TransferTransactionIndexBuilder(
                self, prefix_digit=self.prefix_digit
            )
        ]

    multiple_dup_message = _(
        'A bank transfer payment by %(account)s '
        'for amount %(amount)s on date %(date)s appears %(hist)d time(s) '
        'in history, and %(import)d time(s) in '
        'the current batch of data. '
        'Resolution: %(dupcount)d ruled as duplicate(s).'
    )

    single_dup_message = _(
        'A bank transfer payment by %(account)s '
        'for amount %(amount)s on date %(date)s already appears '
        'in the payment history. '
        'Resolution: likely duplicate, skipped processing.'
    )

    # unreadable payment references should be skipped silently
    unparseable_account_message = None

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        kwargs['ogm'] = transaction.account_lookup_str
        return kwargs
