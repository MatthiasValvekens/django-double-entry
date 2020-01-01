from io import StringIO
from typing import List

from django.test import TestCase
from djmoney.money import Money

from double_entry.forms.csv import BankTransactionInfo
from . import models
from double_entry.forms import csv as forms_csv

SIMPLE_OGMS = ['+++190/5063/21290+++', '+++102/6984/10597+++', '+++191/8961/50327+++', '+++153/4339/48985+++', '+++192/0586/45737+++', '+++180/1915/47216+++', '+++160/9604/46545+++']
TICKET_OGMS = ['+++290/5063/21227+++', '+++202/6984/10534+++', '+++291/8961/50361+++', '+++253/4339/48922+++', '+++292/0586/45771+++', '+++280/1915/47250+++', '+++260/9604/46579+++']

KBC_TEST_CSV = """Rekeningnummer;Rubrieknaam;Naam;Munt;Afschriftnummer;Datum;Omschrijving;Valuta;Bedrag;Saldo;credit;debet;rekeningnummer tegenpartij;BIC tegenpartij;Naam tegenpartij;Adres tegenpartij;gestructureerde mededeling;Vrije mededeling
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***190/5063/21290***; 
"""

class TestBankCSVs(TestCase):
    fixtures = ['reservations.json', 'simple.json']

    def test_tracking_no_permanence(self):
        # I set the hidden tokens to be the same in both sets of fixtures
        # hence, the payment tracking numbers should be the same up to the
        #  first digit, and the check digits
        c1 = models.SimpleCustomer.objects.get(pk=1)
        c2 = models.TicketCustomer.objects.get(pk=1)
        self.assertEquals(c1.payment_tracking_no, '+++190/5063/21290+++')
        self.assertEquals(c2.payment_tracking_no, '+++290/5063/21227+++')
        c2.hidden_token = bytes.fromhex('deadbeefcafebabe')
        self.assertNotEqual(c2.hidden_token, '+++290/5063/21290+++')


        # these values are tightly coupled to the CSV testing data
        # so we should test that they stay the same
        simple_ogms = [c.payment_tracking_no for c in models.SimpleCustomer.objects.all().order_by('pk')]
        ticket_ogms = [c.payment_tracking_no for c in models.TicketCustomer.objects.all().order_by('pk')]
        self.assertEqual(len(simple_ogms), len(SIMPLE_OGMS))
        self.assertEqual(len(ticket_ogms), len(TICKET_OGMS))
        for ix, (val, exp) in enumerate(zip(simple_ogms, SIMPLE_OGMS)):
            with self.subTest(pk=ix + 1, scheme='simple'):
                self.assertEqual(val, exp)
        for ix, (val, exp) in enumerate(zip(ticket_ogms, TICKET_OGMS)):
            with self.subTest(pk=ix + 1, scheme='ticket'):
                self.assertEqual(val, exp)

    def test_kbc_parse(self):
        parser = forms_csv.KBCCSVParser(StringIO(KBC_TEST_CSV))
        row: BankTransactionInfo
        rows: List[BankTransactionInfo] = list(parser.parsed_data)
        self.assertEqual(rows[0].account_id, (1, 1))
        self.assertEqual(rows[0].amount, Money(32, 'EUR'))
