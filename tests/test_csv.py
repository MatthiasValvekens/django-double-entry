import datetime
import pytz

from io import StringIO
from typing import List

from django.test import TestCase
from djmoney.money import Money

from double_entry.forms.csv import BankTransactionInfo
from . import models
from double_entry.forms import csv as forms_csv

SIMPLE_OGMS = ['+++190/5063/21290+++', '+++102/6984/10597+++', '+++191/8961/50327+++', '+++153/4339/48985+++', '+++192/0586/45737+++', '+++180/1915/47216+++', '+++160/9604/46545+++']
TICKET_OGMS = ['+++290/5063/21227+++', '+++202/6984/10534+++', '+++291/8961/50361+++', '+++253/4339/48922+++', '+++292/0586/45771+++', '+++280/1915/47250+++', '+++260/9604/46579+++']

# Quick descriptions (all real users have id 1)
#  2 Happy use case: real user, correct token (OK)
#  3 nonexistent pk (parse OK, lookup not OK)
#  4 real user, wrong token (parse OK, lookup not OK)
#  5 wrong check digits (both not OK)
#  6 totally nonsensical tracking number (both not OK)
#  7 in free-form column (OK)
KBC_LOOKUP_TEST = """Rekeningnummer;Rubrieknaam;Naam;Munt;Afschriftnummer;Datum;Omschrijving;Valuta;Bedrag;Saldo;credit;debet;rekeningnummer tegenpartij;BIC tegenpartij;Naam tegenpartij;Adres tegenpartij;gestructureerde mededeling;Vrije mededeling
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***190/5063/21290***; 
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***154/5988/69980***;
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***176/0220/59911***;
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;***190/5063/21291***;
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;;adlkfjasdlkj
BE00000000000000; ;TEST TEST;EUR; 00000000;08/08/2019;EUROPESE OVERSCHRIJVING NAAR ...;10/08/2019;32,00;100,00;32,00;;BE00 0000 0000 0000;KREDBEBB;DJANGO; ;;***190/5063/21290***
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
        parser = forms_csv.KBCCSVParser(StringIO(KBC_LOOKUP_TEST))
        row: BankTransactionInfo
        rows: List[BankTransactionInfo] = parser.parsed_data
        self.assertEqual(
            {r.line_no for r in rows}, {2,3,4,7},
            msg='\n'.join(map(str, parser.parsed_data))
                + '\n'.join(map(str, parser.errors))
        )
        self.assertEqual(rows[0].account_id, (1, 1))
        self.assertEqual(rows[1].account_id, (1, 9000000))
        self.assertEqual(rows[2].account_id, (1, 1))
        self.assertEqual(rows[3].account_id, (1, 1))
        for row in rows:
            with self.subTest(line_no=row.line_no):
                self.assertEqual(row.amount, Money(32, 'EUR'))
                self.assertEqual(
                    row.timestamp, pytz.timezone('Europe/Brussels').localize(
                        datetime.datetime(2019, 8, 8, 23, 59, 59, 999999)
                    )
                )
