import csv
import datetime
import json
import os
import re
from itertools import groupby
from os import path

from beancount.ingest.importer import ImporterProtocol
from beancount.core.amount import Amount
from beancount.core.data import EMPTY_SET, new_metadata, Cost, Posting, Price, Transaction
from beancount.core.number import D, round_to


class Importer(ImporterProtocol):

    def __init__(self, currency, account):
        self.currency = currency
        self.account = account

    def name(self) -> str:
        return 'Coinbase Pro'

    def identify(self, file) -> bool:
        return (re.match("account.csv", path.basename(file.name)) and
                re.match("portfolio,type,time,amount,balance,amount/balance unit,transfer id,trade id,order id", file.head()))

    def extract(self, file, existing_entries=None) -> list:
        with open(file.name, 'r') as _file:
            transactions = list(csv.DictReader(_file))
        entries = []
        sorted_transactions = sorted(
            transactions,
            key=lambda tx: (tx['time'], tx['type']),
        )
        transactions_by_order = groupby(
            transactions,
            lambda tx: tx['order id'],
        )
        for order_id, transfers in transactions_by_order:
            if order_id == '':
                for transfer in transfers:
                    tx_date = datetime.datetime.strptime(transfer['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    value = D(transfer['amount'])
                    currency = transfer['amount/balance unit']
                    account = f'{self.account}:{currency}'

                    if transfer['type'] == 'deposit':
                        metadata = {'transferid': transfer['transfer id']}
                        from_account = f'Assets:Wallets:{currency}'
                        deposit = Posting(account, None, None, None, None, None)
                        withdrawal = Posting(
                            from_account,
                            Amount(-value, currency),
                            None, None, None, None,
                        )
                        entry = Transaction(
                            new_metadata(file.name, 0, metadata),
                            tx_date.date(),
                            '*',
                            'Deposit',
                            '',
                            EMPTY_SET,
                            EMPTY_SET,
                            [withdrawal, deposit],
                        )
                        entries.append(entry)

                    if transfer['type'] == 'withdrawal':
                        metadata = {'transferid': transfer['transfer id']}
                        to_account = f'Assets:Wallets:{currency}'
                        deposit = Posting(to_account, None, None, None, None, None)
                        withdrawal = Posting(
                            account,
                            Amount(value, currency),
                            None, None, None, None,
                        )
                        entry = Transaction(
                            new_metadata(file.name, 0, metadata),
                            tx_date.date(),
                            '*',
                            f'Withdraw {currency}',
                            '',
                            EMPTY_SET,
                            EMPTY_SET,
                            [withdrawal, deposit],
                        )
                        entries.append(entry)

            else:
                fee_amount = 0
                fee_currency = None
                increase_amount = 0
                increase_currency = None
                postings = []
                reduce_amount = 0
                reduce_currency = None
                title = ' '
                trade_type = None
                tx_date = None

                for transfer in transfers:
                    if tx_date is None:
                        tx_date = datetime.datetime.strptime(transfer['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
                    metadata = {'orderid': transfer['order id']}
                    currency = transfer['amount/balance unit']
                    value = D(transfer['amount'])
                    account = f'{self.account}:{currency}'

                    if transfer['type'] == 'match':
                        if value < 0:
                            reduce_amount -= value
                            if reduce_currency is None:
                                reduce_currency = currency
                            if reduce_currency == 'USD':
                                trade_type = 'Buy'
                            if trade_type is None:
                                trade_type = 'Swap'
                        else:
                            increase_amount += value
                            if increase_currency is None:
                                increase_currency = currency
                            if increase_currency == 'USD':
                                trade_type = 'Sell'
                            if trade_type is None:
                                trade_type = 'Swap'

                    if transfer['type'] == 'fee':
                        fee_amount += value
                        if fee_currency is None:
                            fee_currency = currency


                if trade_type == 'Buy':
                    cost_amount = None
                    if currency == 'USD':
                        cost_amount = Cost(reduce_amount/increase_amount, 'USD', None, None)
                    title = f' {increase_amount} {increase_currency}'
                    postings.append(
                        Posting(f'{self.account}:{increase_currency}', Amount(increase_amount, increase_currency), cost_amount, None, None, None),
                    )
                    if fee_currency:
                        postings.append(
                            Posting(f'Expenses:Coinbase-Pro:{fee_currency}:Fees',
                                    Amount(-fee_amount, fee_currency), None, None, None, None)
                        )
                    postings.append(
                        Posting(f'{self.account}:{currency}', Amount(-reduce_amount + fee_amount, reduce_currency), None, None, None, None)
                    )
                else: # Sell or Swap
                    if fee_currency:
                        postings.append(
                            Posting(f'Assets:Coinbase-Pro:{fee_currency}',
                                    Amount(fee_amount, fee_currency), None, None, None, None)
                        )
                        postings.append(
                            Posting(f'Expenses:Coinbase-Pro:{fee_currency}:Fees',
                                    Amount(-fee_amount, fee_currency), None, None, None, None)
                        )

                    if trade_type == 'Sell':
                        price = Amount(increase_amount/reduce_amount, 'USD')
                        postings.append(
                            Posting(f'{self.account}:{reduce_currency}', Amount(-reduce_amount, reduce_currency), None, price, None, None),
                        )
                        title = f' {reduce_amount} {reduce_currency}'
                    else:
                        postings.append(
                            Posting(f'{self.account}:{reduce_currency}', Amount(-reduce_amount, reduce_currency), None, None, None, None),
                        )
                        title = f' {reduce_amount} {reduce_currency} for {increase_amount} {increase_currency}'

                    postings.append(
                        Posting(f'{self.account}:{increase_currency}', Amount(increase_amount, increase_currency), None, None, None, None),
                    )
                    postings.append(
                        Posting('Income:Coinbase-Pro:PnL', None, None, None, None, None)
                    )

                entry = Transaction(
                    new_metadata(file.name, 0, metadata),
                    tx_date.date(),
                    '*',
                    f'{trade_type}{title}',
                    '',
                    EMPTY_SET,
                    EMPTY_SET,
                    postings,
                )
                entries.append(entry)

        return entries
