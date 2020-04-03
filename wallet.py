from typing import List, Dict
import json
from uuid import uuid4
import rpc_client
import aiohttp
import asyncio
import time
import addressvalidation

from config import config

import sys
sys.path.append("..")
FEE_PER_BYTE_COIN = config.Fee_Per_Byte_Coin.split(",")


async def getTransactions(coin: str, firstBlockIndex: int=2000000, blockCount: int= 200000):
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    result = None
    time_out = 64
    if coin_family == "TRTL" or coin_family == "CCX":
        payload = {
            'firstBlockIndex': firstBlockIndex,
            'blockCount': blockCount,
            }
        result = await rpc_client.call_aiohttp_wallet('getTransactions', COIN_NAME, time_out=time_out, payload=payload)
        if result:
            if 'items' in result:
                return result['items']
    return None


async def send_transaction(from_address: str, to_address: str, amount: int, coin: str, acc_index: int = None) -> str:
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    result = None
    time_out = 64
    if coin_family == "TRTL" or coin_family == "CCX":
        if COIN_NAME not in FEE_PER_BYTE_COIN:
            payload = {
                'addresses': [from_address],
                'transfers': [{
                    "amount": amount,
                    "address": to_address
                }],
                'fee': get_tx_fee(COIN_NAME),
                'anonymity': get_mixin(COIN_NAME)
            }
        else:
            payload = {
                'addresses': [from_address],
                'transfers': [{
                    "amount": amount,
                    "address": to_address
                }],
                'anonymity': get_mixin(COIN_NAME)
            }
        result = await rpc_client.call_aiohttp_wallet('sendTransaction', COIN_NAME, time_out=time_out, payload=payload)
        if result:
            if 'transactionHash' in result:
                if COIN_NAME not in FEE_PER_BYTE_COIN:
                    return {"transactionHash": result['transactionHash'], "fee": get_tx_fee(COIN_NAME)}
                else:
                    return {"transactionHash": result['transactionHash'], "fee": result['fee']}
    elif coin_family == "XMR":
        payload = {
            "destinations": [{'amount': amount, 'address': to_address}],
            "account_index": acc_index,
            "subaddr_indices": [],
            "priority": 1,
            "unlock_time": 0,
            "get_tx_key": True,
            "get_tx_hex": False,
            "get_tx_metadata": False
        }
        result = await rpc_client.call_aiohttp_wallet('transfer', COIN_NAME, time_out=time_out, payload=payload)
        if result:
            if ('tx_hash' in result) and ('tx_key' in result):
                return result
    return result


async def send_transaction_id(from_address: str, to_address: str, amount: int, paymentid: str, coin: str) -> str:
    time_out = 32
    COIN_NAME = coin.upper()
    if COIN_NAME not in FEE_PER_BYTE_COIN:
        payload = {
            'addresses': [from_address],
            'transfers': [{
                "amount": amount,
                "address": to_address
            }],
            'fee': get_tx_fee(COIN_NAME),
            'anonymity': get_mixin(COIN_NAME),
            'paymentId': paymentid
        }
    else:
        payload = {
            'addresses': [from_address],
            'transfers': [{
                "amount": amount,
                "address": to_address
            }],
            'anonymity': get_mixin(COIN_NAME),
            'paymentId': paymentid
        }
    result = None
    result = await rpc_client.call_aiohttp_wallet('sendTransaction', COIN_NAME, time_out=time_out, payload=payload)
    if result:
        if 'transactionHash' in result:
            if COIN_NAME not in FEE_PER_BYTE_COIN:
                return {"transactionHash": result['transactionHash'], "fee": get_tx_fee(COIN_NAME)}
            else:
                return {"transactionHash": result['transactionHash'], "fee": result['fee']}
    return result


async def rpc_cn_wallet_save(coin: str):
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    start = time.time()
    if coin_family == "TRTL" or coin_family == "CCX":
        result = await rpc_client.call_aiohttp_wallet('save', coin)
    elif coin_family == "XMR":
        result = await rpc_client.call_aiohttp_wallet('store', coin)
    end = time.time()
    return float(end - start)


async def doge_register(account: str, coin: str, user_server: str = 'DISCORD') -> str:
    COIN_NAME = coin.upper()
    naming = "marketbot_" + account
    if user_server == "TELEGRAM":
        naming = "telemarketbot_" + account
    payload = f'"{naming}"'
    address_call = await rpc_client.call_doge('getnewaddress', COIN_NAME, payload=payload)
    reg_address = {}
    reg_address['address'] = address_call
    payload = f'"{address_call}"'
    key_call = await rpc_client.call_doge('dumpprivkey', COIN_NAME, payload=payload)
    reg_address['privateKey'] = key_call
    if reg_address['address'] and reg_address['privateKey']:
        return reg_address
    return None


async def doge_sendtoaddress(to_address: str, amount: float, comment: str, coin: str, comment_to: str=None) -> str:
    COIN_NAME = coin.upper()
    if comment_to is None:
        comment_to = "marketxbot"
    payload = f'"{to_address}", {amount}, "{comment}", "{comment_to}", true'
    valid_call = await rpc_client.call_doge('sendtoaddress', COIN_NAME, payload=payload)
    return valid_call


async def doge_listtransactions(coin: str, last_count: int = 50):
    COIN_NAME = coin.upper()
    payload = '"*", 50, 0'
    valid_call = await rpc_client.call_doge('listtransactions', COIN_NAME, payload=payload)
    return valid_call

# not use yet
async def doge_listreceivedbyaddress(coin: str):
    COIN_NAME = coin.upper()
    payload = '0, true'
    valid_call = await rpc_client.call_doge('listreceivedbyaddress', COIN_NAME, payload=payload)
    account_list = []
    if len(valid_call) >= 1:
        for item in valid_call:
            account_list.append({"address": item['address'], "account": item['account'], "amount": item['amount']})
    return account_list


async def doge_dumpprivkey(address: str, coin: str) -> str:
    COIN_NAME = coin.upper()
    payload = f'"{address}"'
    key_call = await rpc_client.call_doge('dumpprivkey', COIN_NAME, payload=payload)
    return key_call
    

async def doge_validaddress(address: str, coin: str) -> str:
    COIN_NAME = coin.upper()
    payload = f'"{address}"'
    valid_call = await rpc_client.call_doge('validateaddress', COIN_NAME, payload=payload)
    return valid_call


def get_wallet_api_url(coin: str = None):
    COIN_NAME = None
    if coin is None:
        COIN_NAME = "WRKZ"
    else:
        COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    if coin_family == "TRTL":
        return "http://"+getattr(config,"coin"+COIN_NAME,config.coinWRKZ).wallethost + ":" + \
            str(getattr(config,"coin"+COIN_NAME,config.coinWRKZ).walletport) \
            + '/json_rpc'
    elif coin_family == "XMR":
        return "http://"+getattr(config,"coin"+COIN_NAME,config.coinWRKZ).wallethost + ":" + \
            str(getattr(config,"coin"+COIN_NAME,config.coinWRKZ).walletport) \
            + '/json_rpc'


def get_mixin(coin: str = None):
    return getattr(config,"coin"+coin,config.coinWRKZ).mixin


def get_decimal(coin: str = None):
    return getattr(config,"coin"+coin,config.coinWRKZ).decimal


def get_addrlen(coin: str = None):
    return getattr(config,"coin"+coin,config.coinWRKZ).AddrLen


def get_intaddrlen(coin: str = None):
    return getattr(config,"coin"+coin,config.coinWRKZ).IntAddrLen


def get_tx_fee(coin: str):
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    if coin_family == "TRTL" or coin_family == "CCX" or coin_family == "DOGE":
        return getattr(config,"coin"+COIN_NAME,config.coinWRKZ).tx_fee        
    elif coin_family == "XMR":
        return getattr(config,"coin"+COIN_NAME,config.coinXMR).tx_fee


async def get_tx_fee_xmr(coin: str, amount: int = None, to_address: str = None):
    COIN_NAME = coin.upper()
    timeout = 32
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","XMR")      
    if coin_family == "XMR":
        if COIN_NAME in ["XAM"]:
            payload = {
                "destinations": [{'amount': amount, 'address': to_address}],
                "account_index": 0,
                "subaddr_indices": [0],
                "priority": 0,
                "get_tx_key": True,
                "do_not_relay": True
            }

            result = await rpc_client.call_aiohttp_wallet('transfer', COIN_NAME, time_out=timeout, payload=payload)
            if result:
                if ('tx_hash' in result) and ('tx_key' in result) and ('fee' in result):
                    return result['fee']
        else:
            payload = {
                "destinations": [{'amount': amount, 'address': to_address}],
                "account_index": 0,
                "subaddr_indices": [],
                "get_tx_key": True,
                "do_not_relay": True,
                "get_tx_hex": True,
                "get_tx_metadata": False
            }
            result = await rpc_client.call_aiohttp_wallet('transfer', COIN_NAME, time_out=timeout, payload=payload)
            if result:
                if ('tx_hash' in result) and ('tx_key' in result) and ('fee' in result):
                    return result['fee']


def get_min_tx_amount(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).min_tx_amount


def get_max_tx_amount(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).max_tx_amount

def get_min_sell(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).min_buysell

def get_max_sell(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).max_buysell


def get_prefix(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).prefix


def get_prefix_char(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).prefixChar

def get_confirm_depth(coin: str):
    return int(getattr(config,"coin"+coin,config.coinWRKZ).confirm_depth)

def get_reserved_fee(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).reserved_fee

def get_main_address(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).MainAddress

def num_format_coin(amount, coin: str = None):
    COIN_NAME = None
    if coin is None:
        COIN_NAME = "WRKZ"
    else:
        COIN_NAME = coin.upper()
    
    if COIN_NAME == "DOGE":
        coin_decimal = 1
    elif COIN_NAME == "LTC":
        coin_decimal = 1
    elif COIN_NAME == "BTC":
        coin_decimal = 1
    elif COIN_NAME == "DASH":
        coin_decimal = 1
    elif COIN_NAME == "BCH":
        coin_decimal = 1
    else:
        coin_decimal = get_decimal(COIN_NAME)
    amount_str = 'Invalid.'
    if COIN_NAME == "DOGE" or COIN_NAME == "LTC" or COIN_NAME == "BTC" or COIN_NAME == "BCH":
        return '{:,.6f}'.format(amount)
    if coin_decimal > 100000000:
        amount_str = '{:,.8f}'.format(amount / coin_decimal)
    elif coin_decimal > 1000000:
        amount_str = '{:,.8f}'.format(amount / coin_decimal)
    elif coin_decimal > 10000:
        amount_str = '{:,.6f}'.format(amount / coin_decimal)
    elif coin_decimal > 100:
        amount_str = '{:,.4f}'.format(amount / coin_decimal)
    else:
        amount_str = '{:,.2f}'.format(amount / coin_decimal)
    return amount_str


# XMR
async def validate_address_xmr(address: str, coin: str):
    coin_family = getattr(getattr(config,"coin"+coin),"coin_family","XMR")
    if coin_family == "XMR":
        payload = {
            "address" : address,
            "any_net_type": True,
            "allow_openalias": True
        }
        address_xmr = await rpc_client.call_aiohttp_wallet('validate_address', coin, payload=payload)
        if address_xmr:
            return address_xmr
        else:
            return None


async def make_integrated_address_xmr(address: str, coin: str, paymentid: str = None):
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","XMR")
    if paymentid:
        try:
            value = int(paymentid, 16)
        except ValueError:
            return False
    else:
        paymentid = addressvalidation.paymentid(8)
    if coin_family == "XMR":
        payload = {
            "standard_address" : address,
            "payment_id": {} or paymentid
        }
        address_ia = await rpc_client.call_aiohttp_wallet('make_integrated_address', COIN_NAME, payload=payload)
        if address_ia:
            return address_ia
        else:
            return None


async def get_transfers_xmr(coin: str, height_start: int = None, height_end: int = None):
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","XMR")
    if coin_family == "XMR":
        payload = None
        if height_start and height_end:
            payload = {
                "in" : True,
                "out": True,
                "pending": False,
                "failed": False,
                "pool": False,
                "filter_by_height": True,
                "min_height": height_start,
                "max_height": height_end
            }
        else:
            payload = {
                "in" : True,
                "out": True,
                "pending": False,
                "failed": False,
                "pool": False,
                "filter_by_height": False
            }
        result = await rpc_client.call_aiohttp_wallet('get_transfers', COIN_NAME, payload=payload)
        return result

