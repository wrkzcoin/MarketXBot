from typing import List, Dict
import json
from uuid import uuid4
import rpc_client
import aiohttp
import asyncio
import time

from config import config

import sys
sys.path.append("..")

FEE_PER_BYTE_COIN = config.Fee_Per_Byte_Coin.split(",")

class RPCException(Exception):
    def __init__(self, message):
        super(RPCException, self).__init__(message)


async def walletapi_send_transaction(from_address: str, to_address: str, amount: int, coin: str) -> str:
    time_out = 300
    COIN_NAME = coin.upper()
    if COIN_NAME not in FEE_PER_BYTE_COIN:
        json_data = {
            "destinations": [{"address": to_address, "amount": amount}],
            "mixin": get_mixin(COIN_NAME),
            "fee": get_tx_fee(COIN_NAME),
            "sourceAddresses": [
                from_address
            ],
            "paymentID": "",
            "changeAddress": from_address
        }
    else:
        json_data = {
            "destinations": [{"address": to_address, "amount": amount}],
            "mixin": get_mixin(COIN_NAME),
            "sourceAddresses": [
                from_address
            ],
            "paymentID": "",
            "changeAddress": from_address
        }
    method = "/transactions/send/advanced"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(get_wallet_api_url(COIN_NAME) + method, headers=get_wallet_api_header(COIN_NAME), json=json_data, timeout=time_out) as response:
                json_resp = await response.json()
                if response.status == 200 or response.status == 201:
                    if COIN_NAME not in FEE_PER_BYTE_COIN:
                        return {"transactionHash": json_resp['transactionHash'], "fee": get_tx_fee(COIN_NAME)}
                    else:
                        return {"transactionHash": json_resp['transactionHash'], "fee": json_resp['fee']}
                elif 'errorMessage' in json_resp:
                    raise RPCException(json_resp['errorMessage'])
    except asyncio.TimeoutError:
        print('TIMEOUT: {} COIN_NAME {} - timeout {}'.format(method, COIN_NAME, time_out))



async def walletapi_send_transaction_id(from_address: str, to_address: str, amount: int, paymentid: str, coin: str) -> str:
    time_out = 300
    COIN_NAME = coin.upper()
    if COIN_NAME not in FEE_PER_BYTE_COIN:
        json_data = {
            'sourceAddresses': [from_address],
            'destinations': [{
                "amount": amount,
                "address": to_address
            }],
            'fee': get_tx_fee(COIN_NAME),
            'mixin': get_mixin(COIN_NAME),
            'paymentID': paymentid
        }
    else:
        json_data = {
            'sourceAddresses': [from_address],
            'destinations': [{
                "amount": amount,
                "address": to_address
            }],
            'mixin': get_mixin(COIN_NAME),
            'paymentID': paymentid
        }
    method = "/transactions/send/advanced"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(get_wallet_api_url(COIN_NAME) + method, headers=get_wallet_api_header(COIN_NAME), json=json_data, timeout=time_out) as response:
                json_resp = await response.json()
                if response.status == 200 or response.status == 201:
                    if COIN_NAME not in FEE_PER_BYTE_COIN:
                        return {"transactionHash": json_resp['transactionHash'], "fee": get_tx_fee(COIN_NAME)}
                    else:
                        return {"transactionHash": json_resp['transactionHash'], "fee": json_resp['fee']}
                elif 'errorMessage' in json_resp:
                    raise RPCException(json_resp['errorMessage'])
    except asyncio.TimeoutError:
        print('TIMEOUT: {} COIN_NAME {} - timeout {}'.format(method, COIN_NAME, time_out))


async def get_transfers_cn(coin: str, height_start: int = None, height_end: int = None):
    time_out = 30
    COIN_NAME = coin.upper()
    method = "/transactions"
    if (height_start is None) or (height_end is None):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(get_wallet_api_url(COIN_NAME) + method, headers=get_wallet_api_header(COIN_NAME), timeout=time_out) as response:
                    json_resp = await response.json()
                    if response.status == 200 or response.status == 201:
                        return json_resp['transactions']
                    elif 'errorMessage' in json_resp:
                        raise RPCException(json_resp['errorMessage'])
        except asyncio.TimeoutError:
            print('TIMEOUT: {} COIN_NAME {} - timeout {}'.format(method, COIN_NAME, time_out))


async def save_walletapi(coin: str):
    time_out = 1200
    COIN_NAME = coin.upper()
    start = time.time()
    method = "/save"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(get_wallet_api_url(COIN_NAME) + method, headers=get_wallet_api_header(COIN_NAME), timeout=time_out) as response:
                if response.status == 200 or response.status == 201:
                    end = time.time()
                    return float(end - start)
                else:
                    return False
    except asyncio.TimeoutError:
        print('TIMEOUT: {} COIN_NAME {} - timeout {}'.format(method, COIN_NAME, time_out))
        return False


def walletapi_get_wallet_api_url(coin: str):
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


def get_tx_fee(coin: str):
    return getattr(config,"coin"+coin,config.coinWRKZ).tx_fee


def get_prefix(coin: str = None):
    return getattr(config,"coin"+coin,config.coinWRKZ).prefix


def get_prefix_char(coin: str = None):
    return getattr(config,"coin"+coin,config.coinWRKZ).prefixChar


def get_wallet_api_url(coin: str):
    COIN_NAME = coin.upper()
    url = "http://"+getattr(config, "coin"+COIN_NAME, config.coinWRKZ).walletapi_host +":"+getattr(config, "coin"+COIN_NAME, config.coinWRKZ).walletapi_port
    return url

 
def get_wallet_api_header(coin: str):
    COIN_NAME = coin.upper()
    headers = {
        'X-API-KEY': f'{getattr(config, "coin"+COIN_NAME, config.coinWRKZ).walletapi_header}',
        'Content-Type': 'application/json'
    }
    return headers


def get_wallet_api_open_str(coin: str):
    COIN_NAME = coin.upper()
    wallet_str = '{"daemonHost":"'+str(getattr(config, "coin"+COIN_NAME, config.coinWRKZ).host)+\
        '", "daemonPort":'+str(getattr(config, "coin"+COIN_NAME, config.coinWRKZ).port)+\
        ', "filename":"'+str(getattr(config, "coin"+COIN_NAME, config.coinWRKZ).walletapi_file)+\
        '", "password":"'+str(getattr(config, "coin"+COIN_NAME, config.coinWRKZ).walletapi_password)+'"}'
    return wallet_str
