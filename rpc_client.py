from typing import Dict
from uuid import uuid4

import aiohttp
import asyncio
import json

from config import config

import sys, traceback
sys.path.append("..")


class RPCException(Exception):
    def __init__(self, message):
        super(RPCException, self).__init__(message)


async def call_aiohttp_wallet(method_name: str, coin: str, time_out: int = None, payload: Dict = None) -> Dict:
    coin_family = getattr(getattr(config,"coin"+coin),"coin_family","TRTL")
    full_payload = {
        'params': payload or {},
        'jsonrpc': '2.0',
        'id': str(uuid4()),
        'method': f'{method_name}'
    }
    url = get_wallet_rpc_url(coin.upper())
    timeout = time_out or 32
    if method_name == "save" or method_name == "store":
        timeout = 300
    elif method_name == "sendTransaction":
        timeout = 180
    elif method_name == "createAddress" or method_name == "getSpendKeys":
        timeout = 60
    try:
        if coin_family == "XMR":
            try:
                async with aiohttp.ClientSession(headers={'Content-Type': 'application/json'}) as session:
                    async with session.post(url, json=full_payload, timeout=timeout) as response:
                        # sometimes => "message": "Not enough unlocked money" for checking fee
                        if method_name == "transfer":
                            print('{} - transfer'.format(coin.upper()))
                            print(full_payload)
                        if response.status == 200:
                            res_data = await response.read()
                            res_data = res_data.decode('utf-8')
                            if method_name == "transfer":
                                print(res_data)
                            await session.close()
                            decoded_data = json.loads(res_data)
                            if 'result' in decoded_data:
                                return decoded_data['result']
                            else:
                                print(decoded_data)
                                return None
            except asyncio.TimeoutError:
                print('TIMEOUT: {} COIN_NAME {} - timeout {}'.format(method_name, coin.upper(), timeout))
                return None
            except Exception:
                traceback.print_exc(file=sys.stdout)
                return None
        elif coin_family == "TRTL" or coin_family == "CCX":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=full_payload, timeout=timeout) as response:
                        if response.status == 200:
                            res_data = await response.read()
                            res_data = res_data.decode('utf-8')
                            await session.close()
                            decoded_data = json.loads(res_data)
                            if 'result' in decoded_data:
                                return decoded_data['result']
                            else:
                                print(decoded_data)
                                return None
            except asyncio.TimeoutError:
                print('TIMEOUT: {} COIN_NAME {} - timeout {}'.format(method_name, coin.upper(), timeout))
                return None
            except Exception:
                traceback.print_exc(file=sys.stdout)
                return None
    except asyncio.TimeoutError:
        print('TIMEOUT: method_name: {} - coin_family: {} - timeout {}'.format(method_name, coin_family, timeout))
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def get_wallet_rpc_url(coin: str):
    coin_family = getattr(getattr(config,"coin"+coin),"coin_family","TRTL")
    if coin_family == "TRTL" or coin_family == "CCX" :
        return "http://"+getattr(config,"coin"+coin,config.coinWRKZ).walletservice + '/json_rpc'
    elif coin_family == "XMR":
        return "http://"+getattr(config,"coin"+coin,config.coinWRKZ).wallethost + ":" + \
            str(getattr(config,"coin"+coin,config.coinWRKZ).walletport) \
            + '/json_rpc'


async def call_doge(method_name: str, coin: str, payload: str = None) -> Dict:
    COIN_NAME = coin.upper()
    timeout = 64
    headers = {
        'content-type': 'text/plain;',
    }
    if payload is None:
        data = '{"jsonrpc": "1.0", "id":"'+str(uuid4())+'", "method": "'+method_name+'", "params": [] }'
    else:
        data = '{"jsonrpc": "1.0", "id":"'+str(uuid4())+'", "method": "'+method_name+'", "params": ['+payload+'] }'
    url = None
    if COIN_NAME == "DOGE":
        url = f'http://{config.coinDOGE.username}:{config.coinDOGE.password}@{config.coinDOGE.rpchost}/'
    elif COIN_NAME == "LTC":
        url = url = f'http://{config.coinLTC.username}:{config.coinLTC.password}@{config.coinLTC.rpchost}/'
    elif COIN_NAME == "BTC":
        url = url = f'http://{config.coinBTC.username}:{config.coinBTC.password}@{config.coinBTC.rpchost}/'
    elif COIN_NAME == "DASH":
        url = url = f'http://{config.coinDASH.username}:{config.coinDASH.password}@{config.coinDASH.rpchost}/'
    elif COIN_NAME == "BCH":
        url = url = f'http://{config.coinBCH.username}:{config.coinBCH.password}@{config.coinBCH.rpchost}/'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=data, timeout=timeout) as response:
                if response.status == 200:
                    res_data = await response.read()
                    res_data = res_data.decode('utf-8')
                    await session.close()
                    decoded_data = json.loads(res_data)
                    return decoded_data['result']
    except asyncio.TimeoutError:
        print('TIMEOUT: method_name: {} - COIN: {} - timeout {}'.format(method_name, COIN_NAME, timeout))
    except Exception as e:
        traceback.print_exc(file=sys.stdout)