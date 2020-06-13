from typing import List, Dict
from datetime import datetime
import time
import json
import asyncio

import daemonrpc_client, rpc_client, wallet, walletapi, addressvalidation
from config import config
import sys, traceback
import os.path

# MySQL
import pymysql, pymysqlpool
import pymysql.cursors

# redis
import redis
redis_pool = None
redis_conn = None
redis_expired = 120

FEE_PER_BYTE_COIN = config.Fee_Per_Byte_Coin.split(",")
XS_COIN = ["DEGO"]

pymysqlpool.logger.setLevel('DEBUG')
myconfig = {
    'host': config.mysql.host,
    'user':config.mysql.user,
    'password':config.mysql.password,
    'database':config.mysql.db,
    'charset':'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit':True
    }

myconfigtipbot = {
    'host': config.mysqltipbot.host,
    'user':config.mysqltipbot.user,
    'password':config.mysqltipbot.password,
    'database':config.mysqltipbot.db,
    'charset':'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit':True
    }

myconfig_proxyweb = {
    'host': config.mysql_proxyweb.host,
    'user':config.mysql_proxyweb.user,
    'password':config.mysql_proxyweb.password,
    'database':config.mysql_proxyweb.db,
    'charset':'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit':True
    }

connPool = pymysqlpool.ConnectionPool(size=4, name='connPool', **myconfig)
conn = connPool.get_connection(timeout=5, retry_num=2)

connPoolTip = pymysqlpool.ConnectionPool(size=2, name='connPoolTip', **myconfigtipbot)
connTip = connPoolTip.get_connection(timeout=5, retry_num=2)

connPoolProxy = pymysqlpool.ConnectionPool(size=2, name='connPoolProxy', **myconfig_proxyweb)
connProxy = connPoolProxy.get_connection(timeout=5, retry_num=2)

sys.path.append("..")

ENABLE_COIN = config.Enable_Coin.split(",")
ENABLE_SWAP = config.Enabe_Swap_Coin.split(",")

# Coin using wallet-api
WALLET_API_COIN = config.Enable_Coin_WalletApi.split(",")

def init():
    global redis_pool
    print("PID %d: initializing redis pool..." % os.getpid())
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True, db=10)


def openRedis():
    global redis_pool, redis_conn
    if redis_conn is None:
        try:
            redis_conn = redis.Redis(connection_pool=redis_pool)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)


# connPool 
def openConnection():
    global conn, connPool
    try:
        if conn is None:
            conn = connPool.get_connection(timeout=5, retry_num=2)
        conn.ping(reconnect=True)  # reconnecting mysql
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()


# connPoolTip 
def openConnectionTip():
    global connTip, connPoolTip
    try:
        if connTip is None:
            connTip = connPoolTip.get_connection(timeout=5, retry_num=2)
        connTip.ping(reconnect=True)  # reconnecting mysql
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance connPoolTip.")
        sys.exit()


# connPoolProxy 
def openConnectionProxy():
    global connProxy, connPoolProxy
    try:
        if connProxy is None:
            connProxy = connPoolProxy.get_connection(timeout=5, retry_num=2)
        connProxy.ping(reconnect=True)  # reconnecting mysql
    except:
        print("ERROR: Unexpected error: Could not connect to MySql instance connPoolProxy.")
        sys.exit()


async def sql_register_user(userID, coin: str, user_server: str, chat_id: int = 0):
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    if user_server == "TELEGRAM" and chat_id == 0:
        return

    global conn
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = None
            result = None
            if coin_family == "TRTL":
                sql = """ SELECT * FROM cn_user_paymentid WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (userID, COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "XMR":
                sql = """ SELECT * FROM xmr_user_paymentid WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "DOGE":
                sql = """ SELECT * FROM doge_user WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            if result is None:
                balance_address = {}
                main_address = getattr(getattr(config,"coin"+COIN_NAME),"MainAddress") if coin_family != "DOGE" else None
                if coin_family == "XMR":
                    balance_address = await wallet.make_integrated_address_xmr(main_address, COIN_NAME)
                    sql = """ INSERT INTO xmr_user_paymentid (`coin_name`, `user_id`, `main_address`, `paymentid`, 
                              `int_address`, `paymentid_ts`, `user_server`, `chat_id`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, str(userID), main_address, balance_address['payment_id'], 
                                balance_address['integrated_address'], int(time.time()), user_server, chat_id))
                    conn.commit()
                elif coin_family == "TRTL":
                    balance_address['payment_id'] = addressvalidation.paymentid()
                    balance_address['integrated_address'] = addressvalidation.make_integrated_cn(main_address, COIN_NAME, balance_address['payment_id'])['integrated_address']
                    sql = """ INSERT INTO cn_user_paymentid (`coin_name`, `user_id`, `main_address`, `paymentid`, 
                              `int_address`, `paymentid_ts`, `user_server`, `chat_id`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, str(userID), main_address, balance_address['payment_id'], 
                                balance_address['integrated_address'], int(time.time()), user_server, chat_id))
                    conn.commit()
                elif coin_family == "DOGE":
                    user_address = await wallet.doge_register(str(userID), COIN_NAME)
                    sql = """ INSERT INTO doge_user (`coin_name`, `user_id`, `address`, `address_ts`, 
                              `privateKey`, `user_server`, `chat_id`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, str(userID), user_address['address'], int(time.time()), user_address['privateKey'], user_server, chat_id))
                    balance_address['address'] = user_address['address']
                    conn.commit()
                return balance_address
            else:
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)

        
async def sql_get_userwallet(userID, coin: str, user_server: str = 'DISCORD'):
    global conn, redis_conn, redis_expired
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            result = None
            if coin_family == "TRTL":
                sql = """ SELECT * FROM cn_user_paymentid 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server` = %s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "XMR":
                sql = """ SELECT * FROM xmr_user_paymentid 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server` = %s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "DOGE":
                sql = """ SELECT * FROM doge_user 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server` = %s LIMIT 1 """
                cur.execute(sql, (str(userID), COIN_NAME, user_server))
                result = cur.fetchone()
            if result:
                userwallet = result
                if coin_family == "XMR" or coin_family == "TRTL":
                    userwallet['balance_wallet_address'] = userwallet['int_address']
                if coin_family == "DOGE":
                    userwallet['balance_wallet_address'] = userwallet['address']
                return userwallet
            else:
                return None
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_update_balances(coin: str):
    global conn, redis_conn, redis_expired, XS_COIN
    updateTime = int(time.time())
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")

    gettopblock = None
    timeout = 12
    try:
        if COIN_NAME not in ["DOGE", "LTC", "BTC", "DASH", "BCH"]:
            gettopblock = await daemonrpc_client.gettopblock(COIN_NAME, time_out=timeout)
        else:
            gettopblock = await rpc_client.call_doge('getblockchaininfo', COIN_NAME)
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        traceback.print_exc(file=sys.stdout)

    height = None
    if gettopblock:
        if coin_family == "TRTL" or coin_family == "XMR":
            height = int(gettopblock['block_header']['height'])
        elif coin_family == "DOGE":
            height = int(gettopblock['blocks'])
        # store in redis
        try:
            openRedis()
            if redis_conn:
                redis_conn.set(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}', str(height))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    else:
        try:
            openRedis()
            if redis_conn and redis_conn.exists(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'):
                height = int(redis_conn.get(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'))
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    if coin_family == "TRTL" and (COIN_NAME not in XS_COIN):
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await walletapi.get_transfers_cn(COIN_NAME)
        if len(get_transfers) >= 1:
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM cn_get_transfers WHERE `coin_name` = %s """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for tx in get_transfers:
                        # add to balance only confirmation depth meet
                        if height > int(tx['blockHeight']) + wallet.get_confirm_depth(COIN_NAME):
                            if ('paymentID' in tx) and (tx['paymentID'] in list_balance_user):
                                if tx['transfers'][0]['amount'] > 0:
                                    list_balance_user[tx['paymentID']] += tx['transfers'][0]['amount']
                            elif ('paymentID' in tx) and (tx['paymentID'] not in list_balance_user):
                                if tx['transfers'][0]['amount'] > 0:
                                    list_balance_user[tx['paymentID']] = tx['transfers'][0]['amount']
                            try:
                                if tx['hash'] not in d:
                                    sql = """ INSERT IGNORE INTO cn_get_transfers (`coin_name`, `txid`, 
                                    `payment_id`, `height`, `timestamp`, `amount`, `fee`, `decimal`, `address`, time_insert) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['hash'], tx['paymentID'], tx['blockHeight'], tx['timestamp'],
                                                      tx['transfers'][0]['amount'], tx['fee'], wallet.get_decimal(COIN_NAME), tx['transfers'][0]['address'], int(time.time())))
                                    conn.commit()
                                    # add to notification list also
                                    sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                    `payment_id`, `height`, `amount`, `fee`, `decimal`) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['hash'], tx['paymentID'], tx['blockHeight'],
                                                      tx['transfers'][0]['amount'], tx['fee'], wallet.get_decimal(COIN_NAME)))
                                    conn.commit()
                            except pymysql.err.Warning as e:
                                print(e)
                            except Exception as e:
                                traceback.print_exc(file=sys.stdout)
                    if len(list_balance_user) > 0:
                        list_update = []
                        timestamp = int(time.time())
                        for key, value in list_balance_user.items():
                            list_update.append((value, timestamp, key))
                        cur.executemany(""" UPDATE cn_user_paymentid SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE paymentid = %s """, list_update)
                        conn.commit()
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    if coin_family == "TRTL" and (COIN_NAME in XS_COIN):
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await wallet.getTransactions(COIN_NAME, int(height)-100000, 100000)
        try:
            if len(get_transfers) >= 1:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM cn_get_transfers WHERE `coin_name` = %s """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for txes in get_transfers:
                        tx_in_block = txes['transactions']
                        for tx in tx_in_block:
                            # Could be one block has two or more tx with different payment ID
                            # add to balance only confirmation depth meet
                            if height > int(tx['blockIndex']) + wallet.get_confirm_depth(COIN_NAME):
                                if ('paymentId' in tx) and (tx['paymentId'] in list_balance_user):
                                    if tx['amount'] > 0:
                                        list_balance_user[tx['paymentId']] += tx['amount']
                                elif ('paymentId' in tx) and (tx['paymentId'] not in list_balance_user):
                                    if tx['amount'] > 0:
                                        list_balance_user[tx['paymentId']] = tx['amount']
                                try:
                                    if tx['transactionHash'] not in d:
                                        addresses = tx['transfers']
                                        address = ''
                                        for each_add in addresses:
                                            if len(each_add['address']) > 0: address = each_add['address']
                                            break
                                            
                                        sql = """ INSERT IGNORE INTO cn_get_transfers (`coin_name`, `txid`, 
                                        `payment_id`, `height`, `timestamp`, `amount`, `fee`, `decimal`, `address`, time_insert) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['transactionHash'], tx['paymentId'], tx['blockIndex'], tx['timestamp'],
                                                          tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME), address, int(time.time())))
                                        conn.commit()
                                        # add to notification list also
                                        sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                        `payment_id`, `height`, `amount`, `fee`, `decimal`) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['transactionHash'], tx['paymentId'], tx['blockIndex'],
                                                          tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME)))
                                        conn.commit()
                                except pymysql.err.Warning as e:
                                    print(e)
                                except Exception as e:
                                    traceback.print_exc(file=sys.stdout)
                            else:
                                print('{} has some tx but not yet meet confirmation depth.'.format(COIN_NAME))
            if list_balance_user and len(list_balance_user) >= 1:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT coin_name, payment_id, SUM(amount) AS txIn FROM cn_get_transfers 
                              WHERE coin_name = %s AND amount > 0 
                              GROUP BY payment_id """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    timestamp = int(time.time())
                    list_update = []
                    if result and len(result) > 0:
                        for eachTxIn in result:
                            list_update.append((eachTxIn['txIn'], timestamp, eachTxIn['payment_id']))
                        cur.executemany(""" UPDATE cn_user_paymentid SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE paymentid = %s """, list_update)
                        conn.commit()
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
    elif coin_family == "XMR":
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await wallet.get_transfers_xmr(COIN_NAME)
        if len(get_transfers) >= 1:
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM xmr_get_transfers WHERE `coin_name` = %s """
                    cur.execute(sql, (COIN_NAME,))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for tx in get_transfers['in']:
                        # add to balance only confirmation depth meet
                        if height > int(tx['height']) + wallet.get_confirm_depth(COIN_NAME):
                            if ('payment_id' in tx) and (tx['payment_id'] in list_balance_user):
                                list_balance_user[tx['payment_id']] += tx['amount']
                            elif ('payment_id' in tx) and (tx['payment_id'] not in list_balance_user):
                                list_balance_user[tx['payment_id']] = tx['amount']
                            try:
                                if tx['txid'] not in d:
                                    sql = """ INSERT IGNORE INTO xmr_get_transfers (`coin_name`, `in_out`, `txid`, 
                                    `payment_id`, `height`, `timestamp`, `amount`, `fee`, `decimal`, `address`, time_insert) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['type'].upper(), tx['txid'], tx['payment_id'], tx['height'], tx['timestamp'],
                                                      tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME), tx['address'], int(time.time())))
                                    conn.commit()
                                    # add to notification list also
                                    sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                    `payment_id`, `height`, `amount`, `fee`, `decimal`) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s) """
                                    cur.execute(sql, (COIN_NAME, tx['txid'], tx['payment_id'], tx['height'],
                                                      tx['amount'], tx['fee'], wallet.get_decimal(COIN_NAME)))
                            except pymysql.err.Warning as e:
                                print(e)
                            except Exception as e:
                                traceback.print_exc(file=sys.stdout)
                    if len(list_balance_user) > 0:
                        list_update = []
                        timestamp = int(time.time())
                        for key, value in list_balance_user.items():
                            list_update.append((value, timestamp, key))
                        cur.executemany(""" UPDATE xmr_user_paymentid SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE paymentid = %s """, list_update)
                        conn.commit()
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    elif coin_family == "DOGE":
        print('SQL: Updating get_transfers '+COIN_NAME)
        get_transfers = await wallet.doge_listtransactions(COIN_NAME)
        if get_transfers and len(get_transfers) >= 1:
            try:
                openConnection()
                with conn.cursor() as cur:
                    sql = """ SELECT * FROM doge_get_transfers WHERE `coin_name` = %s AND `category` IN (%s, %s) """
                    cur.execute(sql, (COIN_NAME, 'receive', 'send'))
                    result = cur.fetchall()
                    d = [i['txid'] for i in result]
                    # print('=================='+COIN_NAME+'===========')
                    # print(d)
                    # print('=================='+COIN_NAME+'===========')
                    list_balance_user = {}
                    for tx in get_transfers:
                        # add to balance only confirmation depth meet
                        if wallet.get_confirm_depth(COIN_NAME) < int(tx['confirmations']):
                            if ('address' in tx) and (tx['address'] in list_balance_user) and (tx['amount'] > 0):
                                list_balance_user[tx['address']] += tx['amount']
                            elif ('address' in tx) and (tx['address'] not in list_balance_user) and (tx['amount'] > 0):
                                list_balance_user[tx['address']] = tx['amount']
                            try:
                                if tx['txid'] not in d:
                                    if tx['category'] == "receive":
                                        sql = """ INSERT IGNORE INTO doge_get_transfers (`coin_name`, `txid`, `blockhash`, 
                                        `address`, `blocktime`, `amount`, `confirmations`, `category`, `time_insert`) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['txid'], tx['blockhash'], tx['address'],
                                                          tx['blocktime'], tx['amount'], tx['confirmations'], tx['category'], int(time.time())))
                                        conn.commit()
                                    # add to notification list also, doge payment_id = address
                                    if (tx['amount'] > 0) and tx['category'] == 'receive':
                                        sql = """ INSERT IGNORE INTO notify_new_tx (`coin_name`, `txid`, 
                                        `payment_id`, `blockhash`, `amount`, `decimal`) 
                                        VALUES (%s, %s, %s, %s, %s, %s) """
                                        cur.execute(sql, (COIN_NAME, tx['txid'], tx['address'], tx['blockhash'],
                                                          tx['amount'], wallet.get_decimal(COIN_NAME)))
                            except pymysql.err.Warning as e:
                                print(e)
                            except Exception as e:
                                traceback.print_exc(file=sys.stdout)
                    if len(list_balance_user) > 0:
                        list_update = []
                        timestamp = int(time.time())
                        for key, value in list_balance_user.items():
                            list_update.append((value, timestamp, key))
                        cur.executemany(""" UPDATE doge_user SET `actual_balance` = %s, `lastUpdate` = %s 
                                        WHERE address = %s """, list_update)
                        conn.commit()
            except Exception as e:
                traceback.print_exc(file=sys.stdout)


def sql_user_balance(userID: str, coin: str, user_server: str = 'DISCORD'):
    global conn, redis_conn, redis_expired
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            # Expense (negative)
            sql = """ SELECT SUM(amount_sell) AS OpenOrder FROM open_order WHERE `coin_sell`=%s AND `userid_sell`=%s 
                      AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, userID, 'OPEN'))
            result = cur.fetchone()
            if result:
                OpenOrder = result['OpenOrder']
            else:
                OpenOrder = 0

            # Complete Order could be partial match but data is at the complete_order, they are CompleteOrderAdd (Negative)
            sql = """ SELECT SUM(amount_sell) AS CompleteOrderMinus FROM open_order WHERE `coin_sell`=%s AND `userid_sell`=%s  
                      AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, userID, 'COMPLETE'))
            result = cur.fetchone()
            CompleteOrderMinus = 0
            if result and ('CompleteOrderMinus' in result) and (result['CompleteOrderMinus'] is not None):
                CompleteOrderMinus = result['CompleteOrderMinus']

            # Complete Order could be partial match but data is at the complete_order, they are CompleteOrderAdd (Negative)
            sql = """ SELECT SUM(amount_get_after_fee) AS CompleteOrderMinus2 FROM open_order WHERE `coin_get`=%s AND `userid_get`=%s  
                      AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, userID, 'COMPLETE'))
            result = cur.fetchone()
            CompleteOrderMinus2 = 0
            if result and ('CompleteOrderMinus2' in result) and (result['CompleteOrderMinus2'] is not None):
                CompleteOrderMinus2 = result['CompleteOrderMinus2']

            # Complete Order could be partial match but data is at the complete_order, they are CompleteOrderAdd (Positive)
            sql = """ SELECT SUM(amount_sell_after_fee) AS CompleteOrderAdd FROM open_order WHERE `coin_sell`=%s AND `userid_get`=%s  
                      AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, userID, 'COMPLETE'))
            result = cur.fetchone()
            CompleteOrderAdd = 0
            if result and ('CompleteOrderAdd' in result) and (result['CompleteOrderAdd'] is not None):
                CompleteOrderAdd = result['CompleteOrderAdd']

            # Complete Order could be partial match but data is at the complete_order, they are CompleteOrderAdd (Positive)
            sql = """ SELECT SUM(amount_get_after_fee) AS CompleteOrderAdd2 FROM open_order WHERE `coin_get`=%s AND `userid_sell`=%s  
                      AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, userID, 'COMPLETE'))
            result = cur.fetchone()
            CompleteOrderAdd2 = 0
            if result and ('CompleteOrderAdd2' in result) and (result['CompleteOrderAdd2'] is not None):
                CompleteOrderAdd2 = result['CompleteOrderAdd2']

            # Credit by admin is positive (Positive)
            sql = """ SELECT SUM(amount) AS Credited FROM credit_balance 
                      WHERE `coin_name`=%s AND `to_userid`=%s AND `user_server`=%s """
            cur.execute(sql, (COIN_NAME, userID, user_server))
            result = cur.fetchone()
            if result:
                Credited = result['Credited']
            else:
                Credited = 0

            # When sending tx out, (negative)
            sql = ""
            if coin_family == "TRTL":
                sql = """ SELECT SUM(amount) AS SendingOut FROM cn_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "XMR":
                sql = """ SELECT SUM(amount) AS SendingOut FROM xmr_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "DOGE":
                sql = """ SELECT SUM(amount) AS SendingOut FROM doge_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            cur.execute(sql, (userID, COIN_NAME, user_server))
            result = cur.fetchone()
            if result:
                SendingOut = result['SendingOut']
            else:
                SendingOut = 0

            # When sending tx out, user needs to pay for tx as well (negative)
            sql = ""
            if coin_family == "TRTL":
                sql = """ SELECT SUM(fee) AS FeeExpense FROM cn_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "XMR":
                sql = """ SELECT SUM(fee) AS FeeExpense FROM xmr_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            elif coin_family == "DOGE":
                sql = """ SELECT SUM(fee) AS FeeExpense FROM doge_external_tx 
                          WHERE `user_id`=%s AND `coin_name` = %s AND `user_server`=%s """
            cur.execute(sql, (userID, COIN_NAME, user_server))
            result = cur.fetchone()
            if result:
                FeeExpense = result['FeeExpense']
            else:
                FeeExpense = 0

            # Bought
            DigiBought = 0
            DigitSold = 0
            sql = """ SELECT SUM(item_cost) AS DigiBought FROM digi_bought WHERE `bought_userid`=%s AND `coin_name` = %s AND `buy_user_server`=%s """
            cur.execute(sql, (userID, COIN_NAME, user_server))
            result = cur.fetchone()
            if result:
                DigiBought = result['DigiBought']
            else:
                DigiBought = 0
            # Sold
            sql = """ SELECT SUM(item_cost_after_fee) AS DigitSold FROM digi_bought WHERE `owner_id`=%s AND `coin_name` = %s """
            cur.execute(sql, (userID, COIN_NAME))
            result = cur.fetchone()
            if result:
                DigitSold = result['DigitSold']
            else:
                DigitSold = 0

        SwapIn = 0
        SwapOut = 0
        if user_server == "DISCORD": 
            openConnectionTip()
            with connTip.cursor() as curTip:
                sql = """ SELECT SUM(amount) AS SwapIn FROM discord_swap_balance WHERE `owner_id`=%s AND `coin_name` = %s and `to` = %s """
                curTip.execute(sql, (userID, COIN_NAME, 'MARKETBOT'))
                result = curTip.fetchone()
                if result:
                    SwapIn = result['SwapIn']
                else:
                    SwapIn = 0

                sql = """ SELECT SUM(amount) AS SwapOut FROM discord_swap_balance WHERE `owner_id`=%s AND `coin_name` = %s and `from` = %s """
                curTip.execute(sql, (userID, COIN_NAME, 'MARKETBOT'))
                result = curTip.fetchone()
                if result:
                    SwapOut = result['SwapOut']
                else:
                    SwapOut = 0

        balance = {}
        balance['Adjust'] = 0
        balance['OpenOrder'] = float(OpenOrder) if OpenOrder else 0
        balance['CompleteOrderMinus'] = float(CompleteOrderMinus) if CompleteOrderMinus else 0
        balance['CompleteOrderMinus2'] = float(CompleteOrderMinus2) if CompleteOrderMinus2 else 0
        balance['CompleteOrderAdd'] = float(CompleteOrderAdd) if CompleteOrderAdd else 0
        balance['CompleteOrderAdd2'] = float(CompleteOrderAdd2) if CompleteOrderAdd2 else 0
        balance['Credited'] = float(Credited) if Credited else 0
        balance['SendingOut'] = float(SendingOut) if SendingOut else 0
        balance['FeeExpense'] = float(FeeExpense) if FeeExpense else 0
        balance['SwapIn'] = float(SwapIn) if SwapIn else 0
        balance['SwapOut'] = float(SwapOut) if SwapOut else 0
        balance['DigitSold'] = float(DigitSold) if DigitSold else 0
        balance['DigiBought'] = float(DigiBought) if DigiBought else 0
        balance['Adjust'] = balance['Credited'] + balance['CompleteOrderAdd']  + balance['CompleteOrderAdd2'] + balance['SwapIn'] \
                             - balance['CompleteOrderMinus'] - balance['CompleteOrderMinus2'] - balance['OpenOrder'] \
                             - balance['SendingOut'] - balance['FeeExpense'] - balance['SwapOut'] \
                             + balance['DigitSold'] - balance['DigiBought']
        #print(COIN_NAME)
        #print(balance)
        return balance
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_cn_xmr_credit(user_from: str, to_user: str, amount: float, coin: str, reason: str, user_server: str = 'DISCORD'):
    global conn
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    COIN_NAME = coin.upper()
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ INSERT INTO credit_balance (`coin_name`, `from_userid`, `to_userid`, `amount`, `decimal`, `credit_date`, `reason`, `user_server`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (COIN_NAME, user_from, to_user, amount, wallet.get_decimal(COIN_NAME), int(time.time()), reason, user_server))
            conn.commit()
        return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_external_cn_xmr_single(user_server: str, user_from: str, amount: float, to_address: str, coin: str, paymentid: str = None):
    global conn, XS_COIN
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        tx_hash = None
        if coin_family == "XMR":
            tx_hash = await wallet.send_transaction('TIPBOT', to_address, 
                                                    amount, COIN_NAME, 0)
            if tx_hash:
                with conn.cursor() as cur: 
                    sql = """ INSERT INTO xmr_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                              `date`, `tx_hash`, `tx_key`, `user_server`) 
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (COIN_NAME, user_from, amount, tx_hash['fee'], wallet.get_decimal(COIN_NAME), to_address, 
                    int(time.time()), tx_hash['tx_hash'], tx_hash['tx_key'], user_server))
                    conn.commit()
            return tx_hash
        elif (coin_family == "TRTL" or coin_family == "CCX") and (COIN_NAME not in XS_COIN):
            from_address = wallet.get_main_address(COIN_NAME)
            if paymentid is None:
                tx_hash = await walletapi.walletapi_send_transaction(from_address, to_address, 
                                                                     amount, COIN_NAME)
                if tx_hash:
                    with conn.cursor() as cur: 
                        sql = """ INSERT INTO cn_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                                  `date`, `tx_hash`, `user_server`) 
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                        cur.execute(sql, (COIN_NAME, user_from, amount, tx_hash['fee'], wallet.get_decimal(COIN_NAME), to_address, 
                        int(time.time()), tx_hash['transactionHash'], user_server))
                        conn.commit()
            else:
                tx_hash = await walletapi.walletapi_send_transaction_id(from_address, to_address, 
                                                                        amount, paymentid, COIN_NAME)
                if tx_hash:
                    with conn.cursor() as cur: 
                        sql = """ INSERT INTO cn_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                                  `date`, `tx_hash`, `user_server`) 
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                        cur.execute(sql, (COIN_NAME, user_from, amount, tx_hash['fee'], wallet.get_decimal(COIN_NAME), to_address, 
                        int(time.time()), tx_hash['transactionHash'], user_server))
                        conn.commit()
            return tx_hash
        elif (coin_family == "TRTL" or coin_family == "CCX") and (COIN_NAME in XS_COIN):
            # TODO: check fee
            from_address = wallet.get_main_address(COIN_NAME)
            tx_fee = wallet.get_tx_fee(COIN_NAME)
            if paymentid is None:
                tx_hash = await wallet.send_transaction(from_address, to_address, 
                                                        amount, COIN_NAME)
                if tx_hash:
                    with conn.cursor() as cur: 
                        sql = """ INSERT INTO cn_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                                  `date`, `tx_hash`, `user_server`) 
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                        cur.execute(sql, (COIN_NAME, user_from, amount, tx_fee, wallet.get_decimal(COIN_NAME), to_address, 
                        int(time.time()), tx_hash['transactionHash'], user_server))
                        conn.commit()
            else:
                tx_hash = await wallet.send_transaction_id(from_address, to_address, 
                                                           amount, paymentid, COIN_NAME)
                if tx_hash:
                    with conn.cursor() as cur: 
                        sql = """ INSERT INTO cn_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `decimal`, `to_address`, 
                                  `date`, `tx_hash`, `user_server`) 
                                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                        cur.execute(sql, (COIN_NAME, user_from, amount, tx_fee, wallet.get_decimal(COIN_NAME), to_address, 
                        int(time.time()), tx_hash['transactionHash'], user_server))
                        conn.commit()
            return tx_hash
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_external_doge(user_server: str, user_from: str, amount: float, fee: float, to_address: str, coin: str):
    global conn
    COIN_NAME = coin.upper()
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    try:
        openConnection()
        print("DOGE EXTERNAL: ")
        print((to_address, amount, user_from, COIN_NAME))
        txHash = await wallet.doge_sendtoaddress(to_address, amount, user_from, COIN_NAME)
        print("COMPLETE DOGE EXTERNAL TX")
        with conn.cursor() as cur: 
            sql = """ INSERT INTO doge_external_tx (`coin_name`, `user_id`, `amount`, `fee`, `to_address`, 
                      `date`, `tx_hash`, `user_server`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (COIN_NAME, user_from, amount, fee, to_address, int(time.time()), txHash, user_server))
            conn.commit()
        return txHash
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_stats_coin(coin: str):
    global conn
    COIN_NAME = coin.upper()
    try:
        openConnection()
        duration_24h = float(time.time()) - 24*3600
        duration_7d = float(time.time()) - 7*24*3600
        duration_30d = float(time.time()) - 30*24*3600
        with conn.cursor() as cur:
            # 24h
            sql = """ SELECT SUM(amount_sell) AS sell_trade24h FROM open_order WHERE `coin_sell`=%s 
                      AND order_completed_date>%s AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, duration_24h, 'COMPLETE'))
            result = cur.fetchone()
            if result:
                sell_trade24h = result['sell_trade24h']
            else:
                sell_trade24h = 0

            sql = """ SELECT SUM(amount_get) AS buy_trade24h FROM open_order WHERE `coin_get`=%s 
                      AND order_completed_date>%s AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, duration_24h, 'COMPLETE'))
            result = cur.fetchone()
            if result:
                buy_trade24h = result['buy_trade24h']
            else:
                buy_trade24h = 0

            # 7d
            sql = """ SELECT SUM(amount_sell) AS sell_trade7d FROM open_order WHERE `coin_sell`=%s 
                      AND order_completed_date>%s AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, duration_7d, 'COMPLETE'))
            result = cur.fetchone()
            if result:
                sell_trade7d = result['sell_trade7d']
            else:
                sell_trade7d = 0

            sql = """ SELECT SUM(amount_get) AS buy_trade7d FROM open_order WHERE `coin_get`=%s 
                      AND order_completed_date>%s AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, duration_7d, 'COMPLETE'))
            result = cur.fetchone()
            if result:
                buy_trade7d = result['buy_trade7d']
            else:
                buy_trade7d = 0
            # 30d
            sql = """ SELECT SUM(amount_sell) AS sell_trade30d FROM open_order WHERE `coin_sell`=%s 
                      AND order_completed_date>%s AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, duration_30d, 'COMPLETE'))
            result = cur.fetchone()
            if result:
                sell_trade30d = result['sell_trade30d']
            else:
                sell_trade30d = 0

            sql = """ SELECT SUM(amount_get) AS buy_trade30d FROM open_order WHERE `coin_get`=%s 
                      AND order_completed_date>%s AND `status`=%s
                  """
            cur.execute(sql, (COIN_NAME, duration_30d, 'COMPLETE'))
            result = cur.fetchone()
            if result:
                buy_trade30d = result['buy_trade30d']
            else:
                buy_trade30d = 0

            coin_stats = {}
            coin_stats['vol_24h'] = (sell_trade24h if sell_trade24h else 0) + (buy_trade24h if buy_trade24h else 0)
            coin_stats['vol_7d'] = (sell_trade7d if sell_trade7d else 0) + (buy_trade7d if buy_trade7d else 0)
            coin_stats['vol_30d'] = (sell_trade30d if sell_trade30d else 0) + (buy_trade30d if buy_trade30d else 0)
            return coin_stats
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


# use to store data
async def sql_store_openorder(msg_id: str, msg_content: str, coin_sell: str, real_amount_sell: float, 
                              amount_sell_after_fee: float, userid_sell: str, coin_get: str, 
                              real_amount_get: float, amount_get_after_fee: float, sell_div_get: float, 
                              sell_user_server: str = 'DISCORD'):
    global conn
    sell_user_server = sell_user_server.upper()
    if sell_user_server not in ['DISCORD', 'TELEGRAM']:
        return

    coin_sell = coin_sell.upper()
    coin_get = coin_get.upper()
    if real_amount_sell == 0 or amount_sell_after_fee == 0 or real_amount_get == 0 \
    or amount_get_after_fee == 0 or sell_div_get == 0:
        print("Catch zero amount in {sql_store_openorder}!!!")
        return False
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ INSERT INTO open_order (`msg_id`, `msg_content`, `coin_sell`, `coin_sell_decimal`, 
                      `amount_sell`, `amount_sell_after_fee`, `userid_sell`, `coin_get`, `coin_get_decimal`, 
                      `amount_get`, `amount_get_after_fee`, `sell_div_get`, `order_created_date`, `pair_name`, 
                      `status`, `sell_user_server`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (str(msg_id), msg_content, coin_sell, wallet.get_decimal(coin_sell),
                              real_amount_sell, amount_sell_after_fee, userid_sell, coin_get, wallet.get_decimal(coin_get),
                              real_amount_get, amount_get_after_fee, sell_div_get, float("%.3f" % time.time()), coin_sell + "-" + coin_get, 
                              'OPEN', sell_user_server))
            conn.commit()
            return cur.lastrowid
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


# use if same rate, then update them up.
async def sql_get_order_by_sellerid_pair_rate(sell_user_server: str, userid_sell: str, coin_sell: str, coin_get: str, sell_div_get: float, 
                                              real_amount_sell, real_amount_buy, fee_sell, fee_buy, status: str = 'OPEN'):
    global conn
    sell_user_server = sell_user_server.upper()
    if sell_user_server not in ['DISCORD', 'TELEGRAM']:
        return

    if real_amount_sell == 0 or real_amount_buy == 0 or fee_sell == 0 \
    or fee_buy == 0 or sell_div_get == 0:
        print("Catch zero amount in {sql_get_order_by_sellerid_pair_rate}!!!")
        return False
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT * FROM open_order WHERE `userid_sell`=%s AND `coin_sell` = %s 
                      AND coin_get=%s AND sell_div_get=%s AND `status`=%s AND `sell_user_server`=%s ORDER BY order_created_date DESC LIMIT 1"""
            cur.execute(sql, (userid_sell, coin_sell, coin_get, sell_div_get, status, sell_user_server))
            result = cur.fetchone()
            if result:
                # then update by adding more amount to it
                sql = """ UPDATE open_order SET amount_sell=amount_sell+%s, amount_sell_after_fee=amount_sell_after_fee+%s,
                          amount_get=amount_get+%s, amount_get_after_fee=amount_get_after_fee+%s
                          WHERE order_id=%s AND `sell_user_server`=%s LIMIT 1 """
                cur.execute(sql, (real_amount_sell, real_amount_sell-fee_sell, real_amount_buy, real_amount_buy-fee_buy, result['order_id'], sell_user_server))
                conn.commit()
                return {"error": False, "msg": f"We added order to your existing one #{result['order_id']}"}
            else:
                return {"error": True, "msg": None}
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return {"error": True, "msg": "Error with database {sql_get_order_by_sellerid_pair_rate}"}


async def sql_get_open_order_by_sellerid(userid_sell: str, coin: str, status: str = 'OPEN'):
    global conn
    COIN_NAME = coin.upper()
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT * FROM open_order WHERE `userid_sell`=%s AND `coin_sell` = %s 
                      AND `status`=%s ORDER BY order_created_date DESC LIMIT 20 """
            cur.execute(sql, (userid_sell, COIN_NAME, status))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_open_order_by_sellerid_all(userid_sell: str, status: str = 'OPEN'):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT * FROM open_order WHERE `userid_sell`=%s 
                      AND `status`=%s ORDER BY order_created_date DESC LIMIT 20 """
            cur.execute(sql, (userid_sell, status))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_open_order_by_alluser(coin: str, status: str, need_to_buy: bool = False):
    global conn
    COIN_NAME = coin.upper()
    try:
        openConnection()
        with conn.cursor() as cur:
            if need_to_buy: 
                sql = """ SELECT * FROM open_order WHERE `status`=%s AND `coin_get`=%s ORDER BY sell_div_get ASC LIMIT 50 """
                cur.execute(sql, (status, COIN_NAME))
            elif COIN_NAME == 'ALL':
                sql = """ SELECT * FROM open_order WHERE `status`=%s ORDER BY order_created_date DESC LIMIT 50 """
                cur.execute(sql, (status))
            else:
                sql = """ SELECT * FROM open_order WHERE `status`=%s AND `coin_sell`=%s ORDER BY sell_div_get ASC LIMIT 50 """
                cur.execute(sql, (status, COIN_NAME))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_open_order_by_alluser_by_coins(coin1: str, coin2: str, status: str = 'OPEN'):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if coin2.upper() == "ALL":
                sql = """ SELECT * FROM open_order WHERE `status`=%s AND `coin_sell`=%s 
                          ORDER BY sell_div_get ASC LIMIT 50 """
                cur.execute(sql, (status, coin1.upper()))
                result = cur.fetchall()
                return result
            else:
                sql = """ SELECT * FROM open_order WHERE `status`=%s AND `coin_sell`=%s AND `coin_get`=%s 
                          ORDER BY sell_div_get ASC LIMIT 50 """
                cur.execute(sql, (status, coin1.upper(), coin2.upper()))
                result = cur.fetchall()
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_cancel_open_order_by_sellerid(userid_sell: str, coin: str = 'ALL'):
    global conn
    COIN_NAME = coin.upper()
    try:
        openConnection()
        with conn.cursor() as cur:
            if len(coin) < 6:
                if COIN_NAME == 'ALL':
                    sql = """ UPDATE open_order SET `status`=%s, `cancel_date`=%s WHERE `userid_sell`=%s 
                              AND `status`=%s """
                    cur.execute(sql, ('CANCEL', float("%.3f" % time.time()), userid_sell, 'OPEN'))
                    conn.commit()
                    return True
                else:
                    sql = """ UPDATE open_order SET `status`=%s, `cancel_date`=%s WHERE `userid_sell`=%s 
                              AND `status`=%s AND `coin_sell`=%s """
                    cur.execute(sql, ('CANCEL', float("%.3f" % time.time()), userid_sell, 'OPEN', COIN_NAME))
                    conn.commit()
                    return True
            else:
                try:
                    ref_numb = int(coin)
                    sql = """ UPDATE open_order SET `status`=%s, `cancel_date`=%s WHERE `userid_sell`=%s 
                              AND `status`=%s AND `order_id`=%s """
                    cur.execute(sql, ('CANCEL', float("%.3f" % time.time()), userid_sell, 'OPEN', ref_numb))
                    conn.commit()
                    return True
                except ValueError:
                    return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_order_numb(order_num: str, status: str = None):
    global conn
    if status is None: status = 'OPEN'
    if status: status = status.upper()
    try:
        openConnection()
        with conn.cursor() as cur:
            result = None
            if status == "ANY":
                sql = """ SELECT * FROM open_order WHERE `order_id` = %s LIMIT 1 """
                cur.execute(sql, (order_num))
                result = cur.fetchone()
            else:
                sql = """ SELECT * FROM open_order WHERE `order_id` = %s 
                          AND `status`=%s LIMIT 1 """
                cur.execute(sql, (order_num, status))
                result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_match_order_by_sellerid(userid_get: str, ref_numb: str, buy_user_server: str):
    global conn
    buy_user_server = buy_user_server.upper()
    if buy_user_server not in ['DISCORD', 'TELEGRAM']:
        return
    try:
        openConnection()
        with conn.cursor() as cur:
            try:
                ref_numb = int(ref_numb)
                sql = """ UPDATE open_order SET `status`=%s, `order_completed_date`=%s, 
                          `userid_get` = %s, `buy_user_server`=%s 
                          WHERE `order_id`=%s AND `status`=%s """
                cur.execute(sql, ('COMPLETE', float("%.3f" % time.time()), userid_get, buy_user_server, ref_numb, 'OPEN'))
                conn.commit()
                return True
            except ValueError:
                return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_count_open_order_by_sellerid(userID: str, user_server: str, status: str = None):
    global conn
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return

    if status is None: status = 'OPEN'
    if status: status = status.upper()
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT COUNT(*) FROM open_order WHERE `userid_sell` = %s 
                      AND `status`=%s AND `sell_user_server`=%s """
            cur.execute(sql, (userID, status, user_server))
            result = cur.fetchone()
            return int(result['COUNT(*)']) if 'COUNT(*)' in result else 0
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_get_userwallet_by_paymentid(paymentid: str, coin: str, user_server: str = 'DISCORD'):
    global conn
    user_server = user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    COIN_NAME = coin.upper()
    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    try:
        openConnection()
        with conn.cursor() as cur:
            result = None
            if coin_family == "TRTL":
                sql = """ SELECT * FROM cn_user_paymentid 
                          WHERE `paymentid`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (paymentid, COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "XMR":
                sql = """ SELECT * FROM xmr_user_paymentid 
                          WHERE `paymentid`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (paymentid, COIN_NAME, user_server))
                result = cur.fetchone()
            elif coin_family == "DOGE":
                # if doge family, address is paymentid
                sql = """ SELECT * FROM doge_user 
                          WHERE `address`=%s AND `coin_name` = %s AND `user_server`=%s LIMIT 1 """
                cur.execute(sql, (paymentid, COIN_NAME, user_server))
                result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


async def sql_get_new_tx_table(notified: str = 'NO', failed_notify: str = 'NO'):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM notify_new_tx WHERE `notified`=%s AND `failed_notify`=%s """
            cur.execute(sql, (notified, failed_notify,))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_update_notify_tx_table(payment_id: str, owner_id: str, owner_name: str, notified: str = 'YES', failed_notify: str = 'NO'):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ UPDATE notify_new_tx SET `owner_id`=%s, `owner_name`=%s, `notified`=%s, `failed_notify`=%s, 
                      `notified_time`=%s WHERE `payment_id`=%s """
            cur.execute(sql, (owner_id, owner_name, notified, failed_notify, float("%.3f" % time.time()), payment_id,))
            conn.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_deposit_alluser(user: str = 'ALL', coin: str = 'ANY'):
    global conn
    COIN_NAME = coin.upper()
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM cn_get_transfers """
            has_userall = True
            if user != 'ALL':
                sql += """ WHERE `user_id`='"""+user+"""' """
                has_userall = False
            if COIN_NAME != 'ANY':
                if has_userall:
                    sql += """ WHERE `coin_name`='"""+COIN_NAME+"""' """
                else:
                    sql += """ AND `coin_name`='"""+COIN_NAME+"""' """
            cur.execute(sql,)
            result1 = cur.fetchall()

            sql = """ SELECT * FROM xmr_get_transfers """
            has_userall = True
            if user != 'ALL':
                sql += """ WHERE `user_id`='"""+user+"""' """
                has_userall = False
            if COIN_NAME != 'ANY':
                if has_userall:
                    sql += """ WHERE `coin_name`='"""+COIN_NAME+"""' """
                else:
                    sql += """ AND `coin_name`='"""+COIN_NAME+"""' """
            cur.execute(sql,)
            result2 = cur.fetchall()

            sql = """ SELECT * FROM doge_get_transfers """
            has_userall = True
            if user != 'ALL':
                sql += """ WHERE `user_id`='"""+user+"""' """
                has_userall = False
            if COIN_NAME != 'ANY':
                if has_userall:
                    sql += """ WHERE `coin_name`='"""+COIN_NAME+"""' """
                else:
                    sql += """ AND `coin_name`='"""+COIN_NAME+"""' """
            cur.execute(sql,)
            result3 = cur.fetchall()

            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_swap_balance(coin: str, owner_id: str, owner_name: str, from_: str, to_: str, amount: float):
    global connTip, ENABLE_SWAP
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_SWAP:
        return False
    try:
        openConnectionTip()
        with connTip.cursor() as cur: 
            sql = """ INSERT INTO discord_swap_balance (`coin_name`, `owner_id`, `owner_name`, `from`, `to`, `amount`, `decimal`) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (COIN_NAME, owner_id, owner_name, from_, to_, amount, wallet.get_decimal(COIN_NAME)))
            connTip.commit()
        return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


async def sql_get_new_swap_table(notified: str = 'NO', failed_notify: str = 'NO'):
    global connTip
    try:
        openConnectionTip()
        with connTip.cursor() as cur:
            sql = """ SELECT * FROM discord_swap_balance WHERE `notified`=%s AND `failed_notify`=%s AND `to` = %s """
            cur.execute(sql, (notified, failed_notify, 'MARKETBOT',))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def sql_update_notify_swap_table(id: int, notified: str = 'YES', failed_notify: str = 'NO'):
    global connTip
    try:
        openConnectionTip()
        with connTip.cursor() as cur:
            sql = """ UPDATE discord_swap_balance SET `notified`=%s, `failed_notify`=%s, 
                      `notified_time`=%s WHERE `id`=%s """
            cur.execute(sql, (notified, failed_notify, float("%.3f" % time.time()), id,))
            connTip.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


def sql_add_logs_tx(list_tx):
    if len(list_tx) == 0:
        return 0
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT IGNORE INTO `action_tx_logs` (`uuid`, `action`, `user_id`, `user_name`, 
                      `event_date`, `msg_content`, `user_server`, `end_point`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s) """
            cur.executemany(sql, list_tx)
            conn.commit()
            return cur.rowcount
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_info_by_server(server_id: str):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur: 
            sql = """ SELECT * FROM discord_server WHERE `serverid` = %s LIMIT 1 """
            cur.execute(sql, (server_id,))
            result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_addinfo_by_server(server_id: str, servername: str, prefix: str, rejoin: bool = True):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if rejoin:
                sql = """ INSERT INTO `discord_server` (`serverid`, `servername`, `prefix`)
                          VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE 
                          `servername` = %s, `prefix` = %s, `status` = %s """
                cur.execute(sql, (server_id, servername[:28], prefix, servername[:28], prefix, "REJOINED", ))
                conn.commit()
            else:
                sql = """ INSERT INTO `discord_server` (`serverid`, `servername`, `prefix`)
                          VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE 
                          `servername` = %s, `prefix` = %s"""
                cur.execute(sql, (server_id, servername[:28], prefix, servername[:28], prefix))
                conn.commit()
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def sql_merchant_search_by_word(userid: str, search_key: str, limit: int=20):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM digi_order WHERE MATCH (`title`, `keyword`, `desc`) 
                      AGAINST (%s) LIMIT """+str(limit)+"""; """
            cur.execute(sql, (search_key))
            result = cur.fetchall()

            sql = """ INSERT INTO `digi_search` (`user_id`, `search_key`, `search_date`) VALUES (%s, %s, %s) """
            cur.execute(sql, (userid, search_key, int(time.time())))
            conn.commit()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_digi_order_add_data(ref_id: str, title: str, desc: str, keyword: str, coin_name: str, item_cost: float, item_cost_after_fee: float, item_coin_decimal: int, 
    owner_id: str, owner_name: str, status: str='PENDING', sell_user_server: str='DISCORD'):
    global conn
    user_server = sell_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    status = status.upper()
    if status not in ['AVAILABLE','SUSPENDED','PENDING']:
        return
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT INTO `digi_order` (`ref_id`, `title`, `desc`, `keyword`, `coin_name`, `item_cost`, 
                      `item_cost_after_fee`, `item_coin_decimal`, `owner_id`, `owner_name`, 
                      `added_date`, `status`, `sell_user_server`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (ref_id, title, desc, keyword, coin_name, item_cost, item_cost_after_fee,
                              item_coin_decimal, owner_id, owner_name, int(time.time()),
                              status, user_server))
            conn.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


def sql_merchant_update_by_ref(ref: str, what: str, value: str, sell_user_server: str='DISCORD'):
    user_server = sell_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    if what not in ["title", "desc", "status", "numb_bought", "amount"]:
        return
    if what.lower() == "amount":
        amount_data = json.loads(value)
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if what.lower() == "amount":
                sql = """ UPDATE digi_order SET `coin_name` = %s, `item_cost`=%s, `item_cost_after_fee`=%s, 
                          `item_coin_decimal`=%s, `updated_date`=%s 
                          WHERE `ref_id` = %s AND `sell_user_server`=%s """
                cur.execute(sql, (amount_data['coin_name'], amount_data['item_cost'], 
                                  amount_data['item_cost_after_fee'], amount_data['item_coin_decimal'], 
                                  int(time.time()), ref, user_server))
                conn.commit()
                return True
            elif what.lower() == "numb_bought":
                sql = """ UPDATE digi_order SET `numb_bought` = `numb_bought` + 1 
                          WHERE `ref_id` = %s AND `sell_user_server`=%s """
                cur.execute(sql, (value, int(time.time()), ref, user_server))
                conn.commit()
                return True
            else:
                sql = """ UPDATE digi_order SET `""" + what.lower() + """` = %s, `updated_date`=%s 
                          WHERE `ref_id` = %s AND `sell_user_server`=%s """
                cur.execute(sql, (value, int(time.time()), ref, user_server))
                conn.commit()
                return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_list_of_user(userid: str, status:str, created_user_server: str='DISCORD', limit: int=100):
    user_server = created_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    status = status.upper()
    if status not in ['AVAILABLE','SUSPENDED','PENDING', 'ALL']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if status != 'ALL':
                if userid == 'ALL':
                    sql = """ SELECT * FROM digi_order WHERE `status` = %s AND `sell_user_server`=%s 
                              ORDER BY `added_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (status, user_server))
                    result = cur.fetchall()
                    return result
                else:
                    sql = """ SELECT * FROM digi_order WHERE `status` = %s AND `sell_user_server`=%s AND `owner_id`=%s 
                              ORDER BY `added_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (status, user_server, userid))
                    result = cur.fetchall()
                    return result
            else:
                if userid == 'ALL':
                    sql = """ SELECT * FROM digi_order WHERE `sell_user_server`=%s 
                              ORDER BY `added_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (user_server))
                    result = cur.fetchall()
                    return result
                else:
                    sql = """ SELECT * FROM digi_order WHERE `sell_user_server`=%s AND `owner_id`=%s 
                              ORDER BY `added_date` DESC LIMIT """+str(limit)+""" """
                    cur.execute(sql, (user_server, userid))
                    result = cur.fetchall()
                    return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_get_ref(ref: str):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM digi_order WHERE `ref_id` = %s LIMIT 1 """
            cur.execute(sql, (ref))
            result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_add_bought(ref_id: str, buy_ref: str, owner_id: str, bought_userid: str, bought_name: str, coin_name: str, 
    item_cost: float, item_cost_after_fee: float, item_coin_decimal: int, buy_user_server: str='DISCORD'):
    global conn
    user_server = buy_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT INTO `digi_bought` (`ref_id`, `buy_ref`, `owner_id`, `bought_userid`, 
                      `bought_name`, `coin_name`, `item_cost`, `item_cost_after_fee`, `item_coin_decimal`, 
                      `bought_date`, `buy_user_server`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (ref_id, buy_ref, owner_id, bought_userid, bought_name, coin_name,
                              item_cost, item_cost_after_fee, item_coin_decimal, int(time.time()), user_server))
            conn.commit()
            # Update buy count
            sql = """ UPDATE digi_order SET `numb_bought`=`numb_bought`+1 WHERE `ref_id`=%s LIMIT 1 """
            cur.execute(sql, (ref_id))
            conn.commit()
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


def sql_merchant_get_bought_by_ref(userid: str, ref: str, buy_user_server: str='DISCORD'):
    user_server = buy_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if userid == 'ALL':
                sql = """ SELECT * FROM digi_bought WHERE `ref_id` = %s  
                          ORDER BY `bought_date` DESC """
                cur.execute(sql, (ref))
                result = cur.fetchall()
                return result
            else:
                sql = """ SELECT * FROM digi_bought WHERE `ref_id` = %s AND `buy_user_server`=%s AND `bought_userid`=%s LIMIT 1 """
                cur.execute(sql, (ref, user_server, userid))
                result = cur.fetchone()
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_get_buy_by_bought_ref(ref: str, bought_ref: str, buy_user_server: str='DISCORD'):
    user_server = buy_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            if bought_ref == 'ALL':
                sql = """ SELECT * FROM digi_bought WHERE `ref_id` = %s  
                          ORDER BY `bought_date` DESC """
                cur.execute(sql, (ref))
                result = cur.fetchall()
                return result
            else:
                sql = """ SELECT * FROM digi_bought WHERE `buy_ref` = %s AND `buy_user_server`=%s LIMIT 1 """
                cur.execute(sql, (bought_ref, user_server))
                result = cur.fetchone()
                return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_get_buy_by_bought_userid(userid: str, buy_user_server: str='DISCORD', limit: int=25):
    user_server = buy_user_server.upper()
    if user_server not in ['DISCORD', 'TELEGRAM']:
        return
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM digi_bought WHERE `bought_userid` = %s  
                      AND `buy_user_server`=%s ORDER BY `bought_date` DESC LIMIT """+str(limit)+""" """
            cur.execute(sql, (userid, user_server))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_add_file(ref_id: str, file_id: str, md5_hash: str, owner_id: str, original_filename:str, 
    stored_name: str, filesize: float, filetype: str):
    global conn, connProxy
    time_now = int(time.time())
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT INTO `digi_files` (`ref_id`, `file_id`, `md5_hash`, `owner_id`, 
                      `original_filename`, `stored_name`, `filesize`, `filetype`, `stored_date`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (ref_id, file_id, md5_hash, owner_id, original_filename, stored_name,
                              filesize, filetype, time_now))
            conn.commit()
            try:
                openConnectionProxy()
                with connProxy.cursor() as cur:
                    sql = """ INSERT INTO `digi_files` (`ref_id`, `file_id`, `md5_hash`, `owner_id`, 
                              `original_filename`, `stored_name`, `filesize`, `filetype`, `stored_date`)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (ref_id, file_id, md5_hash, owner_id, original_filename, stored_name,
                                      filesize, filetype, time_now))
                    connProxy.commit()
                    return True
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


def sql_merchant_get_files_by_ref(ref_id: str):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM digi_files WHERE `ref_id` = %s  
                      ORDER BY `stored_date` DESC LIMIT 20 """
            cur.execute(sql, (ref_id))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_get_files_by_file_id(file_id: str):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM digi_files WHERE `file_id` = %s LIMIT 1 """
            cur.execute(sql, (file_id))
            result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_unlink_by_file_id(file_id: str):
    global conn, connProxy
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ DELETE FROM digi_files WHERE `file_id` = %s LIMIT 1 """
            cur.execute(sql, (file_id))
            conn.commit()
            try:
                openConnectionProxy()
                with connProxy.cursor() as cur:
                    sql = """ DELETE FROM digi_files WHERE `file_id` = %s LIMIT 1 """
                    cur.execute(sql, (file_id))
                    connProxy.commit()
                    return True
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_add_preview(ref_id: str, file_id: str, md5_hash: str, owner_id: str, original_filename:str, 
    stored_name: str, filesize: float, filetype: str):
    global conn, connProxy
    time_now = int(time.time())
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ INSERT INTO `digi_preview` (`ref_id`, `file_id`, `md5_hash`, `owner_id`, 
                      `original_filename`, `stored_name`, `filesize`, `filetype`, `stored_date`)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
            cur.execute(sql, (ref_id, file_id, md5_hash, owner_id, original_filename, stored_name,
                              filesize, filetype, time_now))
            conn.commit()
            try:
                openConnectionProxy()
                with connProxy.cursor() as cur:
                    sql = """ INSERT INTO `digi_preview` (`ref_id`, `file_id`, `md5_hash`, `owner_id`, 
                              `original_filename`, `stored_name`, `filesize`, `filetype`, `stored_date`)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) """
                    cur.execute(sql, (ref_id, file_id, md5_hash, owner_id, original_filename, stored_name,
                                      filesize, filetype, time_now))
                    connProxy.commit()
                    return True
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return False


def sql_merchant_get_preview_by_ref(ref_id: str):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM digi_preview WHERE `ref_id` = %s  
                      ORDER BY `stored_date` DESC LIMIT 20 """
            cur.execute(sql, (ref_id))
            result = cur.fetchall()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_get_preview_files_by_file_id(file_id: str):
    global conn
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ SELECT * FROM digi_preview WHERE `file_id` = %s LIMIT 1 """
            cur.execute(sql, (file_id))
            result = cur.fetchone()
            return result
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None


def sql_merchant_unlink_preview_by_file_id(file_id: str):
    global conn, connProxy
    try:
        openConnection()
        with conn.cursor() as cur:
            sql = """ DELETE FROM digi_preview WHERE `file_id` = %s LIMIT 1 """
            cur.execute(sql, (file_id))
            conn.commit()
            try:
                openConnectionProxy()
                with connProxy.cursor() as cur:
                    sql = """ DELETE FROM digi_preview WHERE `file_id` = %s LIMIT 1 """
                    cur.execute(sql, (file_id))
                    connProxy.commit()
                    return True
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    return None