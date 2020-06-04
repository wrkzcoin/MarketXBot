import logging

from aiogram import Bot, types
from aiogram.utils import exceptions, executor
from aiogram.utils.emoji import emojize
from aiogram.dispatcher import Dispatcher
from aiogram.types.message import ContentType
from aiogram.utils.markdown import text, bold, italic, code, pre
from aiogram.types import ParseMode, InputMediaPhoto, InputMediaVideo, ChatActions
from config import config
from wallet import *
import store, daemonrpc_client, addressvalidation, walletapi
import sys, traceback
# redis
import redis
import math, random
# ascii table
from terminaltables import AsciiTable

# webhook
from discord_webhook import DiscordWebhook

import redis, json
import uuid

import store, addressvalidation, walletapi
from wallet import *

from generic_xmr.address_msr import address_msr as address_msr
from generic_xmr.address_xmr import address_xmr as address_xmr
from generic_xmr.address_upx import address_upx as address_upx
from generic_xmr.address_xam import address_xam as address_xam


from aiogram.types import InlineQuery, \
    InputTextMessageContent, InlineQueryResultArticle

logging.basicConfig(format=u'%(filename)s [ LINE:%(lineno)+3s ]#%(levelname)+8s [%(asctime)s]  %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Coin using wallet-api
ENABLE_COIN = config.Enable_Coin.split(",")
WITHDRAW_IN_PROCESS = []
MIN_RATIO = float(config.Min_Ratio)
TRADE_PERCENT = config.Trade_Margin

redis_pool = None
redis_conn = None
redis_expired = 120

API_TOKEN = config.telegram.token

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

def init():
    global redis_pool
    print("PID %d: initializing redis pool..." % os.getpid())
    redis_pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True, db=8)


def openRedis():
    global redis_pool, redis_conn
    if redis_conn is None:
        try:
            redis_conn = redis.Redis(connection_pool=redis_pool)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)


@dp.message_handler(commands='start')
async def start_cmd_handler(message: types.Message):
    keyboard_markup = types.ReplyKeyboardMarkup(row_width=2)
    # default row_width is 3, so here we can omit it actually
    # kept for clearness

    btns_text = ('/bal', '/send')
    keyboard_markup.row(*(types.KeyboardButton(text) for text in btns_text))
    # adds buttons as a new row to the existing keyboard
    # the behaviour doesn't depend on row_width attribute

    more_btns_text = (
        "/info",
        "/coin",
        "/about",
    )
    keyboard_markup.add(*(types.KeyboardButton(text) for text in more_btns_text))
    # adds buttons. New rows are formed according to row_width parameter

    await message.reply("Hello, Welcome to MarketXBot by WrkzCoin team!", reply_markup=keyboard_markup)


@dp.message_handler(commands='info')
async def start_cmd_handler(message: types.Message):
    content = ' '.join(message.text.split())
    args = content.split(" ")
    if message.chat.type != "private":
        return
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return
    if len(args) == 1:
        keyboard_markup = types.ReplyKeyboardMarkup(row_width=2)
        # default row_width is 3, so here we can omit it actually
        # kept for clearness

        btns_text = tuple(["/info " + item for item in ENABLE_COIN])

        more_btns_text = (
            "/start",
        )
        keyboard_markup.add(*(types.KeyboardButton(text) for text in more_btns_text))
        keyboard_markup.row(*(types.KeyboardButton(text) for text in btns_text))

        await message.reply("Select coin to display information", reply_markup=keyboard_markup)
    else:
        # /info WRKZ
        COIN_NAME = args[1].upper()
        if COIN_NAME not in ENABLE_COIN:
            message_text = text(bold("Invalid command /info"))
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        else:
            if not is_coin_depositable(COIN_NAME):
                message_text = text(bold(f"DEPOSITING is currently disable for {COIN_NAME}."))
                await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                    parse_mode=ParseMode.MARKDOWN)
                return

            user_addr = await store.sql_get_userwallet(message.from_user.username, COIN_NAME, 'TELEGRAM')
            if user_addr is None:
                userregister = await store.sql_register_user(message.from_user.username, COIN_NAME, 'TELEGRAM', message.chat.id)
                user_addr = await store.sql_get_userwallet(message.from_user.username, COIN_NAME, 'TELEGRAM')

            message_text = text(bold(f"{COIN_NAME} INFO:\n\n"),
                                "Deposit: ", code(user_addr['balance_wallet_address']), "\n")
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return


@dp.message_handler(commands='coin')
async def start_cmd_handler(message: types.Message):
    content = ' '.join(message.text.split())
    args = content.split(" ")

    if len(args) == 1:
        keyboard_markup = types.ReplyKeyboardMarkup(row_width=2)
        # default row_width is 3, so here we can omit it actually
        # kept for clearness

        btns_text = tuple(["/coin " + item for item in ENABLE_COIN])
        more_btns_text = (
            "/start",
        )
        keyboard_markup.add(*(types.KeyboardButton(text) for text in more_btns_text))
        keyboard_markup.row(*(types.KeyboardButton(text) for text in btns_text))

        await message.reply("Select coin to display information", reply_markup=keyboard_markup)
    else:
        # /coin WRKZ
        COIN_NAME = args[1].upper()
        if COIN_NAME not in ENABLE_COIN:
            message_text = text(bold("Invalid command /coin"))
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        else:
            response_text = ""
            try:
                openRedis()
                if redis_conn and redis_conn.exists(f'TIPBOT:DAEMON_HEIGHT_{COIN_NAME}'):
                    height = int(redis_conn.get(f'TIPBOT:DAEMON_HEIGHT_{COIN_NAME}'))
                    response_text = "\nHeight: {:,.0f}".format(height) + "\n"
                response_text += "Confirmation: {} Blocks".format(get_confirm_depth(COIN_NAME)) + "\n"
                if is_tradeable_coin(COIN_NAME): 
                    response_text += "Trade: ON\n"
                else:
                    response_text += "Trade: OFF\n"
                if is_depositable_coin(COIN_NAME): 
                    response_text += "Deposit: ON\n"
                else:
                    response_text += "Deposit: OFF\n"
                if is_withdrawable_coin(COIN_NAME): 
                    response_text += "Withdraw: ON\n"
                else:
                    response_text += "Withdraw: OFF\n"
                get_tip_min_max = "Sell Min/Max:\n   " + num_format_coin(get_min_sell(COIN_NAME), COIN_NAME) + " / " + num_format_coin(get_max_sell(COIN_NAME), COIN_NAME) + COIN_NAME
                response_text += get_tip_min_max + "\n"
                get_tx_min_max = "Withdraw Min/Max:\n   " + num_format_coin(get_min_tx_amount(COIN_NAME), COIN_NAME) + " / " + num_format_coin(get_max_tx_amount(COIN_NAME), COIN_NAME) + COIN_NAME
                response_text += get_tx_min_max
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            message_text = text(bold("[ COIN INFO {} ]".format(COIN_NAME)), "\n", code(response_text))
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return


@dp.message_handler(commands='bal')
async def start_cmd_handler(message: types.Message):
    content = ' '.join(message.text.split())
    args = content.split(" ")
    if message.chat.type != "private":
        reply_text = "Please do via private message."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return
    if len(args) == 1:
        keyboard_markup = types.ReplyKeyboardMarkup(row_width=2)
        # default row_width is 3, so here we can omit it actually
        # kept for clearness

        btns_text = tuple(["/bal " + item for item in ENABLE_COIN + ["list"]])

        more_btns_text = (
            "/start",
        )
        keyboard_markup.add(*(types.KeyboardButton(text) for text in more_btns_text))
        keyboard_markup.row(*(types.KeyboardButton(text) for text in btns_text))

        await message.reply("Select coin to display information or use /bal list", reply_markup=keyboard_markup)
    else:
        # /bal WRKZ
        COIN_NAME = args[1].upper()
        if COIN_NAME == "LIST":
            table_data = [
                ['TICKER', 'Available', 'Open Order']
                ]
            message_text = ""
            coin_str = "\n"
            for COIN_ITEM in [coinItem.upper() for coinItem in ENABLE_COIN]:
                if not is_maintenance_coin(COIN_ITEM):
                    COIN_DEC = get_decimal(COIN_ITEM)
                    wallet = await store.sql_get_userwallet(message.from_user.username, COIN_ITEM, 'TELEGRAM')
                    if wallet is None:
                        userregister = await store.sql_register_user(message.from_user.username, COIN_ITEM, 'TELEGRAM', message.chat.id)
                        wallet = await store.sql_get_userwallet(message.from_user.username, COIN_ITEM, 'TELEGRAM')
                    userdata_balance = store.sql_user_balance(message.from_user.username, COIN_ITEM, 'TELEGRAM')
                    wallet['actual_balance'] = wallet['actual_balance'] + float(userdata_balance['Adjust'])
                    wallet['actual_balance'] = int(wallet['actual_balance']) if COIN_ITEM not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else wallet['actual_balance']
                    balance_actual = num_format_coin(wallet['actual_balance'], COIN_ITEM)
                    table_data.append([COIN_ITEM, balance_actual, num_format_coin(userdata_balance['OpenOrder'], COIN_ITEM)])
                else:
                    table_data.append([COIN_ITEM, "***", "***"])
            table = AsciiTable(table_data)
            table.inner_row_border = False
            table.padding_left = 0
            table.padding_right = 0
            message_text = text(bold(f'[YOUR BALANCE SHEET]:\n'),
                                code(table.table))
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        elif COIN_NAME not in ENABLE_COIN:
            message_text = text(bold(f"Invalid coin /bal {COIN_NAME}"))
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        else:    
            # get balance user for a specific coin
            coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
            userwallet = await store.sql_get_userwallet(message.from_user.username, COIN_NAME, 'TELEGRAM')

            if userwallet is None:
                userwallet = await store.sql_register_user(message.from_user.username, COIN_NAME, 'TELEGRAM', message.chat.id)
                userwallet = await store.sql_get_userwallet(message.from_user.username, COIN_NAME, 'TELEGRAM')

            userdata_balance = store.sql_user_balance(message.from_user.username, COIN_NAME, 'TELEGRAM')
            userwallet['actual_balance'] = userwallet['actual_balance'] + float(userdata_balance['Adjust'])

            message_text = text(bold(f'[YOUR {COIN_NAME} BALANCE]:\n'),
                                "Available: ", code(num_format_coin(userwallet['actual_balance'], COIN_NAME) + COIN_NAME + "\n"),
                                "Opened Order: ", code(num_format_coin(userdata_balance['OpenOrder'], COIN_NAME) + COIN_NAME + "\n"),)
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return


@dp.message_handler(commands='send')
async def start_cmd_handler(message: types.Message):
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    content = ' '.join(message.text.split())
    args = content.split(" ")
    if len(args) != 4:
        reply_text = "Please use /send amount coin_name your_address"
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return
   
    COIN_NAME = args[2].upper()
    if COIN_NAME not in ENABLE_COIN:
        message_text = text(bold(f"Invalid {COIN_NAME}\n\n"), 
                            "Supported coins: ", code(", ".join(ENABLE_COIN)))
        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                            parse_mode=ParseMode.MARKDOWN)
        return

    if not is_coin_txable(COIN_NAME):
        message_text = text(bold(f"TX is currently disable for {COIN_NAME}."))
        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                            parse_mode=ParseMode.MARKDOWN)
        return

    amount = args[1].replace(",", "")
    try:
        amount = float(amount)
    except ValueError:
        message_text = text(bold("Invalid amount."))
        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                            parse_mode=ParseMode.MARKDOWN)
        return

    # add redis action
    random_string = str(uuid.uuid4())
    await add_tx_action_redis(json.dumps([random_string, "SEND", message.from_user.username, message.from_user.username, float("%.3f" % time.time()), message.text, "TELEGRAM", "START"]), False)

    wallet_address = args[3]
    if wallet_address.isalnum() == False:
        message_text = text(bold("Invalid address:\n"),
                            code(wallet_address))
        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                            parse_mode=ParseMode.MARKDOWN)
        return
    else:
        COIN_NAME_CHECK = await get_cn_coin_from_address(wallet_address)
        if not COIN_NAME_CHECK:
            message_text = text(bold("Unknown coin name:\n"),
                                code(wallet_address))
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        elif COIN_NAME_CHECK != COIN_NAME:
            message_text = text(bold("Error getting address and coin name from:\n"),
                                code(wallet_address))
            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        # get coin family
        coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
        if coin_family == "TRTL" or coin_family == "DOGE":
            addressLength = get_addrlen(COIN_NAME)
            IntaddressLength = 0
            paymentid = None
            CoinAddress = None

            user_from = await store.sql_get_userwallet(message.from_user.username, COIN_NAME, 'TELEGRAM')
            if user_from is None:
                userregister = await store.sql_register_user(message.from_user.username, COIN_ITEM, 'TELEGRAM', message.chat.id)
                user_from = await store.sql_get_userwallet(message.from_user.username, COIN_ITEM, 'TELEGRAM')
            userdata_balance = store.sql_user_balance(message.from_user.username, COIN_NAME, 'TELEGRAM')
            user_from['actual_balance'] = user_from['actual_balance'] + float(userdata_balance['Adjust'])

            COIN_DEC = get_decimal(COIN_NAME)
            real_amount = int(amount * COIN_DEC) if (coin_family == "TRTL" or coin_family == "XMR") else amount
            MinTx = get_min_tx_amount(COIN_NAME)
            MaxTX = get_max_tx_amount(COIN_NAME)
            NetFee = get_reserved_fee(coin = COIN_NAME)
            message_text = ''
            valid_amount = True
            if real_amount + NetFee > user_from['actual_balance']:
                message_text = 'Not enough reserved fee / Insufficient balance to send ' + num_format_coin(real_amount, COIN_NAME) + COIN_NAME + ' to ' + wallet_address
                valid_amount = False
            elif real_amount > MaxTX:
                message_text = 'Transactions cannot be bigger than ' + num_format_coin(MaxTX, COIN_NAME) + COIN_NAME
                valid_amount = False
            elif real_amount < MinTx:
                message_text = 'Transactions cannot be bigger than ' + num_format_coin(MinTx, COIN_NAME) + COIN_NAME
                valid_amount = False
            if valid_amount == False:
                await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                    parse_mode=ParseMode.MARKDOWN)
                return

            if coin_family == "TRTL" or coin_family == "XMR":
                CoinAddress = None
                IntaddressLength = get_intaddrlen(COIN_NAME)
                if len(wallet_address) == int(addressLength):
                    valid_address = addressvalidation.validate_address_cn(wallet_address, COIN_NAME)
                    if valid_address is None:
                        message_text = text(bold("Invalid address:\n"),
                                            code(wallet_address))
                        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                            parse_mode=ParseMode.MARKDOWN)
                        return
                    else:
                        CoinAddress = wallet_address
                elif len(wallet_address) == int(IntaddressLength): 
                    # use integrated address
                    valid_address = addressvalidation.validate_integrated_cn(wallet_address, COIN_NAME)
                    if valid_address == 'invalid':
                        message_text = text(bold("Invalid address:\n"),
                                            code(wallet_address))
                        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                            parse_mode=ParseMode.MARKDOWN)
                        return
                    elif len(valid_address) == 2:
                        address_paymentID = wallet_address
                        CoinAddress = valid_address['address']
                        paymentid = valid_address['integrated_id']

                main_address = getattr(getattr(config,"coin"+COIN_NAME),"MainAddress")
                if CoinAddress and CoinAddress == main_address:
                    # Not allow to send to own main address
                    message_text = text(bold("Can not send to:\n"),
                                        code(wallet_address))
                    await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                        parse_mode=ParseMode.MARKDOWN)
                    return
                else:
                    sending = None
                    if message.from_user.username not in WITHDRAW_IN_PROCESS:
                        WITHDRAW_IN_PROCESS.append(message.from_user.username)
                    else:
                        message_text = text(bold("You have another tx in progress.\n"))
                        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                            parse_mode=ParseMode.MARKDOWN)
                        return

                    try:
                        if paymentid:
                            sending = await store.sql_external_cn_xmr_single('TELEGRAM', message.from_user.username, real_amount, CoinAddress, COIN_NAME, paymentid)
                        else:
                            sending = await store.sql_external_cn_xmr_single('TELEGRAM', message.from_user.username, real_amount, CoinAddress, COIN_NAME)
                        await add_tx_action_redis(json.dumps([random_string, "SEND", message.from_user.username, message.from_user.username, float("%.3f" % time.time()), message.text, "TELEGRAM", "COMPLETE"]), False)
                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)

                    if message.from_user.username in WITHDRAW_IN_PROCESS:
                        WITHDRAW_IN_PROCESS.remove(message.from_user.username)

                    if sending:
                        tip_tx_tipper = "Transaction hash: {}".format(sending['transactionHash'])
                        tip_tx_tipper += "\nTx Fee: {}{}".format(num_format_coin(sending['fee'], COIN_NAME), COIN_NAME)
                        
                        message_text = text(bold(f"You have sent {num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}:\n"),
                                            code(tip_tx_tipper))
                        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                            parse_mode=ParseMode.MARKDOWN)
                        return
                    else:
                        message_text = text(bold(f"Internal error for sending {num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}"))
                        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                            parse_mode=ParseMode.MARKDOWN)
                        return
            elif coin_family == "DOGE":
                valid_address = await doge_validaddress(str(wallet_address), COIN_NAME)
                if 'isvalid' in valid_address:
                    if str(valid_address['isvalid']) != "True":
                        message_text = text(bold("Invalid address:\n"),
                                            code(wallet_address))
                        await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                            parse_mode=ParseMode.MARKDOWN)
                        return
                    else:
                        sendTx = None
                        if message.from_user.username not in WITHDRAW_IN_PROCESS:
                            WITHDRAW_IN_PROCESS.append(message.from_user.username)
                        else:
                            message_text = text(bold("You have another tx in progress.\n"))
                            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                                parse_mode=ParseMode.MARKDOWN)
                            return

                        try:
                            NetFee = get_tx_fee(coin = COIN_NAME)
                            sendTx = await store.sql_external_doge('TELEGRAM', message.from_user.username, real_amount, NetFee,
                                                                CoinAddress, COIN_NAME)
                            await add_tx_action_redis(json.dumps([random_string, "SEND", message.from_user.username, message.from_user.username, float("%.3f" % time.time()), message.text, "TELEGRAM", "COMPLETE"]), False)
                        except Exception as e:
                            traceback.print_exc(file=sys.stdout)

                        if message.from_user.username in WITHDRAW_IN_PROCESS:
                            WITHDRAW_IN_PROCESS.remove(message.from_user.username)

                        if sendTx:
                            tx_text = "Transaction hash: {}".format(sendTx)
                            tx_text += "\nNetwork fee deducted from the amount."
                            
                            message_text = text(bold(f"You have sent {num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}:\n"),
                                                code(tx_text))
                            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                                parse_mode=ParseMode.MARKDOWN)
                            return
                        else:
                            message_text = text(bold(f"Internal error for sending {num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}"))
                            await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                                parse_mode=ParseMode.MARKDOWN)
                            return


@dp.message_handler(commands='market')
async def start_cmd_handler(message: types.Message):
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    content = ' '.join(message.text.split())
    args = content.split(" ")
    if len(args) == 1:
        get_markets = await store.sql_get_open_order_by_alluser('ALL', 'OPEN')
        msg = None
        if get_markets and len(get_markets) > 0:
            table_data = [
                ['Selling / For', 'Rate', 'Order #']
                ]
            list_numb = 0
            for order_item in get_markets:
                list_numb += 1
                if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                    table_data.append([num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'] + "\nFor "+ 
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                else:
                    table_data.append([num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'] + "\nFor " +
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                if list_numb > 20:
                    break
            table = AsciiTable(table_data)
            #table.inner_heading_row_border = False
            #table.inner_column_border = True
            #table.outer_border = False
            table.inner_row_border = True
            table.padding_left = 0
            table.padding_right = 0
            msg = text(bold("[ MARKET LIST ]\n"), pre("\n" + table.table))
        else:
            msg = text(bold("[ MARKET LIST ]\n"), code("Currently, no opening selling market. Please make some open order for others."))
        reply_text = msg if msg else "Currently, no opening selling market. Please make some open order for others."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove(), parse_mode=ParseMode.MARKDOWN)
        return
    elif len(args) == 2:
        # check if there is / or -
        coin_pair = None
        COIN_NAME = None
        get_markets = None
        coin = args[1].upper()
        if "/" in coin:
            coin_pair = coin.split("/")
        elif "." in coin:
            coin_pair = coin.split(".")
        elif "-" in coin:
            coin_pair = coin.split(".")
        if coin_pair is None:
            COIN_NAME = coin.upper()
            if COIN_NAME not in ENABLE_COIN:
                message_text = text(bold(f"Invalid {COIN_NAME}\n\n"), 
                                    "Supported coins: ", code(", ".join(ENABLE_COIN)))
                await message.reply(message_text, reply_markup=types.ReplyKeyboardRemove(),
                                    parse_mode=ParseMode.MARKDOWN)
                return
            else:
                get_markets = await store.sql_get_open_order_by_alluser(COIN_NAME, 'OPEN')
        elif coin_pair and len(coin_pair) == 2:
            if coin_pair[0] not in ENABLE_COIN:
                msg = text(bold(f"{coin_pair[0]}"), " is not in our list.")
                await message.reply(msg, reply_markup=types.ReplyKeyboardRemove())
                return
            elif coin_pair[1] not in ENABLE_COIN:
                msg = text(bold(f"{coin_pair[1]}"), " is not in our list.")
                await message.reply(msg, reply_markup=types.ReplyKeyboardRemove())
                return
            else:
                get_markets = await store.sql_get_open_order_by_alluser_by_coins(coin_pair[0], coin_pair[1], 'OPEN')
        if get_markets and len(get_markets) > 0:
            list_numb = 0
            table_data = [
                ['Selling / For', 'Rate', 'Order #']
                ]
            for order_item in get_markets:
                list_numb += 1
                if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                    table_data.append([num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'] + "\nFor " +
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                else:
                    table_data.append([num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'] + "\nFor " +
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                if list_numb > 20:
                    break
            table = AsciiTable(table_data)
            table.inner_row_border = True
            table.padding_left = 0
            table.padding_right = 0
            if coin_pair:
                title = "MARKET **{}/{}**".format(coin_pair[0], coin_pair[1])
            else:
                title = "MARKET **{}**".format(COIN_NAME)
            msg = text(bold(f"[ {title} ]\n"), pre("\n" + table.table))
            await message.reply(msg, reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        else:
            reply_text = "Currently, no opening selling market. Please make some open order for others."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return
    else:
        reply_text = "Please use /market coin_name or /market"
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return


@dp.message_handler(commands='myorder')
async def start_cmd_handler(message: types.Message):
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    content = ' '.join(message.text.split())
    args = content.split(" ")
    ticker = args[1].upper() if len(args) == 2 else False
    if ticker:
        if len(ticker) < 6:
            # assume it is a coin
            COIN_NAME = ticker
            if COIN_NAME not in ENABLE_COIN:
                reply_text = f"Invalid ticker {COIN_NAME}."
                await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
                return
            else:
                get_open_order = await store.sql_get_open_order_by_sellerid(message.from_user.username, COIN_NAME, 'OPEN')
                msg = None
                if get_open_order and len(get_open_order) > 0:
                    table_data = [
                        ['Selling / For', 'Order #']
                        ]
                    for order_item in get_open_order:
                        if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                            table_data.append([num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'] + " For "+
                                              num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                              order_item['order_id']])
                        else:
                            table_data.append([num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'] + " For " +
                                              num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                              order_item['order_id']])
                    table = AsciiTable(table_data)
                    table.inner_row_border = True
                    table.padding_left = 0
                    table.padding_right = 0
                    msg = text(bold("[ YOUR MARKET LIST ]\n"), pre("\n" + table.table))                    
                else:
                    msg = text(bold("[ MARKET LIST ]\n"), code("Currently, no opening selling market. Please make some open order for others."))
                await message.reply(msg, reply_markup=types.ReplyKeyboardRemove(), parse_mode=ParseMode.MARKDOWN)
                return
        else:
            reply_text = f"Invalid ticker {ticker}."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return
    else:
        get_open_order = await store.sql_get_open_order_by_sellerid_all(message.from_user.username, 'OPEN')
        msg = None
        if get_open_order and len(get_open_order) > 0:
            table_data = [
                ['Selling / For', 'Order #']
                ]
            for order_item in get_open_order:
                if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                    table_data.append([num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'] + " For "+ 
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], order_item['order_id']])
                else:
                    table_data.append([num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'] + " For "+
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], order_item['order_id']])
            table = AsciiTable(table_data)
            table.inner_row_border = True
            table.padding_left = 0
            table.padding_right = 0
            msg = text(bold("[ YOUR MARKET LIST ]\n"), pre("\n" + table.table)) 
        else:
            msg = text(bold("[ MARKET LIST ]\n"), code("You do not have any active selling."))
        await message.reply(msg, reply_markup=types.ReplyKeyboardRemove(), parse_mode=ParseMode.MARKDOWN)
        return


@dp.message_handler(commands='order')
async def start_cmd_handler(message: types.Message):
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    content = ' '.join(message.text.split())
    args = content.split(" ")
    if len(args) != 2:
        reply_text = "Please use /order ref_number"
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return
    else:
        # /order XXXXX
        # assume this is reference number
        try:
            ref_number = int(args[1])
            ref_number = str(ref_number)
        except ValueError:
            reply_text = f"Invalid # number. Given {args[1]}"
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return

        get_order_num = await store.sql_get_order_numb(ref_number, 'ANY')
        if get_order_num:
            # check if own order
            response_text = "Order #: " + ref_number + "\n"
            response_text += "Sell (After Fee): " + num_format_coin(get_order_num['amount_sell_after_fee'], get_order_num['coin_sell'])+get_order_num['coin_sell'] + "\n"
            response_text += "For (After Fee): " + num_format_coin(get_order_num['amount_get_after_fee'], get_order_num['coin_get'])+get_order_num['coin_get'] + "\n"
            if get_order_num['status'] == "COMPLETE":
                response_text = response_text.replace("Sell", "Sold")
                response_text += "Status: COMPLETED"
            elif get_order_num['status'] == "OPEN":
                response_text += "Status: OPENED"
            elif get_order_num['status'] == "CANCEL":
                response_text += "Status: CANCELLED"

            if get_order_num['sell_user_server'] == "TELEGRAM" and message.from_user.username == get_order_num['userid_sell']:
                # if he is the seller
                response_text = response_text.replace("Sell", "You sell")
                response_text = response_text.replace("Sold", "You sold")
            if get_order_num['buy_user_server'] and get_order_num['buy_user_server'] == "TELEGRAM" \
            and 'userid_get' in get_order_num and (message.from_user.username == get_order_num['userid_get'] if get_order_num['userid_get'] else 0):
                # if he bought this
                response_text = response_text.replace("Sold", "You bought: ")
                response_text = response_text.replace("For (After Fee):", "From selling (After Fee): ")
            await message.reply(text(code(response_text)), reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
            return
        else:
            reply_text = f"I could not find #{ref_number}."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return


@dp.message_handler(commands='sell')
async def start_cmd_handler(message: types.Message):
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    content = ' '.join(message.text.split())
    args = content.split(" ")

    if len(args) != 5:
        reply_text = "Please use /sell sell_amount sell_ticker buy_amount buy_ticker"
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    sell_ticker = args[2].upper()
    buy_ticker = args[4].upper()
    sell_amount = args[1].replace(",", "")
    buy_amount = args[3].replace(",", "")
    try:
        sell_amount = float(sell_amount)
        buy_amount = float(buy_amount)
    except ValueError:
        reply_text = "Invalid sell/buy amount."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if (sell_ticker not in ENABLE_COIN) or (buy_ticker not in ENABLE_COIN):
        reply_text = "Invalid trade ticker (buy/sell)."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if not is_tradeable_coin(sell_ticker):
        reply_text = f"{sell_ticker} trading is currently disable."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if not is_tradeable_coin(buy_ticker):
        reply_text = f"{buy_ticker} trading is currently disable."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if buy_ticker == sell_ticker:
        reply_text = f"{buy_ticker} you cannot trade the same coins."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    # get opened order:
    user_count_order = await store.sql_count_open_order_by_sellerid(message.from_user.username, 'TELEGRAM')
    if user_count_order >= config.Max_Open_Order:
        reply_text = f"You have maximum opened selling {config.Max_Open_Order}. Please cancel some or wait."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return
    
    COIN_DEC_SELL = get_decimal(sell_ticker)
    real_amount_sell = int(sell_amount * COIN_DEC_SELL) if sell_ticker not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else sell_amount
    if real_amount_sell == 0:
        reply_text = f"{sell_amount}{sell_ticker} = 0 {sell_ticker} (below smallest unit)."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if real_amount_sell < get_min_sell(sell_ticker):
        reply_text = f"{sell_amount}{sell_ticker} below minimum trade {num_format_coin(get_min_sell(sell_ticker), sell_ticker)}{sell_ticker}."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if real_amount_sell > get_max_sell(sell_ticker):
        reply_text = f"{sell_amount}{sell_ticker} above maximum trade {num_format_coin(get_max_sell(sell_ticker), sell_ticker)}{sell_ticker}."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    COIN_DEC_BUY = get_decimal(buy_ticker)
    real_amount_buy = int(buy_amount * COIN_DEC_BUY) if buy_ticker not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else buy_amount
    if real_amount_buy == 0:
        reply_text = f"{buy_amount}{buy_ticker} = 0 {buy_ticker} (below smallest unit)."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if real_amount_buy < get_min_sell(buy_ticker):
        reply_text = f"{buy_amount}{buy_ticker} below minimum trade {num_format_coin(get_min_sell(buy_ticker), buy_ticker)}{buy_ticker}."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if real_amount_buy > get_max_sell(buy_ticker):
        reply_text = f"{buy_amount}{buy_ticker} above maximum trade {num_format_coin(get_max_sell(buy_ticker), buy_ticker)}{buy_ticker}."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    if not is_maintenance_coin(sell_ticker):
        wallet = await store.sql_get_userwallet(message.from_user.username, sell_ticker, 'TELEGRAM')
        if wallet is None:
            userregister = await store.sql_register_user(message.from_user.username, sell_ticker, 'TELEGRAM', message.chat.id)
            wallet = await store.sql_get_userwallet(message.from_user.username, sell_ticker, 'TELEGRAM')

        userdata_balance = store.sql_user_balance(message.from_user.username, sell_ticker, 'TELEGRAM')
        wallet['actual_balance'] = wallet['actual_balance'] + float(userdata_balance['Adjust'])
        wallet['actual_balance'] = int(wallet['actual_balance']) if sell_ticker not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else wallet['actual_balance']
        if wallet['actual_balance'] < real_amount_sell:
            reply_text = "You do not have enough " \
                        f"{sell_ticker}.\nYou have currently: {num_format_coin(wallet['actual_balance'], sell_ticker)}{sell_ticker}."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return

        if (sell_amount / buy_amount) < MIN_RATIO or (buy_amount / sell_amount) < MIN_RATIO:
            reply_text = "ratio buy/sell rate is so low."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return

        # call other function
        return await sell_process(message, real_amount_sell, sell_ticker, real_amount_buy, buy_ticker)


async def sell_process(message, real_amount_sell: float, sell_ticker: str, real_amount_buy: float, buy_ticker: str):
    global ENABLE_COIN, NOTIFY_CHAN
    sell_ticker = sell_ticker.upper()
    buy_ticker = buy_ticker.upper()
    real_amount_sell = round(real_amount_sell, 8)
    real_amount_buy = round(real_amount_buy, 8)
    sell_div_get = round(real_amount_sell / real_amount_buy, 16)
    fee_sell = round(TRADE_PERCENT * real_amount_sell, 8)
    fee_buy = round(TRADE_PERCENT * real_amount_buy, 8)
    if fee_sell == 0: fee_sell = 0.00000100
    if fee_buy == 0: fee_buy = 0.00000100
    # Check if user already have another open order with the same rate
    # Check if user make a sell process of his buy coin which already in open order
    check_if_same_rate = await store.sql_get_order_by_sellerid_pair_rate('TELEGRAM', message.from_user.username, sell_ticker, 
                         buy_ticker, sell_div_get, real_amount_sell, real_amount_buy, fee_sell, fee_buy, 'OPEN')
    if check_if_same_rate and check_if_same_rate['error'] == True and check_if_same_rate['msg']:
        get_message = check_if_same_rate['msg']
        await message.reply(get_message, reply_markup=types.ReplyKeyboardRemove())
        return
    elif check_if_same_rate and check_if_same_rate['error'] == False:
        get_message = check_if_same_rate['msg']
        await message.reply(get_message, reply_markup=types.ReplyKeyboardRemove())
        return

    order_add = await store.sql_store_openorder(message.message_id, (message.text)[:120], sell_ticker, 
                            real_amount_sell, real_amount_sell-fee_sell, message.from_user.username, 
                            buy_ticker, real_amount_buy, real_amount_buy-fee_buy, sell_div_get, 'TELEGRAM')
    if order_add:        
        get_message = text("New open order created: #" + str(order_add),
                           "\nSelling: {}{}\nFor: {}{}\nFee: {}{}".format( 
                           num_format_coin(real_amount_sell, sell_ticker), sell_ticker,
                           num_format_coin(real_amount_buy, buy_ticker), buy_ticker,
                           num_format_coin(fee_sell, sell_ticker), sell_ticker))
        try:
            post_text = "**[Telegram]**:" + "```" + get_message + "```"
            webhook = DiscordWebhook(url=config.telegram.discordwebhook, content=post_text)
            webhook.execute()
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        try:
            await message.reply(text(code("\n" + get_message)), reply_markup=types.ReplyKeyboardRemove(),
                                parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        return


@dp.message_handler(commands='buy')
async def start_cmd_handler(message: types.Message):
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    content = ' '.join(message.text.split())
    args = content.split(" ")

    if len(args) != 2:
        reply_text = "Please use /buy ref_number"
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    # check if the argument is ref or ticker by length
    ref_number = args[1]
    if len(ref_number) < 6:
        # assume it is ticker
        # ,buy trtl (example)
        COIN_NAME = ref_number.upper()
        if COIN_NAME not in ENABLE_COIN:
            reply_text = "Please use /buy ref_number"
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return
        # TODO: add /buy coin_name
    else:
        # assume reference number
        get_order_num = await store.sql_get_order_numb(ref_number)
        if get_order_num:
            # check if own order
            if message.from_user.username == get_order_num['userid_sell']:
                reply_text = text("#", bold(ref_number), " is your own selling order.")
                await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove(),
                                    parse_mode=ParseMode.MARKDOWN)
                return
            else:
                # check if sufficient balance
                wallet = await store.sql_get_userwallet(message.from_user.username, get_order_num['coin_get'], 'TELEGRAM')
                if wallet is None:
                    userregister = await store.sql_register_user(message.from_user.username, get_order_num['coin_get'], 'TELEGRAM')
                    wallet = await store.sql_get_userwallet(message.from_user.username, get_order_num['coin_get'], 'TELEGRAM')
                userdata_balance = store.sql_user_balance(message.from_user.username, get_order_num['coin_get'], 'TELEGRAM')
                wallet['actual_balance'] = wallet['actual_balance'] + float(userdata_balance['Adjust'])
                if wallet['actual_balance'] < get_order_num['amount_get_after_fee']:
                    reply_text = text(bold("You do not have sufficient balance."),
                                        code("\nNeeded: {}{}\nHave:   {}{}".format(
                                            num_format_coin(get_order_num['amount_get'], 
                                            get_order_num['coin_get']), get_order_num['coin_get'],
                                            num_format_coin(wallet['actual_balance'], get_order_num['coin_get']), 
                                            get_order_num['coin_get']
                                        )))
                    await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove(),
                                        parse_mode=ParseMode.MARKDOWN)
                    return
                else:
                    # let's make order update
                    match_order = await store.sql_match_order_by_sellerid(message.from_user.username, ref_number, 'TELEGRAM')
                    if match_order:
                        reply_text = text(bold(f"#{ref_number}"), " Order completed!\n",
                                        code("\nGet: {}{}\nFrom selling: {}{}\nFee: {}{}\n".format(
                                            num_format_coin(get_order_num['amount_sell_after_fee'], 
                                            get_order_num['coin_sell']), get_order_num['coin_sell'], 
                                            num_format_coin(get_order_num['amount_get_after_fee'], 
                                            get_order_num['coin_get']), get_order_num['coin_get'],
                                            num_format_coin(get_order_num['amount_get']-get_order_num['amount_get_after_fee'], 
                                            get_order_num['coin_get']), get_order_num['coin_get']
                                        )))
                        # reply to telegram
                        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove(),
                                            parse_mode=ParseMode.MARKDOWN)
                        # send to other seller
                        sold = num_format_coin(get_order_num['amount_sell'], get_order_num['coin_sell']) + get_order_num['coin_sell']
                        bought = num_format_coin(get_order_num['amount_get_after_fee'], get_order_num['coin_get']) + get_order_num['coin_get']
                        fee = str(num_format_coin(get_order_num['amount_get']-get_order_num['amount_get_after_fee'], get_order_num['coin_get']))
                        fee += get_order_num['coin_get']
                        selling_text = "A user has bought #" + ref_number + "\n" + f"\nSold: {sold}\nGet: {bought}\nFee: {fee}"
                        # Post via webhook
                        try:
                            post_text = "**[Telegram]**:" + "```" + selling_text + "```"
                            webhook = DiscordWebhook(url=config.telegram.discordwebhook, content=post_text)
                            webhook.execute()
                        except Exception as e:
                            traceback.print_exc(file=sys.stdout)

                        if get_order_num['sell_user_server'] == "TELEGRAM":
                            to_teleuser = await store.sql_get_userwallet(get_order_num['userid_sell'], COIN_NAME, 'TELEGRAM')
                            if to_teleuser is None:
                                print(f"Can not find user {get_order_num['userid_sell']} in TELEGRAM")
                            else:
                                try:
                                    to_user = to_teleuser['chat_id']
                                    send_msg = await bot.send_message(chat_id=to_user, text=text(code("\n" + selling_text)), parse_mode=ParseMode.MARKDOWN)
                                except Exception as e:
                                    traceback.print_exc(file=sys.stdout)
                        return
                    else:
                        reply_text = f"#{ref_number} internal error, please report."
                        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
                        return
        else:
            reply_text = f"#{ref_number} does not exist or already completed."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return


@dp.message_handler(commands='cancel')
async def start_cmd_handler(message: types.Message):
    if message.from_user.username is None:
        reply_text = "I can not get your username."
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    content = ' '.join(message.text.split())
    args = content.split(" ")

    if len(args) != 2:
        reply_text = "Please use /cancel ref_number|all|coin_name"
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
        return

    # check if the argument is ref or ticker by length
    ref_number = args[1]
    if ref_number.upper() == "ALL":
        # cancel all
        get_open_order = await store.sql_get_open_order_by_sellerid_all(message.from_user.username, 'OPEN')
        if len(get_open_order) == 0:
            reply_text = "You do not have any open order."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return
        else:
            cancel_order = await store.sql_cancel_open_order_by_sellerid(message.from_user.username, 'ALL')
            reply_text = "You have cancelled all opened order(s)."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return
    elif len(ref_number) < 6:
        # assume coin name
        COIN_NAME = ref_number.upper()
        if COIN_NAME not in ENABLE_COIN:
            reply_text = f"{COIN_NAME} is not valid."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return
        else:
            get_open_order = await store.sql_get_open_order_by_sellerid(message.from_user.username, COIN_NAME, 'OPEN')
            if len(get_open_order) == 0:
                reply_text = f"You do not have any open order with {COIN_NAME}."
                await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
                return
            else:
                cancel_order = await store.sql_cancel_open_order_by_sellerid(message.from_user.username, COIN_NAME)
                reply_text = f"You have cancelled all opened sell(s) for {COIN_NAME}."
                await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
                return
    else:
        # assume reference
        # open order number
        get_open_order = await store.sql_get_open_order_by_sellerid_all(message.from_user.username, 'OPEN')
        if len(get_open_order) == 0:
            reply_text = "You do not have any open order."
            await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
            return
        else:
            cancelled = False
            for open_order_list in get_open_order:
                if ref_number == str(open_order_list['order_id']):
                    cancel_order = await store.sql_cancel_open_order_by_sellerid(message.from_user.username, ref_number) 
                    if cancel_order: cancelled = True
            if cancelled == False:
                reply_text = f"You do not have sell #{ref_number}."
                await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
                return
            else:
                reply_text = f"You cancelled #{ref_number}."
                await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
                return


@dp.message_handler(commands='about')
async def start_cmd_handler(message: types.Message):
    reply_text = text(bold("Thank you for checking:\n"),
                      code("Twitter dev: https://twitter.com/wrkzdev\n"),
                      code("Discord: https://chat.wrkz.work\n"),
                      code("Telegram: https://t.me/wrkzcoinchat\n"),
                      code("Donation: via /donate amount coin_name\n"),
                      code("Run by WrkzCoin team\n"))
    await message.reply(reply_text, parse_mode=ParseMode.MARKDOWN)
    return


@dp.message_handler()
async def all_msg_handler(message: types.Message):
    # pressing of a KeyboardButton is the same as sending the regular message with the same text
    # so, to handle the responses from the keyboard, we need to use a message_handler
    # in real bot, it's better to define message_handler(text="...") for each button
    # but here for the simplicity only one handler is defined

    button_text = message.text
    logger.debug('The answer is %r', button_text)  # print the text we've got

    reply_command = True
    if button_text.upper() == 'XXXX':
        reply_text = "balance start"
    else:
        reply_text = "Unknown Command!"
        reply_command = False
    if reply_command:
        await message.reply(reply_text, reply_markup=types.ReplyKeyboardRemove())
    # with message, we send types.ReplyKeyboardRemove() to hide the keyboard



async def get_cn_coin_from_address(CoinAddress: str):
    COIN_NAME = None
    if CoinAddress.startswith("Wrkz"):
        COIN_NAME = "WRKZ"
    elif CoinAddress.startswith("dg"):
        COIN_NAME = "DEGO"
    elif CoinAddress.startswith("cat1"):
        COIN_NAME = "CX"
    elif CoinAddress.startswith("btcm"):
        COIN_NAME = "BTCMZ"
    elif CoinAddress.startswith("dicKTiPZ"):
        COIN_NAME = "MTIP"
    elif CoinAddress.startswith("PLe"):
        COIN_NAME = "PLE"
    elif CoinAddress.startswith("Nib1"):
        COIN_NAME = "NBXC"
    elif CoinAddress.startswith("guns"):
        COIN_NAME = "ARMS"
    elif CoinAddress.startswith("ir"):
        COIN_NAME = "IRD"
    elif CoinAddress.startswith("NaCa"):
        COIN_NAME = "NACA"
    elif CoinAddress.startswith("TRTL"):
        COIN_NAME = "TRTL"
    elif CoinAddress.startswith("bit") and (len(CoinAddress) == 98 or len(CoinAddress) == 109):
        COIN_NAME = "XTOR"
    elif (CoinAddress.startswith("4") or CoinAddress.startswith("8") or CoinAddress.startswith("5") or CoinAddress.startswith("9")) \
        and (len(CoinAddress) == 95 or len(CoinAddress) == 106):
        # XMR / MSR
        # 5, 9: MSR
        # 4, 8: XMR
        addr = None
        # Try MSR
        try:
            addr = address_msr(CoinAddress)
            COIN_NAME = "MSR"
            return COIN_NAME
        except Exception as e:
            # traceback.print_exc(file=sys.stdout)
            pass
        # Try UPX
        try:
            addr = address_upx(CoinAddress)
            COIN_NAME = "UPX"
            return COIN_NAME
        except Exception as e:
            # traceback.print_exc(file=sys.stdout)
            pass
        # Try XAM
        try:
            addr = address_xam(CoinAddress)
            COIN_NAME = "XAM"
            return COIN_NAME
        except Exception as e:
            # traceback.print_exc(file=sys.stdout)
            pass
        # Try XMR
        try:
            addr = address_xmr(CoinAddress)
            COIN_NAME = "XMR"
            return COIN_NAME
        except Exception as e:
            # traceback.print_exc(file=sys.stdout)
            pass
    elif (CoinAddress.startswith("amit") and len(CoinAddress) == 98) or (CoinAddress.startswith("aint") and len(CoinAddress) == 109)  or \
        (CoinAddress.startswith("asub") and len(CoinAddress) == 99):
        COIN_NAME = "XAM"
    elif CoinAddress.startswith("L") and (len(CoinAddress) == 95 or len(CoinAddress) == 106):
        COIN_NAME = "LOKI"
    elif CoinAddress.startswith("cms") and (len(CoinAddress) == 98 or len(CoinAddress) == 109):
        COIN_NAME = "BLOG"
    elif (CoinAddress.startswith("ar") or CoinAddress.startswith("aR")) and (len(CoinAddress) == 97 or len(CoinAddress) == 98 or len(CoinAddress) == 109):
        COIN_NAME = "ARQ"
    elif ((CoinAddress.startswith("UPX") and len(CoinAddress) == 98) or (CoinAddress.startswith("UPi") and len(CoinAddress) == 109) or (CoinAddress.startswith("Um") and len(CoinAddress) == 97)):
        COIN_NAME = "UPX"
    elif (CoinAddress.startswith("5") or CoinAddress.startswith("9")) and (len(CoinAddress) == 95 or len(CoinAddress) == 106):
        COIN_NAME = "MSR"
    elif (CoinAddress.startswith("fh") and len(CoinAddress) == 97) or \
    (CoinAddress.startswith("fi") and len(CoinAddress) == 108) or \
    (CoinAddress.startswith("fs") and len(CoinAddress) == 97):
        COIN_NAME = "XWP"
    elif CoinAddress.startswith("D") and len(CoinAddress) == 34:
        COIN_NAME = "DOGE"
    elif (CoinAddress[0] in ["M", "L"]) and len(CoinAddress) == 34:
        COIN_NAME = "LTC"
    elif (CoinAddress[0] in ["3", "1"]) and len(CoinAddress) == 34:
        COIN_NAME = "BTC"
    elif CoinAddress.startswith("bitcoincash") and len(CoinAddress) == 54:
        COIN_NAME = "BCH"
    elif (CoinAddress[0] in ["X"]) and len(CoinAddress) == 34:
        COIN_NAME = "DASH"
    print('get_cn_coin_from_address return {}: {}'.format(CoinAddress, COIN_NAME))
    return COIN_NAME


# Notify user
async def notify_new_tx_user():
    INTERVAL_EACH = config.interval.notify_tx
    while True:
        pending_tx = await store.sql_get_new_tx_table('NO', 'NO')
        #print(pending_tx)
        if pending_tx and len(pending_tx) > 0:
            # let's notify_new_tx_user
            for eachTx in pending_tx:
                user_tx = None
                if len(eachTx['payment_id']) > 0:
                    user_tx = await store.sql_get_userwallet_by_paymentid(eachTx['payment_id'], eachTx['coin_name'], 'TELEGRAM')
                if user_tx:
                    #get_user_chat = await bot.get_chat_member()
                    is_notify_failed = False
                    to_user = user_tx['chat_id']
                    message_text = None
                    if eachTx['coin_name'] not in ["DOGE"]:
                        message_text = text(bold(f"You got a new deposit {eachTx['coin_name']}:\n"), code("Tx: {}\nAmount: {}\nHeight: {:,.0f}".format(eachTx['txid'], num_format_coin(eachTx['amount'], eachTx['coin_name']), eachTx['height'])))
                    else:
                        message_text = text(bold(f"You got a new deposit {eachTx['coin_name']}:\n"), code("Tx: {}\nAmount: {}\nBlock Hash: {}".format(eachTx['txid'], num_format_coin(eachTx['amount'], eachTx['coin_name']), eachTx['blockhash'])))
                    try:
                        send_msg = await bot.send_message(chat_id=to_user, text=message_text, parse_mode=ParseMode.MARKDOWN)
                        if send_msg:
                            is_notify_failed = False
                        else:
                            print("Can not send message")
                            is_notify_failed = True
                    except exceptions.BotBlocked:
                        print(f"Target [ID:{to_user}]: blocked by user")
                    except exceptions.ChatNotFound:
                        print(f"Target [ID:{to_user}]: invalid user ID")
                    except exceptions.RetryAfter as e:
                        print(f"Target [ID:{to_user}]: Flood limit is exceeded. Sleep {e.timeout} seconds.")
                        await asyncio.sleep(e.timeout)
                        return await bot.send_message(chat_id=to_user, text=message_text, parse_mode=ParseMode.MARKDOWN)  # Recursive call
                    except exceptions.UserDeactivated:
                        print(f"Target [ID:{to_user}]: user is deactivated")
                    except exceptions.TelegramAPIError:
                        print(f"Target [ID:{to_user}]: failed")
                    except Exception as e:
                        traceback.print_exc(file=sys.stdout)
                        is_notify_failed = True
                    finally:
                         update_notify_tx = await store.sql_update_notify_tx_table(eachTx['payment_id'], user_tx['user_id'], user_tx['user_id'], 'YES', 'NO' if is_notify_failed == False else 'YES')
        await asyncio.sleep(INTERVAL_EACH)


def is_maintenance_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()
    # Check if exist in redis
    try:
        openRedis()
        key = 'TIPBOT:COIN_' + COIN_NAME + '_MAINT'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_coin_txable(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()
    if is_maintenance_coin(COIN_NAME):
        return False
    # Check if exist in redis
    try:
        openRedis()
        key = 'TIPBOT:COIN_' + COIN_NAME + '_TX'
        if redis_conn and redis_conn.exists(key):
            return False
        else:
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_coin_depositable(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()
    if is_maintenance_coin(COIN_NAME):
        return False
    # Check if exist in redis
    try:
        openRedis()
        key = 'TIPBOT:COIN_' + COIN_NAME + '_DEPOSIT'
        if redis_conn and redis_conn.exists(key):
            return False
        else:
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_coin_tipable(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()
    if is_maintenance_coin(COIN_NAME):
        return False
    # Check if exist in redis
    try:
        openRedis()
        key = 'TIPBOT:COIN_' + COIN_NAME + '_TIP'
        if redis_conn and redis_conn.exists(key):
            return False
        else:
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_tradeable_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_TRADEABLE'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_depositable_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_DEPOSIT'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_withdrawable_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_WITHDRAW'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


async def store_action_list():
    while True:
        interval_action_list = 60
        try:
            openRedis()
            key = "MarketBOT:ACTIONTX"
            if redis_conn and redis_conn.llen(key) > 0 :
                temp_action_list = []
                for each in redis_conn.lrange(key, 0, -1):
                    temp_action_list.append(tuple(json.loads(each)))
                num_add = store.sql_add_logs_tx(temp_action_list)
                if num_add > 0:
                    redis_conn.delete(key)
                else:
                    print(f"Failed delete {key}")
        except Exception as e:
            traceback.print_exc(file=sys.stdout)
        await asyncio.sleep(interval_action_list)


async def add_tx_action_redis(action: str, delete_temp: bool = False):
    try:
        openRedis()
        key = "MarketBOT:ACTIONTX"
        if redis_conn:
            if delete_temp:
                redis_conn.delete(key)
            else:
                redis_conn.lpush(key, action)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


if __name__ == '__main__':
    dp.loop.create_task(notify_new_tx_user())
    dp.loop.create_task(store_action_list())
    executor.start_polling(dp, skip_updates=True)
