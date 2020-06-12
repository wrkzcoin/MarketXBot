import discord
from discord.ext import commands
from discord.ext.commands import Bot, AutoShardedBot, when_mentioned_or, CheckFailure
from discord.utils import get

import time, timeago
from datetime import datetime, timedelta
from config import config
import click
import sys, traceback
import asyncio
# ascii table
from terminaltables import AsciiTable

import uuid, json
import aiohttp

import store, addressvalidation, walletapi
from wallet import *

from generic_xmr.address_msr import address_msr as address_msr
from generic_xmr.address_xmr import address_xmr as address_xmr
from generic_xmr.address_xam import address_xam as address_xam

# regex
import re

# hashlib
import hashlib
import base64
import os.path, os

# for randomString
import random
import string

#
from Crypto.Cipher import AES

# redis
import redis
redis_pool = None
redis_conn = None
redis_expired = 120

# Coin using wallet-api
ENABLE_COIN = config.Enable_Coin.split(",")
ENABLE_XMR = config.Enable_Coin_XMR.split(",")
ENABLE_SWAP = config.Enabe_Swap_Coin.split(",")
ENABLE_COIN_DIGI = config.Enable_Coin_Digi.split(",")

MIN_RATIO = float(config.Min_Ratio)
TRADE_PERCENT = config.Trade_Margin
IS_TESTING = False
IS_RESTARTING = False
TESTER = [386761001808166912, 430148078855520296, 185223615107694592, 431654339359277067]
NOTIFY_CHAN = config.discord.channelNotify

EMOJI_ERROR = "\u274C"
EMOJI_OK_BOX = "\U0001F197"
EMOJI_MAINTENANCE = "\U0001F527"
EMOJI_CHECK = "\u2705"
EMOJI_WARNING = "\u26A1"
EMOJI_RED_NO = "\u26D4"
EMOJI_SCALE = "\u2696"
EMOJI_OK_HAND = "\U0001F44C"
EMOJI_MONEYBAG = "\U0001F4B0"
EMOJI_QUESTEXCLAIM = "\u2049"
EMOJI_ARROW_RIGHTHOOK = "\u21AA"
EMOJI_REFRESH = "\U0001F504"

EMOJI_THUMB_UP = "\U0001F44D"
EMOJI_THUMB_DOWN = "\U0001F44E"

NOTICE_START_SELL = """
```
By creating a ditigal selling using this bot, you will need to accept the following term:\n

1) Maintenance fee """+"{0:.0%}".format(config.Merchant_Setting.Fee_Margin)+""" for each sold product.

1) If you buy, it is not refund-able. However, you can rate / review the seller.

2) This bot will also needs to DM you in case of some notification of buy/sell activities.

3) This term can be updated without notification.

4) This Bot is still under testing.

* Consider donation via donate command if you like it.
```
"""

# param introduce by @bobbieltd
WITHDRAW_IN_PROCESS = []
QUEUE_ADD_LIST = []

# Some notice about coin that going to swap or take out.
NOTICE_COIN = {
    "WRKZ" : getattr(getattr(config,"coinWRKZ"),"coin_notice", None),
    "TRTL" : getattr(getattr(config,"coinTRTL"),"coin_notice", None),
    "DEGO" : getattr(getattr(config,"coinDEGO"),"coin_notice", None),
    "BTCMZ" : getattr(getattr(config,"coinBTCMZ"),"coin_notice", None),
    "PLE" : getattr(getattr(config,"coinPLE"),"coin_notice", None),
    "XTOR" : getattr(getattr(config,"coinXTOR"),"coin_notice", None),
    "LOKI" : getattr(getattr(config,"coinLOKI"),"coin_notice", None),
    "XMR" : getattr(getattr(config,"coinXMR"),"coin_notice", None),
    "ARQ" : getattr(getattr(config,"coinARQ"),"coin_notice", None),
    "XEQ" : getattr(getattr(config,"coinARQ"),"coin_notice", None),
    "default": "Thank you for using."
    }

bot_help_admin_lockuser = "Lock a user from any action by user id"
bot_help_admin_unlockuser = "Unlock a user"
bot_help_deposit = "Get your wallet ticker's deposit."
bot_help_balance = "Check your balance."
bot_help_send = "Send coin to an address from your balance."
bot_help_coininfo = "List of coin status."
bot_help_market = "List of coin open order market."
bot_help_buy = "Buy from open order number."
bot_help_sell = "Sell for another coin."
bot_help_cancel = "Cancel sell order by ticker or ALL."
bot_help_myorder = "List your active open sell orders"
bot_help_order_num = "Show an order number"
bot_help_admin_baluser = "Show a user balance"
bot_help_swap = "Swap balance amount between our bot to our bot"

bot_help_merchant = "Manage your digital selling."
bot_help_merchant_add = "Add a digital product for selling"
bot_help_merchant_addfile = "Attach file to a selling product"
bot_help_merchant_detail = "View an item in detail by a ref number."
bot_help_merchant_buy = "Buy a digital product using your balance."
bot_help_merchant_mylist = "List of your active selling items."
bot_help_merchant_getitem = "Get an item that you already purchased."
bot_help_merchant_sell = "Turn a pending to available to purchase by other."
bot_help_merchant_unsell = "Turn a selling item to PENDING."
bot_help_merchant_unlink = "Remove a file from an item"
bot_help_merchant_blist = "List items that you purchased."

bot_help_about = "About MarketXBot."
bot_help_invite = "Invite link of bot to your server."
bot_help_donate = "Donate to support Crypto MarketXBot."

# Steal from https://github.com/cree-py/RemixBot/blob/master/bot.py#L49
async def get_prefix(bot, message):
    """Gets the prefix for the guild"""
    pre_cmd = config.discord.prefixCmd
    if isinstance(message.channel, discord.DMChannel):
        pre_cmd = config.discord.prefixCmd
        extras = [pre_cmd, ',', 'mxb.', 'mxb!']
        return when_mentioned_or(*extras)(bot, message)

    serverinfo = store.sql_info_by_server(str(message.guild.id))
    if serverinfo is None:
        # Let's add some info if guild return None
        add_server_info = store.sql_addinfo_by_server(str(message.guild.id), message.guild.name,
                                                      config.discord.prefixCmd)
        pre_cmd = config.discord.prefixCmd
        serverinfo = store.sql_info_by_server(str(message.guild.id))
    if serverinfo and ('prefix' in serverinfo):
        pre_cmd = serverinfo['prefix']
    else:
        pre_cmd =  config.discord.prefixCmd
    extras = [pre_cmd, 'mxb.', 'mxb!']
    return when_mentioned_or(*extras)(bot, message)

bot = AutoShardedBot(command_prefix=get_prefix, owner_id = config.discord.ownerID, case_insensitive=True)


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


def get_notice_txt(coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME in NOTICE_COIN:
        if NOTICE_COIN[COIN_NAME] is None:
            return "*Any support for this TradeBot, please join* `https://chat.wrkz.work`"
        else:
            return NOTICE_COIN[COIN_NAME]
    else:
        return "*Any support for this TradeBot, please join* `https://chat.wrkz.work`"


@bot.event
async def on_shard_ready(shard_id):
    print(f'Shard {shard_id} connected')


@bot.event
async def on_ready():
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')
    game = discord.Game(name="Testing Only")
    await bot.change_presence(status=discord.Status.online, activity=game)


@bot.event
async def on_reaction_add(reaction, user):
    # If bot re-act, ignore.
    if user.id == bot.user.id:
        return
    # If other people beside bot react.
    else:
        # If re-action is OK box and message author is bot itself
        if reaction.emoji == EMOJI_OK_BOX and reaction.message.author.id == bot.user.id:
            await reaction.message.delete()


@bot.event
async def on_raw_reaction_add(payload):
    if payload.guild_id is None:
        return  # Reaction is on a private message
    """Handle a reaction add."""
    try:
        emoji_partial = str(payload.emoji)
        message_id = payload.message_id
        channel_id = payload.channel_id
        user_id = payload.user_id
        guild = bot.get_guild(payload.guild_id)
        channel = bot.get_channel(id=channel_id)
        if not channel:
            return
        if isinstance(channel, discord.DMChannel):
            return
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        return
    message = None
    author = None
    if message_id:
        try:
            message = await channel.fetch_message(message_id)
            author = message.author
        except (discord.errors.NotFound, discord.errors.Forbidden) as e:
            # No message found
            return
        member = bot.get_user(id=user_id)
        if emoji_partial in [EMOJI_OK_BOX] and message.author.id == bot.user.id \
            and author != member and message:
            # Delete message
            try:
                await message.delete()
                return
            except discord.errors.NotFound as e:
                # No message found
                return


@bot.group(hidden = True)
@commands.is_owner()
async def admin(ctx):
    if ctx.invoked_subcommand is None:
        await ctx.send('Invalid `admin` command passed...')
    return


@commands.is_owner()
@admin.command(help=bot_help_admin_baluser)
async def baluser(ctx, user_id: str):
    global ENABLE_COIN
    table_data = [
        ['TICKER', 'Available', 'Open Order']
        ]
    for COIN_NAME in [coinItem.upper() for coinItem in ENABLE_COIN]:
        if not is_maintenance_coin(COIN_NAME):
            wallet = await store.sql_get_userwallet(user_id, COIN_NAME, 'DISCORD')
            if wallet is None:
                table_data.append([COIN_NAME, "N/A", "N/A"])
            else:
                actual = wallet['actual_balance']
                userdata_balance = store.sql_user_balance(user_id, COIN_NAME, 'DISCORD')
                balance_actual = num_format_coin(actual + float(userdata_balance['Adjust']), COIN_NAME)
                table_data.append([COIN_NAME, balance_actual, num_format_coin(userdata_balance['OpenOrder'], COIN_NAME)])
        else:
            table_data.append([COIN_NAME, "***", "***"])

    table = AsciiTable(table_data)
    # table.inner_column_border = False
    # table.outer_border = False
    table.padding_left = 0
    table.padding_right = 0
    await ctx.message.author.send(f'**[ {user_id} BALANCE LIST ]**\n'
                                  f'```{table.table}```')
    return


@commands.is_owner()
@admin.command(pass_context=True, name='shutdown', aliases=['restart'])
async def shutdown(ctx):
    global IS_RESTARTING
    if IS_RESTARTING:
        await ctx.send(f'{EMOJI_REFRESH} {ctx.author.mention} I already got this command earlier.')
        return
    IS_RESTARTING = True
    await ctx.send(f'{EMOJI_REFRESH} {ctx.author.mention} .. I will restarting in 10s.. back soon.')
    await asyncio.sleep(10)
    await bot.logout()


@commands.is_owner()
@admin.command(aliases=['addbalance'])
async def credit(ctx, amount: str, coin: str, to_userid: str):
    global IS_RESTARTING
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    # check if bot can find user
    member = bot.get_user(id=int(to_userid))
    if not member:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} I cannot find user with userid **{to_userid}**.')
        return
    # check if user / address exist in database
    amount = amount.replace(",", "")
    try:
        amount = float(amount)
    except ValueError:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid credit amount.')
        return

    coin_family = None
    wallet = None
    try:
        coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **INVALID TICKER**')
        return

    COIN_DEC = get_decimal(COIN_NAME)
    real_amount = int(amount * COIN_DEC) if coin_family in ["XMR", "TRTL"] else amount * COIN_DEC

    if coin_family == "TRTL" or coin_family == "CCX" or coin_family == "XMR":
        wallet = await store.sql_get_userwallet(to_userid, COIN_NAME, 'DISCORD')
        if wallet is None:
            userregister = await store.sql_register_user(to_userid, COIN_NAME, 'DISCORD')
            wallet = await store.sql_get_userwallet(to_userid, COIN_NAME, 'DISCORD')
    credit_to = store.sql_cn_xmr_credit(str(ctx.message.author.id), to_userid, real_amount, COIN_NAME, ctx.message.content)
    if credit_to:
        msg = await ctx.send(f'{ctx.author.mention} amount **{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}** has been credited to userid **{to_userid}**.')
        return


@commands.is_owner()
@admin.command(aliases=['maintenance'])
async def maint(ctx, coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    if is_maintenance_coin(COIN_NAME):
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** to maintenance **OFF**.')
        set_main = set_maintenance_coin(COIN_NAME, False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** to maintenance **ON**.')
        set_main = set_maintenance_coin(COIN_NAME, True)
    return


@commands.is_owner()
@admin.command(aliases=['stat'])
async def stats(ctx):
    table_data = [
        ['TICKER', 'Daemon Height']
        ]
    for COIN_NAME in [coinItem.upper() for coinItem in ENABLE_COIN]:
        height = None
        try:
            openRedis()
            if redis_conn and redis_conn.exists(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'):
                height = int(redis_conn.get(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'))
                if not is_maintenance_coin(COIN_NAME):
                    table_data.append([COIN_NAME,  '{:,.0f}'.format(height)])
                else:
                    table_data.append([COIN_NAME, "***"])
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    table = AsciiTable(table_data)
    # table.inner_column_border = False
    # table.outer_border = False
    table.padding_left = 1
    table.padding_right = 1
    msg = await ctx.message.author.send('**[ DAEMON LIST ]**\n'
                                        f'```{table.table}```')
    
    return


@commands.is_owner()
@admin.command(aliases=['txable'])
async def sendable(ctx, coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    if is_coin_sendable(COIN_NAME):
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **DISABLE** TX.')
        set_main = set_coin_sendable(COIN_NAME, False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **ENABLE** TX.')
        set_main = set_coin_sendable(COIN_NAME, True)
    return


@commands.is_owner()
@admin.command(aliases=['trade'])
async def tradeable(ctx, coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    if is_tradeable_coin(COIN_NAME):
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **DISABLE** trade.')
        set_main = set_tradeable_coin(COIN_NAME, False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **ENABLE** trade.')
        set_main = set_tradeable_coin(COIN_NAME, True)
    return


@commands.is_owner()
@admin.command(aliases=['withdraw'])
async def withdrawable(ctx, coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    if is_withdrawable_coin(COIN_NAME):
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **DISABLE** withdraw.')
        set_main = set_withdrawable_coin(COIN_NAME, False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **ENABLE** withdraw.')
        set_main = set_withdrawable_coin(COIN_NAME, True)
    return


@commands.is_owner()
@admin.command(aliases=['deposit'])
async def depositable(ctx, coin: str):
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
        return

    if is_depositable_coin(COIN_NAME):
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **DISABLE** deposit.')
        set_main = set_depositable_coin(COIN_NAME, False)
    else:
        await ctx.send(f'{EMOJI_OK_BOX} Set **{COIN_NAME}** **ENABLE** deposit.')
        set_main = set_depositable_coin(COIN_NAME, True)
    return


# just use only one command to switch ON/OFF
@commands.is_owner()
@admin.command(help=bot_help_admin_lockuser)
async def lockuser(ctx, user_id: str, *, reason: str):
    get_discord_userinfo = store.sql_discord_userinfo_get(user_id)
    if get_discord_userinfo is None:
        store.sql_userinfo_locked(user_id, 'YES', reason, str(ctx.message.author.id))
        await ctx.message.add_reaction(EMOJI_OK_HAND)
        await ctx.message.author.send(f'{user_id} is locked.')
        return
    else:
        if get_discord_userinfo['locked'].upper() == "YES":
            await ctx.message.author.send(f'{user_id} was already locked.')
        else:
            store.sql_userinfo_locked(user_id, 'YES', reason, str(ctx.message.author.id))
            await ctx.message.add_reaction(EMOJI_OK_HAND)
            await ctx.message.author.send(f'Turn {user_id} to locked.')
        return


@commands.is_owner()
@admin.command(help=bot_help_admin_unlockuser)
async def unlockuser(ctx, user_id: str):
    get_discord_userinfo = store.sql_discord_userinfo_get(user_id)
    if get_discord_userinfo:
        if get_discord_userinfo['locked'].upper() == "NO":
            await ctx.message.author.send(f'**{user_id}** was already unlocked. Nothing to do.')
        else:
            store.sql_change_userinfo_single(user_id, 'locked', 'NO')
            await ctx.message.author.send(f'Unlocked {user_id} done.')
        return      
    else:
        await ctx.message.author.send(f'{user_id} not stored in **discord userinfo** yet. Nothing to unlocked.')
        return


@bot.group(name='merchant', aliases=['m'], help=bot_help_merchant)
async def merchant(ctx):
    prefix = await get_guild_prefix(ctx)
    if ctx.invoked_subcommand is None:
        await ctx.send(f'{ctx.author.mention} Invalid merchant command.\n'
                       f'Please use help of each subcommand. Example: {prefix}help merchant OR {prefix}help merchant subcommand')
    return


@merchant.command(help=bot_help_merchant_add)
async def add(ctx):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    prefix = await get_guild_prefix(ctx)
    if ctx.message.author.id in QUEUE_ADD_LIST:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you are still in another progress of adding bounty.\n'
                       'You can cancel it also by: {prefix}merchant cancel')
        return
    random_string = randomString(12) # use this as reference
    try:
        msg = await ctx.send(f'{ctx.author.mention} You are about to adding a digital item for selling. Please input or re-act as bot requested.\n'
                             f'{NOTICE_START_SELL}'
                             f'Re-act: {EMOJI_THUMB_UP} if you agree.')
        await msg.add_reaction(EMOJI_THUMB_UP)

        def check(reaction, user):
            return user == ctx.message.author and reaction.message.author == bot.user and reaction.message.id == msg.id and str(reaction.emoji) == EMOJI_THUMB_UP
        try:
            reaction, user = await bot.wait_for('reaction_add', timeout=config.Merchant_Setting.default_timeout, check=check)
        except asyncio.TimeoutError:
            await ctx.send(f'{ctx.author.mention} too long. We assumed you are {EMOJI_THUMB_DOWN}')
            return
        else:
            await ctx.send(f'{ctx.author.mention} Thank you for {EMOJI_THUMB_UP}. Your ongoing product input ref_id: **{random_string}**. Let\'s move to next step.')
            pass
        # store temporary data and start interactive
        QUEUE_ADD_LIST.append(ctx.message.author.id)
        msg = await ctx.send(f'ITEM ID: **{random_string}**\n'
                             f'Please type in **amount coin_name** for the price to sell (timeout {config.Merchant_Setting.price_timeout}s):\n'
                             'Supported coin: {}'.format(", ".join(ENABLE_COIN_DIGI)))

        amount = None
        COIN_NAME = None
        while (amount is None) or (COIN_NAME not in ENABLE_COIN_DIGI):
            waiting_pricemsg = None
            try:
                waiting_pricemsg = await bot.wait_for('message', timeout=config.Merchant_Setting.price_timeout, check=lambda msg: msg.author == ctx.author)
            except asyncio.TimeoutError:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'ITEM ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during price input.')
                return
            if waiting_pricemsg is None:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'ITEM ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during price input.')
                return
            else:
                msg_content = waiting_pricemsg.content.split(" ")
                if len(msg_content) != 2:
                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'{ctx.author.mention} Please use **amount coin_name**. Example `1,000,000 WRKZ`')
                else:
                    amount = msg_content[0]
                    COIN_NAME = msg_content[1].upper()
                    if COIN_NAME not in ENABLE_COIN_DIGI:
                        accepted_coin = ", ".join(ENABLE_COIN_DIGI)
                        await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                        await ctx.send(f'{ctx.author.mention} Please use accepted coin: **{accepted_coin}**.')
                    else:
                        amount = amount.replace(",", "")
                        try:
                            amount = float(amount)
                            MinTx = get_min_digisell(COIN_NAME)
                            MaxTX = get_max_digisell(COIN_NAME)
                            real_amount = amount*get_decimal(COIN_NAME)
                            if MinTx <= real_amount <= MaxTX:
                                # OK, we can list it
                                pass
                            else:
                                await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                                await ctx.send(f'{ctx.author.mention} Amount input is not between '
                                               f'{num_format_coin(MinTx, COIN_NAME)}{COIN_NAME}, {num_format_coin(MaxTX, COIN_NAME)}{COIN_NAME}.')
                                amount = None
                        except ValueError:
                            amount = None
                            await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid amount.')
        msg = await ctx.send(f'ITEM ID: **{random_string}**\n'
                             f'Selling price is set to: **{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}**\n'
                             f'Next, please give a title of this item (min. {config.Merchant_Setting.title_min}, '
                             f'max. {config.Merchant_Setting.title_max} chars, timeout {config.Merchant_Setting.title_timeout}s):')
        # TITLE
        title = None
        while title is None:
            waiting_titlemsg = None
            try:
                waiting_titlemsg = await bot.wait_for('message', timeout=config.Merchant_Setting.title_timeout, check=lambda msg: msg.author == ctx.author)
            except asyncio.TimeoutError:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'ITEM ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during title input.')
                return
            if waiting_titlemsg is None:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'ITEM ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during title input.')
                return
            else:
                if config.Merchant_Setting.title_min <= len(waiting_titlemsg.content) <= config.Merchant_Setting.title_max:
                    title = waiting_titlemsg.content.strip()
                    msg = await ctx.send(f'ITEM ID: **{random_string}**\n'
                                         f'Item title set: **{title}**\n'
                                         f'Next, please give a description of this item (min. {config.Merchant_Setting.desc_min}, '
                                         f'max. {config.Merchant_Setting.desc_max} chars, timeout {config.Merchant_Setting.desc_timeout}s):')
                else:
                    title = None
                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'ITEM ID: **{random_string}**\n'
                                   f'{ctx.author.mention} title too long or too short.')
        # DESC
        desc = None
        while desc is None:
            waiting_descmsg = None
            try:
                waiting_descmsg = await bot.wait_for('message', timeout=config.Merchant_Setting.desc_timeout, check=lambda msg: msg.author == ctx.author)
            except asyncio.TimeoutError:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'ITEM ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during description input.')
                return
            if waiting_descmsg is None:
                # Delete redis and remove user from QUEUE
                delete_queue_going(ctx.message.author.id)
                await ctx.send(f'ITEM ID: **{random_string}**\n'
                               f'{ctx.author.mention} too long. We assumed you are gave up during description input.')
                return
            else:
                if config.Merchant_Setting.desc_min <= len(waiting_descmsg.content) <= config.Merchant_Setting.desc_max:
                    desc = waiting_descmsg.content.strip()
                    msg = await ctx.send(f'ITEM ID: **{random_string}**\n'
                                         f'Description set: **{desc}**\n')
                else:
                    await waiting_pricemsg.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'ITEM ID: **{random_string}**\n'
                                   f'{ctx.author.mention} description too long or too short.')
        try:
            embed = discord.Embed(title="ITEM: {}".format(title), description="Here's your item briefing.", color=0x00ff00)
            embed.add_field(name="Amount", value=f'{num_format_coin(real_amount, COIN_NAME)}{COIN_NAME}', inline=True)
            embed.add_field(name="Seller", value=ctx.author.mention, inline=False)
            embed.add_field(name="Description", value=desc, inline=False)
            embed.add_field(name="Re-act", value=f'{EMOJI_THUMB_UP} to confirm, {EMOJI_THUMB_DOWN} to cancel', inline=False)
            embed.set_thumbnail(url=ctx.message.author.avatar_url)
            msg = await ctx.send(embed=embed)
            await msg.add_reaction(EMOJI_THUMB_DOWN)
            await msg.add_reaction(EMOJI_THUMB_UP)
            def check(reaction, user):
                return user == ctx.message.author and reaction.message.id == msg.id  \
                and str(reaction.emoji) in (EMOJI_THUMB_DOWN, EMOJI_THUMB_UP)
            reaction, user = await bot.wait_for('reaction_add', check=check)
            if str(reaction.emoji) == EMOJI_THUMB_UP:
                add = store.sql_digi_order_add_data(random_string, title, desc, COIN_NAME, real_amount, real_amount*(1-config.Merchant_Setting.Fee_Margin),
                                                    get_decimal(COIN_NAME), str(ctx.message.author.id), 
                                                    '{}#{}'.format(ctx.message.author.name, ctx.message.author.discriminator), 'PENDING', 'DISCORD')
                if add: 
                    msg = await ctx.send(f'ITEM ID: **{random_string}**\n'
                                         f'Status: **Submitted and pending**\n'
                                         f'Please attach file and input comment **{prefix}m addfile {random_string}**\n'
                                         f'You can add file one by one (supported only {config.Merchant_Setting.allow_ext} format).')
                    delete_queue_going(ctx.message.author.id)
                    await msg.add_reaction(EMOJI_OK_BOX)
                else:
                    msg = await ctx.send(f'ITEM ID: **{random_string}**\n'
                                         f'Status: **Not submitted** (Internal Error)')
                    await msg.add_reaction(EMOJI_OK_BOX)
            elif str(reaction.emoji) == EMOJI_THUMB_DOWN:
                delete_queue_going(ctx.message.author.id)
                msg = await ctx.send(f'ITEM ID: **{random_string}**\n'
                                     f'Status: **Cancelled**')
        except:
            error = discord.Embed(title=":exclamation: Error", description=" :warning: Failed to display!", color=0xe51e1e)
            await ctx.send(embed=error)
    except (discord.Forbidden, discord.errors.Forbidden) as e:
        # no permission to text or to re-act
        await ctx.message.add_reaction(EMOJI_ERROR)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


@merchant.command(help=bot_help_merchant_addfile)
async def addfile(ctx, ref_id: str):
    global redis_pool, redis_conn
    ref = ref_id.lower()
    prefix = await get_guild_prefix(ctx)
    get_item_ref = store.sql_merchant_get_ref(ref)
    if get_item_ref:
        if int(get_item_ref['owner_id']) != ctx.message.author.id:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You can not add files to item **{ref}**.')
            return
        if get_item_ref['status'] == 'SUSPENDED':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Item **{ref}** is **SUSPENDED**.')
            return
        # Check number of file if exceed
        get_files_ref = store.sql_merchant_get_files_by_ref(ref)
        if get_files_ref and len(get_files_ref) >= config.Merchant_Setting.max_file_per_item:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Item **{ref}** is already having {len(get_files_ref)} files.'
                           ' Can not add more.')
            return
        if not ctx.message.attachments:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} There is no attachment in your message for **{ref}**.')
            return
        else:
            attachment = ctx.message.attachments[0]
            allowed_ext = config.Merchant_Setting.allow_ext
            if not (attachment.filename.lower()).endswith(tuple(allowed_ext.split(","))):
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Attachment type rejected. Allowed: {allowed_ext}')
                return
            if attachment.size >= config.Merchant_Setting.max_size:
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} File too big.')
                return
            # check filename:
            link = attachment.url # https://cdn.discordapp.com/attachments
            if not re.match(r'^[a-zA-Z0-9\d.\-_]*$', attachment.filename.replace(" ", "-")):
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Unacceptable file name.')
                return
            else:
                attach_save_name = str(uuid.uuid4()) + '-' + attachment.filename.lower().replace(" ", "-")
            try:
                if link.startswith("https://cdn.discordapp.com/attachments"):
                    async with aiohttp.ClientSession() as session:
                        async with session.get(link) as resp:
                            if resp.status == 200:
                                if resp.headers["Content-Type"] not in ["application/zip", "application/gzip", "application/x-bzip2", "application/x-xz", "application/x-7z-compressed"]:
                                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Unsupported format file.')
                                    return
                                else: 
                                    with open(config.Merchant_Setting.path + attach_save_name, 'wb') as f:
                                        f.write(await resp.read())
                                    file_ref = "file_" + randomString(12)
                                    md5_hash = get_md5_file(config.Merchant_Setting.path + attach_save_name)
                                    uploadfile = store.sql_merchant_add_file(ref, file_ref, md5_hash, str(ctx.message.author.id), attachment.filename, 
                                                                             attach_save_name, attachment.size, resp.headers["Content-Type"])
                                    if uploadfile:
                                        await ctx.message.add_reaction(EMOJI_OK_HAND)
                                        await ctx.send(f'{ctx.author.mention} Successfully uploaded file **{attachment.filename}** for selling item: **{ref}**.')
                                        return
                                    else:
                                        await ctx.message.add_reaction(EMOJI_ERROR)
                                        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Failed to upload and store.')
                                        return
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not find selling item **{ref}**.')
        return


@merchant.command(help=bot_help_merchant_sell)
async def sell(ctx, ref_id: str):
    global redis_pool, redis_conn
    ref = ref_id.lower()
    prefix = await get_guild_prefix(ctx)
    get_item_ref = store.sql_merchant_get_ref(ref)
    if get_item_ref:
        if int(get_item_ref['owner_id']) != ctx.message.author.id:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You can not turn on this item **{ref}**.')
            return
        if get_item_ref['status'] == 'SUSPENDED':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Item **{ref}** is **SUSPENDED**.')
            return
        elif get_item_ref['status'] == 'PENDING':
            # Check if there is file with it or not
            get_files_ref = store.sql_merchant_get_files_by_ref(ref)
            if get_files_ref is None or (get_files_ref and len(get_files_ref) == 0):
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} There is no files for item **{ref}**\n.'
                               f'Please upload one or more before doing this by: {prefix}m addfile {ref}')
                return
            else:
                # OK turn it to available
                updated = store.sql_merchant_update_by_ref(ref, 'status', 'AVAILABLE', 'DISCORD')
                if updated:
                    await ctx.message.add_reaction(EMOJI_OK_HAND)
                    await ctx.send(f'{ctx.author.mention} Successfully updated status item **{ref}** to **AVAILABLE**')
                    return
                else:
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Failed to update status item **{ref}**')
                    return
        elif get_item_ref['status'] == 'AVAILABLE':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Item **{ref}** is already **AVAILABLE** Nothing to do.')
            return
    return


@merchant.command(help=bot_help_merchant_unsell)
async def unsell(ctx, ref_id: str):
    global redis_pool, redis_conn
    ref = ref_id.lower()
    prefix = await get_guild_prefix(ctx)
    get_item_ref = store.sql_merchant_get_ref(ref)
    if get_item_ref:
        if int(get_item_ref['owner_id']) != ctx.message.author.id:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You can not have permission for this item **{ref}**.')
            return
        if get_item_ref['status'] == 'SUSPENDED':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Item **{ref}** is **SUSPENDED**.')
            return
        elif get_item_ref['status'] == 'AVAILABLE':
            # OK turn it to back to pending
            updated = store.sql_merchant_update_by_ref(ref, 'status', 'PENDING', 'DISCORD')
            if updated:
                await ctx.message.add_reaction(EMOJI_OK_HAND)
                await ctx.send(f'{ctx.author.mention} Successfully updated status item **{ref}** to **PENDING**')
                return
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Failed to update status item **{ref}**')
                return
        elif get_item_ref['status'] == 'PENDING':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Item **{ref}** is already **PENDING** Nothing to do.')
            return
    return


@merchant.command(help=bot_help_merchant_mylist)
async def mylist(ctx, status: str='ALL'):
    list_selling_items = None
    status = status.upper()
    if status.upper() not in ['AVAILABLE','SUSPENDED','PENDING', 'ALL']:
        status_list = ", ".join(['AVAILABLE','SUSPENDED','PENDING'])
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Please give filter of correct status ({status_list}).')
        return
    else:
        list_selling_items = store.sql_merchant_list_of_user(str(ctx.message.author.id), status, 'DISCORD', 20)
    if list_selling_items and len(list_selling_items) > 0:
        table_data = [
            ['REF', 'Amount', 'Status', 'Sold', 'Title']
        ]
        for each in list_selling_items:
            table_data.append([each['ref_id'], num_format_coin(each['item_cost'], each['coin_name'])+each['coin_name'], 
                               each['status'], each['numb_bought'], each['title'][:32]])
        table = AsciiTable(table_data)
        # table.inner_column_border = False
        # table.outer_border = False
        table.padding_left = 1
        table.padding_right = 1
        msg = await ctx.send('**[ YOUR SELLING LIST ]**\n'
                             f'```{table.table}```')
        return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you do not have selling product.')
        return


@merchant.command(help=bot_help_merchant_blist)
async def blist(ctx):
    list_selling_items = store.sql_merchant_get_buy_by_bought_userid(str(ctx.message.author.id), 'DISCORD', 25)
    if list_selling_items and len(list_selling_items) > 0:
        table_data = [
            ['Ref', 'Amount', 'Bought Date']
        ]
        for each in list_selling_items:
            table_data.append([each['buy_ref'], num_format_coin(each['item_cost'], each['coin_name'])+each['coin_name'], 
                              datetime.fromtimestamp(each['bought_date'])])
        table = AsciiTable(table_data)
        # table.inner_column_border = False
        # table.outer_border = False
        table.padding_left = 1
        table.padding_right = 1
        msg = await ctx.send('**[ YOUR BOUGHT ITEM ]**\n'
                             f'```{table.table}```')
        return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} you did not buy any product.')
        return

@merchant.command(help=bot_help_merchant_buy)
async def buy(ctx, ref: str):
    global redis_pool, redis_conn
    ref = ref.lower()
    prefix = await get_guild_prefix(ctx)
    get_item_ref = store.sql_merchant_get_ref(ref)
    if get_item_ref:
        if int(get_item_ref['owner_id']) == ctx.message.author.id:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You can not buy for your own item **{ref}**.')
            return
        if get_item_ref['status'] != 'AVAILABLE':
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Item **{ref}** is not **AVAILABLE**.')
            return
        else:
            # OK, item is available to buy
            check_bought = store.sql_merchant_get_bought_by_ref(str(ctx.message.author.id), ref, 'DISCORD')
            if check_bought:
                item_ref = check_bought['buy_ref']
                await ctx.message.add_reaction(EMOJI_ERROR)
                msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You already bought this item: **{ref}**.\n'
                                     f'Type: {prefix}m get {item_ref}')
                await msg.add_reaction(EMOJI_OK_BOX)
                return
            COIN_NAME = get_item_ref['coin_name']
            # Check available balance
            user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            if user_from is None:
                userregister = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')

            userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            user_from['actual_balance'] = user_from['actual_balance'] + float(userdata_balance['Adjust'])

            needed = num_format_coin(get_item_ref['item_cost'], COIN_NAME)
            having = num_format_coin(user_from['actual_balance'], COIN_NAME)
                
            if get_item_ref['item_cost'] > user_from['actual_balance']:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to buy **{ref}**\n'
                               f'Need: {needed}{COIN_NAME}\n'
                               f'You have {having}{COIN_NAME}')
                return
            else:
                buy_ref = "b_" + randomString(10)
                # He has sufficient balance to buy
                bought = store.sql_merchant_add_bought(ref, buy_ref, get_item_ref['owner_id'], str(ctx.message.author.id), 
                                                       '{}#{}'.format(ctx.message.author.name, ctx.message.author.discriminator), 
                                                       COIN_NAME, get_item_ref['item_cost'], get_item_ref['item_cost_after_fee'],
                                                       get_decimal(COIN_NAME), 'DISCORD')
                if bought:
                    await ctx.message.add_reaction(EMOJI_OK_HAND)
                    msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You have bought item **{ref}**\n'
                                         f'For a cost of: {needed}{COIN_NAME}\n'
                                         f'To get the item, type: **{prefix}m get {buy_ref}**. Bot shall DM you.')
                    await msg.add_reaction(EMOJI_OK_BOX)
                    # TODO: alert seller
                    return
                else:
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Failed to buy item: **{ref}**.')
                    await msg.add_reaction(EMOJI_OK_BOX)
                return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not find selling item **{ref}**.')
        return
    return


@merchant.command(help=bot_help_merchant_getitem)
async def get(ctx, buy_ref: str):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    prefix = await get_guild_prefix(ctx)
    buy_ref = buy_ref.lower()
    check_bought = store.sql_merchant_get_buy_by_bought_ref('*', buy_ref, 'DISCORD')
    if check_bought and (int(check_bought['bought_userid']) == ctx.message.author.id or int(check_bought['owner_id']) == ctx.message.author.id):
        # He bought it or he is the Owner
        await ctx.message.add_reaction(EMOJI_OK_HAND)
        try:
            get_files_ref = store.sql_merchant_get_files_by_ref(check_bought['ref_id'])
            ref = check_bought['ref_id']
            if get_files_ref and len(get_files_ref) < 1:
                await ctx.message.add_reaction(EMOJI_ERROR)
                msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} There is no file for ref bought **{buy_ref}** for item **{ref}**.')
                return
            elif get_files_ref and len(get_files_ref) >= 1:
                embed = discord.Embed(title="ITEM bought ref: {}".format(buy_ref), description="Files", color=0x00ff00)
                i=1
                for each in get_files_ref:
                    url = encrypted_string('{},{},{}'.format(each['ref_id'], each['file_id'], str(int(time.time())+300)))
                    embed.add_field(name='File {}/{}'.format(i, len(get_files_ref)), value='{}'.format(config.Merchant_Setting.static_link + '/download-' + url), inline=False)
                    i+=1
                embed.set_thumbnail(url=ctx.message.author.avatar_url)
                try:
                    msg = await ctx.send(embed=embed)
                    await msg.add_reaction(EMOJI_OK_BOX)
                    if isinstance(ctx.channel, discord.DMChannel) == False:
                        # If public, delete msg after a few seconds
                        await asyncio.sleep(10)
                        await msg.delete()
                except (discord.errors.NotFound, discord.errors.Forbidden) as e:
                    msg = await ctx.send(f'{ctx.author.mention} I failed to DM you.\n')
                    await msg.add_reaction(EMOJI_OK_BOX)
                    return
        except (discord.errors.NotFound, discord.errors.Forbidden) as e:
            msg = await ctx.send(f'{ctx.author.mention} I failed to DM you.\n')
            await msg.add_reaction(EMOJI_OK_BOX)
        return
    else:
        # Check if input is selling ref
        get_item_ref = store.sql_merchant_get_ref(buy_ref)
        get_files_ref = store.sql_merchant_get_files_by_ref(buy_ref)
        if get_item_ref and int(get_item_ref['owner_id']) == ctx.message.author.id and get_files_ref and len(get_files_ref) >= 1:
            await ctx.message.add_reaction(EMOJI_OK_BOX)
            embed = discord.Embed(title="ITEM Ref: {}".format(buy_ref), description="Files", color=0x00ff00)
            embed.add_field(name="Note", value='Found via selling ref_id', inline=False)
            i=1
            for each in get_files_ref:
                url = encrypted_string('{},{},{}'.format(each['ref_id'], each['file_id'], str(int(time.time())+300)))
                embed.add_field(name='File {}/{}'.format(i, len(get_files_ref)), value='{}'.format(config.Merchant_Setting.static_link + '/download-' + url), inline=False)
                i+=1
            embed.set_thumbnail(url=ctx.message.author.avatar_url)
            msg = await ctx.send(embed=embed)
            await msg.add_reaction(EMOJI_OK_BOX)
            if isinstance(ctx.channel, discord.DMChannel) == False:
                # If public, delete msg after a few seconds
                await asyncio.sleep(10)
                await msg.delete()
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You do not have any bought item with reference: **{buy_ref}**.\n')
            await msg.add_reaction(EMOJI_OK_BOX)
            return


@merchant.command(help=bot_help_merchant_unlink)
async def unlink(ctx, ref_file: str):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    prefix = await get_guild_prefix(ctx)
    ref_file = ref_file.lower()
    check_file = store.sql_merchant_get_files_by_file_id(ref_file)
    if check_file and int(check_file['owner_id']) == ctx.message.author.id:
        # check if there is only one file left
        ref = check_file['ref_id']
        get_files_ref = store.sql_merchant_get_files_by_ref(ref)
        if get_files_ref and len(get_files_ref) <= 1:
            await ctx.message.add_reaction(EMOJI_ERROR)
            msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} There is only one file left for selling item: **{ref}**. '
                                'You need to have at least one remaining.')
            return
        else:
            # check if file exist
            if os.path.exists(config.Merchant_Setting.path + check_file['stored_name']):
                await ctx.message.add_reaction(EMOJI_OK_HAND)
                store.sql_merchant_unlink_by_file_id(ref_file)
                os.unlink(config.Merchant_Setting.path + check_file['stored_name'])
                msg = await ctx.send(f'{ctx.author.mention} Sucessfully remove **{ref_file}** from selling item: **{ref}**.')
                return
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Internal error to remove file_id **{ref_file}**.')
                return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You do not permission to file: **{ref_file}** or it does not exist.\n')
        await msg.add_reaction(EMOJI_OK_BOX)
        return


@merchant.command(help=bot_help_merchant_detail)
async def detail(ctx, ref: str):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    prefix = await get_guild_prefix(ctx)
    ref = ref.lower()
    get_item_ref = store.sql_merchant_get_ref(ref)
    if get_item_ref:
        try:
            get_files_ref = store.sql_merchant_get_files_by_ref(ref)
            embed = discord.Embed(title="Title: {}".format(get_item_ref['title']), description="Ref: {}".format(ref), color=0x00ff00)
            embed.add_field(name="Amount", value='{}{}'.format(num_format_coin(get_item_ref['item_cost'], get_item_ref['coin_name']), get_item_ref['coin_name']), inline=True)
            embed.add_field(name="Creator", value='<@{}> (id: {})'.format(get_item_ref['owner_id'], get_item_ref['owner_id']), inline=False)
            embed.add_field(name="Description", value=get_item_ref['desc'], inline=False)
            if isinstance(get_item_ref['numb_bought'], int):
                embed.add_field(name="Sold", value='{}'.format(get_item_ref['numb_bought']), inline=False)
            if get_files_ref and len(get_files_ref) > 0:
                embed.add_field(name="Files", value='{}'.format(len(get_files_ref)), inline=False)
            embed.add_field(name="Status", value=get_item_ref['status'], inline=False)
            if get_item_ref['status'] == 'AVAILABLE' and int(get_item_ref['owner_id']) != ctx.message.author.id:
                embed.add_field(name="Buy via", value='{}m buy {}'.format(prefix, ref), inline=False)
            elif int(get_item_ref['owner_id']) != ctx.message.author.id:
                embed.add_field(name="Buy?", value='Not available yet', inline=False)
            if int(get_item_ref['owner_id']) == ctx.message.author.id and isinstance(ctx.channel, discord.DMChannel) == True and len(get_files_ref) > 0:
                # if he is the owner and message via DM
                for each in get_files_ref:
                    embed.add_field(name="File_ID / File Name", value='{} / {}'.format(each['file_id'], each['original_filename']), inline=False)
            embed.set_thumbnail(url=bot.user.avatar_url)
            msg = await ctx.send(embed=embed)
            await msg.add_reaction(EMOJI_OK_BOX)
        except:
            error = discord.Embed(title=":exclamation: Error", description=" :warning: Failed to display!", color=0xe51e1e)
            await ctx.send(embed=error)
            return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not find **{ref}** bounty.')
        return
    return


@bot.command(pass_context=True, name='deposit', help=bot_help_deposit)
async def deposit(ctx, coin: str, pub: str = None):
    global ENABLE_COIN
    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    show_pub = False
    if pub and (pub.upper() == "PUB" or pub.upper() == "PUBLIC"):
        show_pub = True
    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_COIN:
        await ctx.message.add_reaction(EMOJI_RED_NO)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} in not in our list.')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {COIN_NAME} in not in our list.')
        return
    if is_maintenance_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} in maintenance.')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {COIN_NAME} in maintenance.')
        return

    if not is_depositable_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} deposit currently disable.')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {COIN_NAME} deposit currently disable.')
        return

    coin_family = None
    wallet = None
    try:
        coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **INVALID TICKER**')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} **INVALID TICKER**')
        return
    if coin_family in ["TRTL", "XMR", "DOGE"]:
        wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        if wallet is None:
            userregister = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
    if wallet is None:
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Internal Error for `.info`')
        return
    if wallet['balance_wallet_address']:
        await ctx.message.add_reaction(EMOJI_OK_HAND)
        if show_pub:
            await ctx.send(f'**[{COIN_NAME} DEPOSIT INFO]**\n'
                                        f'{EMOJI_MONEYBAG} Deposit Address: `' + wallet['balance_wallet_address'] + '`\n'
                                        f'{get_notice_txt(COIN_NAME)}')
        else:
            await ctx.message.author.send(f'**[{COIN_NAME} DEPOSIT INFO]**\n'
                                        f'{EMOJI_MONEYBAG} Deposit Address: `' + wallet['balance_wallet_address'] + '`\n'
                                        f'{get_notice_txt(COIN_NAME)}')
    return


@bot.command(pass_context=True, name='coininfo', aliases=['coinf_info', 'coin'], help=bot_help_coininfo)
async def coininfo(ctx, coin: str = None):
    global ENABLE_COIN
    if coin is None:
        table_data = [
            ['TICKER', 'Block.H', 'Trade', "Wdraw", "Deposit", "Depth"]
            ]
        for COIN_NAME in [coinItem.upper() for coinItem in ENABLE_COIN]:
            height = None
            try:
                openRedis()
                if redis_conn and redis_conn.exists(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'):
                    height = int(redis_conn.get(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'))
                    if not is_maintenance_coin(COIN_NAME):
                        table_data.append([COIN_NAME,  '{:,.0f}'.format(height), "ON" if is_tradeable_coin(COIN_NAME) else "OFF"\
                        , "ON" if is_withdrawable_coin(COIN_NAME) else "OFF", "ON" if is_depositable_coin(COIN_NAME) else "OFF"\
                        , get_confirm_depth(COIN_NAME)])
                    else:
                        table_data.append([COIN_NAME, "***", "***", "***", get_confirm_depth(COIN_NAME)])
            except Exception as e:
                traceback.print_exc(file=sys.stdout)

        table = AsciiTable(table_data)
        # table.inner_column_border = False
        # table.outer_border = False
        table.padding_left = 0
        table.padding_right = 0
        msg = await ctx.send('**[ COIN LIST ]**\n'
                                            f'```{table.table}```')
        
        return
    else:
        COIN_NAME = coin.upper()
        if COIN_NAME not in ENABLE_COIN:
            await ctx.message.author.send(f'{ctx.author.mention} **{COIN_NAME}** is not in our list.')
            return
        else:
            response_text = "**[ COIN INFO {} ]**".format(COIN_NAME)
            response_text += "```"
            try:
                openRedis()
                if redis_conn and redis_conn.exists(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'):
                    height = int(redis_conn.get(f'MarketBOT:DAEMON_HEIGHT_{COIN_NAME}'))
                    response_text += "Height: {:,.0f}".format(height) + "\n"
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
                get_sell_min_max = "Sell Min/Max:\n   " + num_format_coin(get_min_sell(COIN_NAME), COIN_NAME) + " / " + num_format_coin(get_max_sell(COIN_NAME), COIN_NAME) + COIN_NAME
                response_text += get_sell_min_max + "\n"
                get_tx_min_max = "Withdraw Min/Max:\n   " + num_format_coin(get_min_tx_amount(COIN_NAME), COIN_NAME) + " / " + num_format_coin(get_max_tx_amount(COIN_NAME), COIN_NAME) + COIN_NAME
                response_text += get_tx_min_max
            except Exception as e:
                traceback.print_exc(file=sys.stdout)


            get_markets = await store.sql_get_open_order_by_alluser(COIN_NAME, 'OPEN')
            if get_markets and len(get_markets) > 0:
                table_data = [
                    ['Order #', 'Selling', 'For']
                    ]
                total_list = 0
                for order_item in get_markets:
                    if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']) and total_list < 5:
                        table_data.append([order_item['order_id'], num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                          num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get']])
                        total_list += 1
                table = AsciiTable(table_data)
                # table.inner_column_border = False
                # table.outer_border = False
                table.padding_left = 0
                table.padding_right = 0
                response_text += "\n\nSome Active Market:\n"
                response_text += table.table
            coin_stats = await store.sql_stats_coin(COIN_NAME)
            if coin_stats:
                response_text += "\nTrade Volume:\n"
                response_text += "+ 24h: {}{}\n".format(num_format_coin(coin_stats['vol_24h'], COIN_NAME), COIN_NAME)
                response_text += "+  7d: {}{}\n".format(num_format_coin(coin_stats['vol_7d'], COIN_NAME), COIN_NAME)
                response_text += "+ 30d: {}{}".format(num_format_coin(coin_stats['vol_30d'], COIN_NAME), COIN_NAME)
            response_text += "```"
            await ctx.send(response_text)
            return


@bot.command(pass_context=True, name='balance', aliases=['bal'], help=bot_help_balance)
async def balance(ctx, pub: str = None):
    global IS_RESTARTING, ENABLE_COIN
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    if not is_testing(ctx):
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    show_pub = False
    if pub and (pub.upper() == "PUB" or pub.upper() == "PUBLIC"):
        show_pub = True

    table_data = [
        ['TICKER', 'Available', 'Open Order']
        ]
    for COIN_NAME in [coinItem.upper() for coinItem in ENABLE_COIN]:
        if not is_maintenance_coin(COIN_NAME):
            wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            if wallet is None:
                userregister = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                wallet = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            if wallet:
                userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
                wallet['actual_balance'] = int(wallet['actual_balance']) if COIN_NAME not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else wallet['actual_balance']
                balance_actual = num_format_coin(wallet['actual_balance'] + float(userdata_balance['Adjust']), COIN_NAME)
                table_data.append([COIN_NAME, balance_actual, num_format_coin(userdata_balance['OpenOrder'], COIN_NAME)])
        else:
            table_data.append([COIN_NAME, "***", "***"])

    table = AsciiTable(table_data)
    # table.inner_column_border = False
    # table.outer_border = False
    table.padding_left = 0
    table.padding_right = 0
    if show_pub:
        msg = await ctx.send('**[ YOUR BALANCE LIST ]**\n'
                                            f'```{table.table}```')
    else:
        msg = await ctx.message.author.send('**[ YOUR BALANCE LIST ]**\n'
                                            f'```{table.table}```')
    return


@bot.command(pass_context=True, aliases=['markets'], help=bot_help_market)
async def market(ctx, coin: str = None):
    global ENABLE_COIN
    if coin is None:
        get_markets = await store.sql_get_open_order_by_alluser('ALL', 'OPEN')
        if get_markets and len(get_markets) > 0:
            table_data = [
                ['PAIR', 'Selling', 'For', 'Rate', 'Order #']
                ]
            list_numb = 0
            for order_item in get_markets:
                list_numb += 1
                if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                    table_data.append([order_item['pair_name'], num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                else:
                    table_data.append([order_item['pair_name']+"*", num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), order_item['order_id']])
                if list_numb > 15:
                    break
            table = AsciiTable(table_data)
            # table.inner_column_border = False
            # table.outer_border = False
            table.padding_left = 0
            table.padding_right = 0
            await ctx.send(f'**[ MARKET LIST ]**\n'
                                                f'```{table.table}```')
            return
        else:
            await ctx.send(f'{ctx.author.mention} Currently, no opening selling market. Please make some open order for others.')
            return
    else:
        # check if there is / or -
        coin_pair = None
        COIN_NAME = None
        get_markets = None
        coin = coin.upper()
        if "/" in coin:
            coin_pair = coin.split("/")
        elif "." in coin:
            coin_pair = coin.split(".")
        elif "-" in coin:
            coin_pair = coin.split(".")
        if coin_pair is None:
            COIN_NAME = coin.upper()
            if COIN_NAME not in ENABLE_COIN:
                await ctx.message.add_reaction(EMOJI_RED_NO)
                await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} in not in our list.')
                return
            else:
                get_markets = await store.sql_get_open_order_by_alluser(COIN_NAME, 'OPEN')
        elif coin_pair and len(coin_pair) == 2:
            if coin_pair[0] not in ENABLE_COIN:
                await ctx.send(f'{EMOJI_ERROR} **{coin_pair[0]}** is not in our list.')
                return
            elif coin_pair[1] not in ENABLE_COIN:
                await ctx.send(f'{EMOJI_ERROR} **{coin_pair[1]}** is not in our list.')
                return
            else:
                get_markets = await store.sql_get_open_order_by_alluser_by_coins(coin_pair[0], coin_pair[1], 'OPEN')
        if get_markets and len(get_markets) > 0:
            list_numb = 0
            table_data = [
                ['PAIR', 'Selling', 'For', 'Rate', 'Order #']
                ]
            for order_item in get_markets:
                list_numb += 1
                if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                    table_data.append([order_item['pair_name'], num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                else:
                    table_data.append([order_item['pair_name']+"*", num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                if list_numb > 15:
                    break
            table = AsciiTable(table_data)
            # table.inner_column_border = False
            # table.outer_border = False
            table.padding_left = 0
            table.padding_right = 0
            if coin_pair:
                title = "MARKET **{}/{}**".format(coin_pair[0], coin_pair[1])
            else:
                title = "MARKET **{}**".format(COIN_NAME)
            await ctx.send(f'[ {title} ]\n'
                           f'```{table.table}```')
            return
        else:
            if coin_pair is None:
                # get another buy of ticker
                get_markets = await store.sql_get_open_order_by_alluser(COIN_NAME, 'OPEN', need_to_buy = True)
                if get_markets and len(get_markets) > 0:
                    list_numb = 0
                    table_data = [
                        ['PAIR', 'Selling', 'For', 'Rate', 'Order #']
                        ]
                    for order_item in get_markets:
                        list_numb += 1
                        if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                            table_data.append([order_item['pair_name'], num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                              num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                              '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                              order_item['order_id']])
                        else:
                            table_data.append([order_item['pair_name']+"*", num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                              num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                              '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                              order_item['order_id']])
                        if list_numb > 15:
                            break
                    table = AsciiTable(table_data)
                    table.padding_left = 0
                    table.padding_right = 0
                    title = "MARKET **{}**".format(COIN_NAME)
                    await ctx.send(f'There is no selling for {COIN_NAME} but there are buy order of {COIN_NAME}.\n[ {title} ]\n'
                                   f'```{table.table}```')
                    return
                else:
                    await ctx.send(f'{ctx.author.mention} Currently, no opening selling or buying market for {COIN_NAME}. Please make some open order for others.')
            else:
                await ctx.send(f'{ctx.author.mention} Currently, no opening selling market pair for {coin}. Please make some open order for others.')
            return



@bot.command(pass_context=True, aliases=['buying'], help=bot_help_buy)
async def buy(ctx, ref_number: str):
    global IS_RESTARTING, ENABLE_COIN, NOTIFY_CHAN
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    # check if the argument is ref or ticker by length
    if len(ref_number) < 6:
        # assume it is ticker
        # ,buy trtl (example)
        COIN_NAME = ref_number.upper()
        if COIN_NAME not in ENABLE_COIN:
            await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in our list.')
            return
        
        # get list of all coin where they sell XXX
        get_markets = await store.sql_get_open_order_by_alluser_by_coins(COIN_NAME, 'ALL', 'OPEN')
        if get_markets and len(get_markets) > 0:
            list_numb = 0
            table_data = [
                ['PAIR', 'Selling', 'For', 'Rate', 'Order #']
                ]
            for order_item in get_markets:
                list_numb += 1
                if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                    table_data.append([order_item['pair_name'], num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                else:
                    table_data.append([order_item['pair_name']+"*", num_format_coin(order_item['amount_sell_after_fee'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                      '{:.8f}'.format(round(order_item['amount_sell']/order_item['amount_get']/get_decimal(order_item['coin_sell'])*get_decimal(order_item['coin_get']), 8)), 
                                      order_item['order_id']])
                if list_numb > 20:
                    break
            table = AsciiTable(table_data)
            # table.inner_column_border = False
            # table.outer_border = False
            table.padding_left = 0
            table.padding_right = 0
            title = "MARKET SELLING **{}**".format(COIN_NAME)
            await ctx.send(f'[ {title} ]\n'
                           f'```{table.table}```')
            return
        else:
            await ctx.send(f'{ctx.author.mention} Currently, no opening selling {COIN_NAME}. Please make some open order for others.')
            return
    else:
        # assume reference number
        get_order_num = await store.sql_get_order_numb(ref_number)
        if get_order_num:
            # check if own order
            if get_order_num['sell_user_server'] == "DISCORD" and ctx.message.author.id == int(get_order_num['userid_sell']):
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} #**{ref_number}** is your own selling order.')
                return
            else:
                # check if sufficient balance
                balance_actual = 0
                wallet = await store.sql_get_userwallet(str(ctx.message.author.id), get_order_num['coin_get'], 'DISCORD')
                if wallet is None:
                    userregister = await store.sql_register_user(str(ctx.message.author.id), get_order_num['coin_get'], 'DISCORD')
                    wallet = await store.sql_get_userwallet(str(ctx.message.author.id), get_order_num['coin_get'], 'DISCORD')
                if wallet:
                    actual = wallet['actual_balance']
                    userdata_balance = store.sql_user_balance(str(ctx.message.author.id), get_order_num['coin_get'], 'DISCORD')
                    balance_actual = actual + float(userdata_balance['Adjust'])
                if balance_actual < get_order_num['amount_get_after_fee']:
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    await ctx.send('{} {} You do not have sufficient balance.'
                                   '```Needed: {}{}\n'
                                   'Have:   {}{}```'.format(EMOJI_RED_NO, ctx.author.mention, 
                                                     num_format_coin(get_order_num['amount_get'], 
                                                     get_order_num['coin_get']), get_order_num['coin_get'],
                                                     num_format_coin(balance_actual, get_order_num['coin_get']), 
                                                     get_order_num['coin_get']))
                    return
                else:
                    # let's make order update
                    match_order = await store.sql_match_order_by_sellerid(str(ctx.message.author.id), ref_number, 'DISCORD')
                    if match_order:
                        await ctx.message.add_reaction(EMOJI_OK_BOX)
                        try:
                            await ctx.send('{} #**{}** Order completed!'
                                           '```'
                                           'Get: {}{}\n'
                                           'From selling: {}{}\n'
                                           'Fee: {}{}\n'
                                           '```'.format(ctx.author.mention, ref_number, num_format_coin(get_order_num['amount_sell_after_fee'], 
                                                        get_order_num['coin_sell']), get_order_num['coin_sell'], 
                                                        num_format_coin(get_order_num['amount_get_after_fee'], 
                                                        get_order_num['coin_get']), get_order_num['coin_get'],
                                                        num_format_coin(get_order_num['amount_get']-get_order_num['amount_get_after_fee'], 
                                                        get_order_num['coin_get']), get_order_num['coin_get']))
                            try:
                                sold = num_format_coin(get_order_num['amount_sell'], get_order_num['coin_sell']) + get_order_num['coin_sell']
                                bought = num_format_coin(get_order_num['amount_get_after_fee'], get_order_num['coin_get']) + get_order_num['coin_get']
                                fee = str(num_format_coin(get_order_num['amount_get']-get_order_num['amount_get_after_fee'], get_order_num['coin_get']))
                                fee += get_order_num['coin_get']
                                if get_order_num['sell_user_server'] == "DISCORD":
                                    member = bot.get_user(id=int(get_order_num['userid_sell']))
                                    if member:
                                        try:
                                            await member.send(f'A user has bought #**{ref_number}**\n```Sold: {sold}\nGet: {bought}```')
                                        except (discord.Forbidden, discord.errors.Forbidden) as e:
                                            pass
                                # add message to trade channel as well.
                                if ctx.message.channel.id != NOTIFY_CHAN or isinstance(ctx.message.channel, discord.DMChannel) == True:
                                    botLogChan = bot.get_channel(id=NOTIFY_CHAN)
                                    await botLogChan.send(f'A user has bought #**{ref_number}**\n```Sold: {sold}\nGet: {bought}\nFee: {fee}```')
                            except (discord.Forbidden, discord.errors.Forbidden) as e:
                                pass
                        except (discord.Forbidden, discord.errors.Forbidden) as e:
                            pass
                        return
                    else:
                        await ctx.message.add_reaction(EMOJI_ERROR)
                        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **{ref_number}** internal error, please report.')
                        return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} #**{ref_number}** does not exist or already completed.')
            return


@bot.command(pass_context=True, help=bot_help_cancel)
async def cancel(ctx, order_num: str = 'ALL'):
    global ENABLE_COIN
    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    if order_num.upper() == 'ALL':
        get_open_order = await store.sql_get_open_order_by_sellerid_all(str(ctx.message.author.id), 'OPEN')
        if len(get_open_order) == 0:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You do not have any open order.')
            return
        else:
            cancel_order = await store.sql_cancel_open_order_by_sellerid(str(ctx.message.author.id), 'ALL')
            await ctx.message.add_reaction(EMOJI_OK_BOX)
            await ctx.send(f'{ctx.author.mention} You have cancelled all opened order(s).')
            return
    else:
        if len(order_num) < 6:
            # use coin name
            COIN_NAME = order_num.upper()
            if COIN_NAME not in ENABLE_COIN:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **{COIN_NAME}** is not valid.')
                return
            else:
                get_open_order = await store.sql_get_open_order_by_sellerid(str(ctx.message.author.id), COIN_NAME, 'OPEN')
                if len(get_open_order) == 0:
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'{ctx.author.mention} You do not have any open order for **{COIN_NAME}**.')
                    return
                else:
                    cancel_order = await store.sql_cancel_open_order_by_sellerid(str(ctx.message.author.id), COIN_NAME)
                    await ctx.message.add_reaction(EMOJI_OK_BOX)
                    await ctx.send(f'{ctx.author.mention} You have cancelled all opened sell(s) for **{COIN_NAME}**.')
                    return
        else:
            # open order number
            get_open_order = await store.sql_get_open_order_by_sellerid_all(str(ctx.message.author.id), 'OPEN')
            if len(get_open_order) == 0:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You do not have any open order.')
                return
            else:
                cancelled = False
                for open_order_list in get_open_order:
                    if order_num == str(open_order_list['order_id']):
                        cancel_order = await store.sql_cancel_open_order_by_sellerid(str(ctx.message.author.id), order_num) 
                        if cancel_order: cancelled = True
                if cancelled == False:
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You do not have sell #**{order_num}**. Please check command `myorder`')
                    return
                else:
                    await ctx.message.add_reaction(EMOJI_OK_BOX)
                    await ctx.send(f'{ctx.author.mention} You cancelled #**{order_num}**.')
                    return

@bot.command(pass_context=True, aliases=['myorders'], help=bot_help_myorder)
async def myorder(ctx, ticker: str = None):
    global ENABLE_COIN
    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    if ticker:
        if len(ticker) < 6:
            # assume it is a coin
            COIN_NAME = ticker.upper()
            if COIN_NAME not in ENABLE_COIN:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid ticker **{COIN_NAME}**.')
                return
            else:
                get_open_order = await store.sql_get_open_order_by_sellerid(str(ctx.message.author.id), COIN_NAME, 'OPEN')
                if get_open_order and len(get_open_order) > 0:
                    table_data = [
                        ['PAIR', 'Selling', 'For', 'Order #']
                        ]
                    for order_item in get_open_order:
                        if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                            table_data.append([order_item['pair_name'], num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'],
                                              num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                              order_item['order_id']])
                        else:
                            table_data.append([order_item['pair_name']+"*", num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'],
                                              num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], 
                                              order_item['order_id']])
                    table = AsciiTable(table_data)
                    # table.inner_column_border = False
                    # table.outer_border = False
                    table.padding_left = 0
                    table.padding_right = 0
                    msg = await ctx.message.author.send(f'**[ OPEN SELLING LIST {COIN_NAME}]**\n'
                                                        f'```{table.table}```')
                    
                    return
                else:
                    await ctx.send(f'{ctx.author.mention} You do not have any active selling of **{COIN_NAME}**.')
                    return
        else:
            # assume this is reference number
            try:
                ref_number = int(ticker)
                ref_number = str(ref_number)
            except ValueError:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid # number.')
                return
            get_order_num = await store.sql_get_order_numb(ref_number)
            if get_order_num:
                # check if own order
                response_text = "```"
                response_text += "Order #: " + ref_number + "\n"
                response_text += "Sell (After Fee): " + num_format_coin(get_order_num['amount_sell_after_fee'], get_order_num['coin_sell'])+get_order_num['coin_sell'] + "\n"
                response_text += "For (After Fee): " + num_format_coin(get_order_num['amount_get_after_fee'], get_order_num['coin_get'])+get_order_num['coin_get'] + "\n"
                if get_order_num['status'] == "COMPLETE":
                    response_text = response_text.replace("Sell", "Sold")
                    response_text += "Status: COMPLETED"
                elif get_order_num['status'] == "OPEN":
                    response_text += "Status: OPENED"
                elif get_order_num['status'] == "CANCEL":
                    response_text += "Status: CANCELLED"
                response_text += "```"

                if get_order_num['sell_user_server'] == "DISCORD" and ctx.message.author.id == int(get_order_num['userid_sell']):
                    # if he is the seller
                    response_text = response_text.replace("Sell", "You sell")
                    response_text = response_text.replace("Sold", "You sold")
                if get_order_num['sell_user_server'] and get_order_num['sell_user_server'] == "DISCORD" and \
                    'userid_get' in get_order_num and (ctx.message.author.id == int(get_order_num['userid_get'] if get_order_num['userid_get'] else 0)):
                    # if he bought this
                    response_text = response_text.replace("Sold", "You bought: ")
                    response_text = response_text.replace("For (After Fee):", "From selling (After Fee): ")
                await ctx.send(f'{ctx.author.mention} {response_text}')
                return
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} I could not find #**{ref_number}**.')
            return
    else:
        get_open_order = await store.sql_get_open_order_by_sellerid_all(str(ctx.message.author.id), 'OPEN')
        if get_open_order and len(get_open_order) > 0:
            table_data = [
                ['PAIR', 'Selling', 'For', 'Order #']
                ]
            for order_item in get_open_order:
                if is_tradeable_coin(order_item['coin_get']) and is_tradeable_coin(order_item['coin_sell']):
                    table_data.append([order_item['pair_name'], num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], order_item['order_id']])
                else:
                    table_data.append([order_item['pair_name']+"*", num_format_coin(order_item['amount_sell'], order_item['coin_sell'])+order_item['coin_sell'],
                                      num_format_coin(order_item['amount_get_after_fee'], order_item['coin_get'])+order_item['coin_get'], order_item['order_id']])
            table = AsciiTable(table_data)
            # table.inner_column_border = False
            # table.outer_border = False
            table.padding_left = 0
            table.padding_right = 0
            msg = await ctx.message.author.send(f'**[ OPEN SELLING LIST ]**\n'
                                                f'```{table.table}```')
            
            return
        else:
            await ctx.send(f'{ctx.author.mention} You do not have any active selling.')
            return


@bot.command(pass_context=True, aliases=['order_num'], help=bot_help_order_num)
async def order(ctx, order_num: str):
    global ENABLE_COIN
    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    # assume this is reference number
    try:
        ref_number = int(order_num)
        ref_number = str(ref_number)
    except ValueError:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid # number.')
        return
    get_order_num = await store.sql_get_order_numb(ref_number, 'ANY')
    if get_order_num:
        # check if own order
        response_text = "```"
        response_text += "Order #: " + ref_number + "\n"
        response_text += "Sell (After Fee): " + num_format_coin(get_order_num['amount_sell_after_fee'], get_order_num['coin_sell'])+get_order_num['coin_sell'] + "\n"
        response_text += "For (After Fee): " + num_format_coin(get_order_num['amount_get_after_fee'], get_order_num['coin_get'])+get_order_num['coin_get'] + "\n"
        if get_order_num['status'] == "COMPLETE":
            response_text = response_text.replace("Sell", "Sold")
            response_text += "Status: COMPLETED"
        elif get_order_num['status'] == "OPEN":
            response_text += "Status: OPENED"
        elif get_order_num['status'] == "CANCEL":
            response_text += "Status: CANCELLED"
        response_text += "```"

        if get_order_num['sell_user_server'] == "DISCORD" and ctx.message.author.id == int(get_order_num['userid_sell']):
            # if he is the seller
            response_text = response_text.replace("Sell", "You sell")
            response_text = response_text.replace("Sold", "You sold")
        if get_order_num['buy_user_server'] and get_order_num['buy_user_server'] == "DISCORD" \
        and 'userid_get' in get_order_num and (ctx.message.author.id == int(get_order_num['userid_get'] if get_order_num['userid_get'] else 0)):
            # if he bought this
            response_text = response_text.replace("Sold", "You bought: ")
            response_text = response_text.replace("For (After Fee):", "From selling (After Fee): ")
        await ctx.send(f'{ctx.author.mention} {response_text}')
        return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} I could not find #**{ref_number}**.')
    return


@bot.command(pass_context=True, aliases=['selling'], help=bot_help_sell)
async def sell(ctx, sell_amount: str, sell_ticker: str, buy_amount: str, buy_ticker: str):
    global IS_RESTARTING, ENABLE_COIN, NOTIFY_CHAN
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    sell_ticker = sell_ticker.upper()
    buy_ticker = buy_ticker.upper()
    sell_amount = sell_amount.replace(",", "")
    buy_amount = buy_amount.replace(",", "")
    try:
        sell_amount = float(sell_amount)
        buy_amount = float(buy_amount)
    except ValueError:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid sell/buy amount.')
        return
    if (sell_ticker not in ENABLE_COIN) or (buy_ticker not in ENABLE_COIN):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid trade ticker (buy/sell).')
        return

    if not is_tradeable_coin(sell_ticker):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        await ctx.send(f'{EMOJI_RED_NO} {sell_ticker} trading is currently disable.')
        return

    if not is_tradeable_coin(buy_ticker):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        await ctx.send(f'{EMOJI_RED_NO} {buy_ticker} trading is currently disable.')
        return

    if buy_ticker == sell_ticker:
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        await ctx.send(f'{EMOJI_RED_NO} {buy_ticker} you cannot trade the same coins.')
        return

    # get opened order:
    user_count_order = await store.sql_count_open_order_by_sellerid(str(ctx.message.author.id), 'DISCORD')
    if user_count_order >= config.Max_Open_Order:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You have maximum opened selling **{config.Max_Open_Order}**. Please cancel some or wait.')
        return
    
    COIN_DEC_SELL = get_decimal(sell_ticker)
    real_amount_sell = int(sell_amount * COIN_DEC_SELL) if sell_ticker not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else sell_amount
    if real_amount_sell == 0:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {sell_amount}{sell_ticker} = 0 {sell_ticker} (below smallest unit).')
        return

    if real_amount_sell < get_min_sell(sell_ticker):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **{sell_amount}{sell_ticker}** below minimum trade **{num_format_coin(get_min_sell(sell_ticker), sell_ticker)}{sell_ticker}**.')
        return
    if real_amount_sell > get_max_sell(sell_ticker):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **{sell_amount}{sell_ticker}** above maximum trade **{num_format_coin(get_max_sell(sell_ticker), sell_ticker)}{sell_ticker}**.')
        return

    COIN_DEC_BUY = get_decimal(buy_ticker)
    real_amount_buy = int(buy_amount * COIN_DEC_BUY) if buy_ticker not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else buy_amount
    if real_amount_buy == 0:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {buy_amount}{buy_ticker} = 0 {buy_ticker} (below smallest unit).')
        return
    if real_amount_buy < get_min_sell(buy_ticker):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **{buy_amount}{buy_ticker}** below minimum trade **{num_format_coin(get_min_sell(buy_ticker), buy_ticker)}{buy_ticker}**.')
        return
    if real_amount_buy > get_max_sell(buy_ticker):
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} **{buy_amount}{buy_ticker}** above maximum trade **{num_format_coin(get_max_sell(buy_ticker), buy_ticker)}{buy_ticker}**.')
        return

    if not is_maintenance_coin(sell_ticker):
        balance_actual = 0
        wallet = await store.sql_get_userwallet(str(ctx.message.author.id), sell_ticker, 'DISCORD')
        if wallet is None:
            userregister = await store.sql_register_user(str(ctx.message.author.id), sell_ticker, 'DISCORD')
            wallet = await store.sql_get_userwallet(str(ctx.message.author.id), sell_ticker, 'DISCORD')
        if wallet:
            userdata_balance = store.sql_user_balance(str(ctx.message.author.id), sell_ticker, 'DISCORD')
            balance_actual = wallet['actual_balance'] + float(userdata_balance['Adjust'])
        balance_actual = int(balance_actual) if sell_ticker not in ["DOGE", "LTC", "BTC", "DASH", "BCH"] else balance_actual
        if balance_actual < real_amount_sell:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You do not have enough '
                           f'**{sell_ticker}**. You have currently: {num_format_coin(balance_actual, sell_ticker)}{sell_ticker}.')
            return
        if (sell_amount / buy_amount) < MIN_RATIO or (buy_amount / sell_amount) < MIN_RATIO:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} ratio buy/sell rate is so low.')
            return
        # call other function
        return await sell_process(ctx, real_amount_sell, sell_ticker, real_amount_buy, buy_ticker)


async def sell_process(ctx, real_amount_sell: float, sell_ticker: str, real_amount_buy: float, buy_ticker: str):
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
    check_if_same_rate = await store.sql_get_order_by_sellerid_pair_rate('DISCORD', str(ctx.message.author.id), sell_ticker, 
                         buy_ticker, sell_div_get, real_amount_sell, real_amount_buy, fee_sell, fee_buy, 'OPEN')
    if check_if_same_rate and check_if_same_rate['error'] == True and check_if_same_rate['msg']:
        get_message = check_if_same_rate['msg']
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {get_message}')
        return
    elif check_if_same_rate and check_if_same_rate['error'] == False:
        get_message = check_if_same_rate['msg']
        await ctx.message.add_reaction(EMOJI_OK_BOX)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {get_message}')
        return

    order_add = await store.sql_store_openorder(str(ctx.message.id), (ctx.message.content)[:120], sell_ticker, 
                            real_amount_sell, real_amount_sell-fee_sell, str(ctx.message.author.id), 
                            buy_ticker, real_amount_buy, real_amount_buy-fee_buy, sell_div_get, 'DISCORD')
    if order_add:
        get_message = "New open order created: #**{}**```Selling: {}{}\nFor: {}{}\nFee: {}{}```".format(order_add, 
                                                                        num_format_coin(real_amount_sell, sell_ticker), sell_ticker,
                                                                        num_format_coin(real_amount_buy, buy_ticker), buy_ticker,
                                                                        num_format_coin(fee_sell, sell_ticker), sell_ticker)
        await ctx.message.add_reaction(EMOJI_OK_BOX)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} {get_message}')
        # add message to trade channel as well.
        if ctx.message.channel.id != NOTIFY_CHAN or isinstance(ctx.message.channel, discord.DMChannel) == True:
            botLogChan = bot.get_channel(id=NOTIFY_CHAN)
            await botLogChan.send(get_message)
        return


@bot.command(pass_context=True, help=bot_help_send)
async def send(ctx, amount: str, ticker: str, CoinAddress: str):
    global IS_RESTARTING, WITHDRAW_IN_PROCESS, ENABLE_COIN
    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    if ctx.message.author.id in WITHDRAW_IN_PROCESS:
        # reject and tell to wait
        msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You have another tx in process. Please wait it to finish. ')
        return

    amount = amount.replace(",", "")
    try:
        amount = float(amount)
    except ValueError:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid amount.')
        return

    # Check which coinname is it.
    COIN_NAME = get_cn_coin_from_address(CoinAddress)
    if not is_withdrawable_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} withdraw is currently disable.')
        return
    coin_family = None

    if ticker.upper() != COIN_NAME:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} It seemed I can not catch `{CoinAddress}` as **{ticker.upper()}**.')
        return

    if COIN_NAME:
        coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        try:
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} could not find what address it is.')
        except (discord.Forbidden, discord.errors.Forbidden) as e:
            try:
                await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} could not find what address it is.')
            except (discord.Forbidden, discord.errors.Forbidden) as e:
                return
        return

    # add redis action
    random_string = str(uuid.uuid4())
    await add_tx_action_redis(json.dumps([random_string, "SEND", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "START"]), False)

    COIN_DEC = get_decimal(COIN_NAME)
    MinTx = get_min_tx_amount(COIN_NAME)
    MaxTX = get_max_tx_amount(COIN_NAME)
    real_amount = int(amount * COIN_DEC) if (coin_family == "TRTL" or coin_family == "XMR") else amount
    NetFee = get_reserved_fee(COIN_NAME)

    if coin_family == "TRTL" or coin_family == "CCX":
        addressLength = get_addrlen(COIN_NAME)
        IntaddressLength = get_intaddrlen(COIN_NAME)
        print('{} - {} - {}'.format(COIN_NAME, addressLength, IntaddressLength))
        if len(CoinAddress) == int(addressLength):
            valid_address = addressvalidation.validate_address_cn(CoinAddress, COIN_NAME)
            # print(valid_address)
            if valid_address != CoinAddress:
                valid_address = None

            if valid_address is None:
                await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
                try:
                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                                   f'`{CoinAddress}`')
                except (discord.Forbidden, discord.errors.Forbidden) as e:
                    try:
                        await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                                                      f'`{CoinAddress}`')
                    except (discord.Forbidden, discord.errors.Forbidden) as e:
                        return
                return
        elif len(CoinAddress) == int(IntaddressLength):
            valid_address = addressvalidation.validate_integrated_cn(CoinAddress, COIN_NAME)
            # print(valid_address)
            if valid_address == 'invalid':
                await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
                try:
                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid integrated address:\n'
                                   f'`{CoinAddress}`')
                except (discord.Forbidden, discord.errors.Forbidden) as e:
                    try:
                        await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid integrated address:\n'
                                                      f'`{CoinAddress}`')
                    except (discord.Forbidden, discord.errors.Forbidden) as e:
                        return
                return
            if len(valid_address) == 2:
                iCoinAddress = CoinAddress
                CoinAddress = valid_address['address']
                paymentid = valid_address['integrated_id']
        elif len(CoinAddress) == int(addressLength) + 64 + 1:
            valid_address = {}
            check_address = CoinAddress.split(".")
            if len(check_address) != 2:
                await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
                try:
                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid {COIN_NAME} address + paymentid')
                except (discord.Forbidden, discord.errors.Forbidden) as e:
                    try:
                        await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid {COIN_NAME} address + paymentid')
                    except (discord.Forbidden, discord.errors.Forbidden) as e:
                        return
                return
            else:
                valid_address_str = addressvalidation.validate_address_cn(check_address[0], COIN_NAME)
                paymentid = check_address[1].strip()
                if valid_address_str is None:
                    await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
                    try:
                        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                                       f'`{check_address[0]}`')
                    except (discord.Forbidden, discord.errors.Forbidden) as e:
                        try:
                            await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                                                          f'`{check_address[0]}`')
                        except (discord.Forbidden, discord.errors.Forbidden) as e:
                            return
                    return
                else:
                    valid_address['address'] = valid_address_str
            # Check payment ID
                if len(paymentid) == 64:
                    if not re.match(r'[a-zA-Z0-9]{64,}', paymentid.strip()):
                        await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
                        try:
                            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} PaymentID: `{paymentid}`\n'
                                            'Should be in 64 correct format.')
                        except (discord.Forbidden, discord.errors.Forbidden) as e:
                            try:
                                await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} PaymentID: `{paymentid}`\n'
                                                              'Should be in 64 correct format.')
                            except (discord.Forbidden, discord.errors.Forbidden) as e:
                                return
                        return
                    else:
                        CoinAddress = valid_address['address']
                        valid_address['paymentid'] = paymentid
                        iCoinAddress = addressvalidation.make_integrated_cn(valid_address['address'], COIN_NAME, paymentid)['integrated_address']
                        pass
                else:
                    await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
                    try:
                        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} PaymentID: `{paymentid}`\n'
                                        'Incorrect length')
                    except (discord.Forbidden, discord.errors.Forbidden) as e:
                        try:
                            await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} PaymentID: `{paymentid}`\n'
                                                         'Incorrect length')
                        except (discord.Forbidden, discord.errors.Forbidden) as e:
                            return
                    return
        else:
            await ctx.message.add_reaction(EMOJI_QUESTEXCLAIM)
            try:
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                               f'`{CoinAddress}`')
            except (discord.Forbidden, discord.errors.Forbidden) as e:
                try:
                    await ctx.message.author.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid address:\n'
                                                  f'`{CoinAddress}`')
                except (discord.Forbidden, discord.errors.Forbidden) as e:
                    return
            return
        # Check available balance
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        if user_from is None:
            userregister = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
            user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')

        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        user_from['actual_balance'] = user_from['actual_balance'] + float(userdata_balance['Adjust'])

        if real_amount + NetFee > user_from['actual_balance']:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient reserved fee to send '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME} to {CoinAddress}.')

            return

        if real_amount > MaxTX:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transactions cannot be bigger than '
                           f'{num_format_coin(MaxTX, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        elif real_amount < MinTx:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transactions cannot be smaller than '
                           f'{num_format_coin(MinTx, COIN_NAME)} '
                           f'{COIN_NAME}.')

            return

        if len(valid_address) == 2:
            sending = None
            try:
                sending = await store.sql_external_cn_xmr_single('DISCORD', str(ctx.message.author.id), real_amount, CoinAddress, COIN_NAME, paymentid)
                tip_tx_tipper = "Transaction hash: `{}`".format(sending['transactionHash'])
                tip_tx_tipper += "\nTx Fee: `{}{}`".format(num_format_coin(sending['fee'], COIN_NAME), COIN_NAME)
                await add_tx_action_redis(json.dumps([random_string, "SEND", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "COMPLETE"]), False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            if sending:
                await ctx.message.add_reaction(EMOJI_OK_BOX)
                await ctx.message.author.send(
                                       f'{EMOJI_ARROW_RIGHTHOOK} You have sent {num_format_coin(real_amount, COIN_NAME)} '
                                       f'{COIN_NAME} '
                                       f'to `{iCoinAddress}`\n\n'
                                       f'Address: `{CoinAddress}`\n'
                                       f'Payment ID: `{paymentid}`\n'
                                       f'{tip_tx_tipper}')
                return
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                msg = await ctx.send(f'{ctx.author.mention} You may need to `optimize` or retry.')
                
                return
        else:
            sending = None
            try:
                sending = await store.sql_external_cn_xmr_single('DISCORD', str(ctx.message.author.id), real_amount, CoinAddress, COIN_NAME)
                tip_tx_tipper = "Transaction hash: `{}`".format(sending['transactionHash'])
                tip_tx_tipper += "\nTx Fee: `{}{}`".format(num_format_coin(sending['fee'], COIN_NAME), COIN_NAME)
                await add_tx_action_redis(json.dumps([random_string, "SEND", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "COMPLETE"]), False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            if sending:
                await ctx.message.add_reaction(EMOJI_OK_BOX)
                await ctx.message.author.send(f'{EMOJI_ARROW_RIGHTHOOK} You have sent {num_format_coin(real_amount, COIN_NAME)} '
                                              f'{COIN_NAME} '
                                              f'to `{CoinAddress}`\n'
                                              f'{tip_tx_tipper}')
                return
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{ctx.author.mention} Can not deliver TX for {COIN_NAME} right now. Try again soon.')
                return
    elif coin_family == "XMR":
        addressLength = get_addrlen(COIN_NAME)
        IntaddressLength = get_intaddrlen(COIN_NAME)

        # If not Masari
        if COIN_NAME != "MSR":
            valid_address = await validate_address_xmr(str(CoinAddress), COIN_NAME)
            if valid_address['valid'] == False or valid_address['nettype'] != 'mainnet':
                    await ctx.message.add_reaction(EMOJI_ERROR)
                    await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Address: `{CoinAddress}` '
                                   'is invalid.')
                    return
        # OK valid address
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')

        # If balance 0, no need to check anything
        if float(user_from['actual_balance']) + float(userdata_balance['Adjust']) <= 0:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Please check your **{COIN_NAME}** balance.')
            return
        if real_amount > float(user_from['actual_balance']) + float(userdata_balance['Adjust']):
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to send out '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return

        NetFee = await get_tx_fee_xmr(coin = COIN_NAME, amount = real_amount, to_address = CoinAddress)
        if NetFee is None:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Can not get fee from network for: '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}. Please try again later in a few minutes.')
            return
        if real_amount + NetFee > float(user_from['actual_balance']) + float(userdata_balance['Adjust']):
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to send out '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}. You need to leave at least network fee: {num_format_coin(NetFee, COIN_NAME)}{COIN_NAME}')
            return
        if real_amount < MinTx:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be smaller than '
                           f'{num_format_coin(MinTx, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        if real_amount > MaxTX:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be bigger than '
                           f'{num_format_coin(MaxTX, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return

        SendTx = None
        if ctx.message.author.id not in WITHDRAW_IN_PROCESS:
            WITHDRAW_IN_PROCESS.append(ctx.message.author.id)
            try:
                SendTx = await store.sql_external_cn_xmr_single('DISCORD', str(ctx.message.author.id), real_amount,
                                                                CoinAddress, COIN_NAME)
                await add_tx_action_redis(json.dumps([random_string, "SEND", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "COMPLETE"]), False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            WITHDRAW_IN_PROCESS.remove(ctx.message.author.id)
        else:
            # reject and tell to wait
            msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You have another tx in process. Please wait it to finish. ')
            
            return
        if SendTx:
            SendTx_hash = SendTx['tx_hash']
            await ctx.message.add_reaction(EMOJI_OK_BOX)
            await ctx.message.author.send(f'{EMOJI_ARROW_RIGHTHOOK} You have sent {num_format_coin(real_amount, COIN_NAME)} '
                                          f'{COIN_NAME} to `{CoinAddress}`.\n'
                                          f'Transaction hash: `{SendTx_hash}`\n'
                                          'Network fee deducted from your account balance.')
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            return
        return
    elif coin_family == "DOGE":
        valid_address = await doge_validaddress(str(CoinAddress), COIN_NAME)
        if 'isvalid' in valid_address:
            if str(valid_address['isvalid']) == "True":
                pass
            else:
                await ctx.message.add_reaction(EMOJI_ERROR)
                await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Address: `{CoinAddress}` '
                               'is invalid.')
                return

        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')        

        real_amount = float(amount)
        userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        if real_amount + NetFee > float(user_from['actual_balance']) + float(userdata_balance['Adjust']):
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to send out '
                           f'{num_format_coin(real_amount, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        if real_amount < MinTx:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be smaller than '
                           f'{num_format_coin(MinTx, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        if real_amount > MaxTX:
            await ctx.message.add_reaction(EMOJI_ERROR)
            await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be bigger than '
                           f'{num_format_coin(MaxTX, COIN_NAME)} '
                           f'{COIN_NAME}.')
            return
        SendTx = None
        if ctx.message.author.id not in WITHDRAW_IN_PROCESS:
            WITHDRAW_IN_PROCESS.append(ctx.message.author.id)
            try:
                SendTx = await store.sql_external_doge('DISCORD', str(ctx.message.author.id), real_amount, NetFee,
                                                       CoinAddress, COIN_NAME)
                await add_tx_action_redis(json.dumps([random_string, "SEND", str(ctx.message.author.id), ctx.message.author.name, float("%.3f" % time.time()), ctx.message.content, "DISCORD", "COMPLETE"]), False)
            except Exception as e:
                traceback.print_exc(file=sys.stdout)
            WITHDRAW_IN_PROCESS.remove(ctx.message.author.id)
        else:
            msg = await ctx.send(f'{EMOJI_ERROR} {ctx.author.mention} You have another tx in progress.')
            await msg.add_reaction(EMOJI_OK_BOX)
            return
        if SendTx:
            await ctx.message.author.send(f'{EMOJI_ARROW_RIGHTHOOK} You have sent {num_format_coin(real_amount, COIN_NAME)} '
                                          f'{COIN_NAME} to `{CoinAddress}`.\n'
                                          f'Transaction hash: `{SendTx}`\n'
                                          'Network fee deducted from the amount.')
            return
        else:
            await ctx.message.add_reaction(EMOJI_ERROR)
            return


@bot.command(pass_context=True, help=bot_help_swap)
async def swap(ctx, amount: str, coin: str, to: str):
    global IS_RESTARTING, WITHDRAW_IN_PROCESS
    to = to.upper()
    if to != "TIPBOT":
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} supporting to **TIPBOT** only right now.')
        return

    # check if bot is going to restart
    if IS_RESTARTING:
        await ctx.message.add_reaction(EMOJI_REFRESH)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Bot is going to restart soon. Wait until it is back for using this.')
        return

    if not is_testing(ctx):
        return # to delete after test
        await ctx.send(f'{ctx.author.mention} Wait, we are still testing it.')
        return

    if ctx.message.author.id in WITHDRAW_IN_PROCESS:
        # reject and tell to wait
        msg = await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} You have another tx in process. Please wait it to finish. ')
        return

    amount = amount.replace(",", "")
    try:
        amount = float(amount)
    except ValueError:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Invalid amount.')
        return

    COIN_NAME = coin.upper()
    if COIN_NAME not in ENABLE_SWAP:
        await ctx.send(f'{EMOJI_ERROR} **{COIN_NAME}** is not in swap list.')
        return

    if is_maintenance_coin(COIN_NAME):
        await ctx.message.add_reaction(EMOJI_MAINTENANCE)
        if show_pub:
            await ctx.send(f'{EMOJI_RED_NO} {COIN_NAME} in maintenance.')
        else:
            await ctx.message.author.send(f'{EMOJI_RED_NO} {COIN_NAME} in maintenance.')
        return

    coin_family = getattr(getattr(config,"coin"+COIN_NAME),"coin_family","TRTL")

    user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
    if user_from is None:
        user_reg = await store.sql_register_user(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
        user_from = await store.sql_get_userwallet(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
    COIN_DEC = get_decimal(COIN_NAME)
    real_amount = int(amount * COIN_DEC) if coin_family in ["TRTL", "XMR"] else amount
    MinTx = get_min_tx_amount(COIN_NAME)
    MaxTX = get_max_tx_amount(COIN_NAME)

    userdata_balance = store.sql_user_balance(str(ctx.message.author.id), COIN_NAME, 'DISCORD')
    user_from['actual_balance'] = user_from['actual_balance'] + float(userdata_balance['Adjust'])

    if real_amount > user_from['actual_balance']:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Insufficient balance to swap '
                       f'{num_format_coin(real_amount, COIN_NAME)} '
                       f'{COIN_NAME} to {to.upper()}.')
        return

    if real_amount > MaxTX:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be bigger than '
                       f'{num_format_coin(MaxTX, COIN_NAME)} '
                       f'{COIN_NAME}.')
        return
    elif real_amount < MinTx:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Transaction cannot be smaller than '
                       f'{num_format_coin(MinTx, COIN_NAME)} '
                       f'{COIN_NAME}.')
        return

    swapit = None
    try:
        swapit = await store.sql_swap_balance(COIN_NAME, str(ctx.message.author.id), ctx.message.author.name, 'MARKETBOT', to.upper(), real_amount)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
    if swapit:
        await ctx.message.add_reaction(EMOJI_OK_BOX)
        await ctx.message.author.send(
                f'{EMOJI_ARROW_RIGHTHOOK} You swap {num_format_coin(real_amount, COIN_NAME)} '
                f'{COIN_NAME} to **{to.upper()}**.')
        return
    else:
        await ctx.message.add_reaction(EMOJI_ERROR)
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Internal error during swap.')
        return


@bot.command(pass_context=True, name='donate', help=bot_help_donate)
async def donate(ctx):
    invite_link = "https://discordapp.com/oauth2/authorize?client_id="+str(bot.user.id)+"&scope=bot"
    donatelist = discord.Embed(title='Support Me', description='', colour=7047495)
    donatelist.add_field(name='BTC:', value=config.donate.btc, inline=False)
    donatelist.add_field(name='LTC:', value=config.donate.ltc, inline=False)
    donatelist.add_field(name='DOGE:', value=config.donate.doge, inline=False)
    donatelist.add_field(name='BCH:', value=config.donate.bch, inline=False)
    donatelist.add_field(name='DASH:', value=config.donate.dash, inline=False)
    donatelist.add_field(name='XMR:', value=config.donate.xmr, inline=False)
    donatelist.add_field(name='WRKZ:', value=config.donate.wrkz, inline=False)
    donatelist.set_author(name=bot.user.name, icon_url=bot.user.avatar_url)
    try:
        await ctx.send(embed=donatelist)
        return
    except Exception as e:
        await ctx.message.author.send(embed=donatelist)
        traceback.print_exc(file=sys.stdout)


@bot.command(pass_context=True, name='invite', aliases=['inviteme'], help=bot_help_invite)
async def invite(ctx):
    invite_link = "https://discordapp.com/oauth2/authorize?client_id="+str(bot.user.id)+"&scope=bot"
    await ctx.send('**[INVITE LINK]**\n\n'
                f'{invite_link}')


@bot.command(pass_context=True, name='about', help=bot_help_about)
async def about(ctx):
    invite_link = "https://discordapp.com/oauth2/authorize?client_id="+str(bot.user.id)+"&scope=bot"
    botdetails = discord.Embed(title='About Me', description='', colour=7047495)
    botdetails.add_field(name='My Github:', value='https://github.com/wrkzcoin/MarketXBot', inline=False)
    botdetails.add_field(name='Invite Me:', value=f'{invite_link}', inline=False)
    botdetails.add_field(name='Servers I am in:', value=len(bot.guilds), inline=False)
    botdetails.add_field(name='Supported by:', value='WrkzCoin Community Team', inline=False)
    botdetails.add_field(name='Supported Server:', value='https://chat.wrkz.work', inline=False)
    botdetails.set_footer(text='Made in Python3.6+ with discord.py library!', icon_url='http://findicons.com/files/icons/2804/plex/512/python.png')
    botdetails.set_author(name=bot.user.name, icon_url=bot.user.avatar_url)
    try:
        await ctx.send(embed=botdetails)
    except Exception as e:
        await ctx.message.author.send(embed=botdetails)
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


def set_depositable_coin(coin: str, set_deposit: bool = True):
    global redis_conn, redis_expired 
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_DEPOSIT'
        if set_deposit == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
            return True
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


def set_withdrawable_coin(coin: str, set_withdraw: bool = True):
    global redis_conn, redis_expired 
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_WITHDRAW'
        if set_withdraw == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
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


def set_tradeable_coin(coin: str, set_trade: bool = True):
    global redis_conn, redis_expired 
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_TRADEABLE'
        if set_trade == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_maintenance_coin(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_MAINT'
        if redis_conn and redis_conn.exists(key):
            return True
        else:
            return False
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def set_maintenance_coin(coin: str, set_maint: bool = True):
    global redis_conn, redis_expired 
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_MAINT'
        if set_maint == True:
            if redis_conn and redis_conn.exists(key):
                return True
            else:
                redis_conn.set(key, "ON")
                return True
        else:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_coin_sendable(coin: str):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()
    if is_maintenance_coin(COIN_NAME):
        return False
    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_TX'
        if redis_conn and redis_conn.exists(key):
            return False
        else:
            return True
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


def is_testing(ctx):
    if IS_TESTING:
        if ctx.message.author.id in TESTER:
            return True
        else:
            return False
    else:
        return True

def set_coin_sendable(coin: str, set_sendable: bool = True):
    global redis_conn, redis_expired
    COIN_NAME = coin.upper()

    # Check if exist in redis
    try:
        openRedis()
        key = 'MarketBOT:COIN_' + COIN_NAME + '_TX'
        if set_sendable == True:
            if redis_conn and redis_conn.exists(key):
                redis_conn.delete(key)
                return True
        else:
            if redis_conn and not redis_conn.exists(key):
                redis_conn.set(key, "ON")                
    except Exception as e:
        traceback.print_exc(file=sys.stdout)


# Let's run balance update by a separate process
async def update_balance():
    INTERVAL_EACH = 10
    while True:
        print('sleep in second: '+str(INTERVAL_EACH))
        for coinItem in ["BCH", "DASH", "BTC", "DEGO", "DOGE", "LTC", "XTOR", "TRTL", "WRKZ", "BTCMZ", "LOKI", "XMR", "ARQ", "PLE"]:
            await asyncio.sleep(INTERVAL_EACH)
            print('Update balance: '+ coinItem)
            start = time.time()
            try:
                await store.sql_update_balances(coinItem)
            except Exception as e:
                print(e)
            end = time.time()


# Notify user
async def notify_new_tx_user():
    INTERVAL_EACH = 10
    while True:
        pending_tx = await store.sql_get_new_tx_table('NO', 'NO')
        if pending_tx and len(pending_tx) > 0:
            # let's notify_new_tx_user
            for eachTx in pending_tx:
                user_tx = None
                if eachTx['coin_name'] not in ["DOGE"]:
                    user_tx = await store.sql_get_userwallet_by_paymentid(eachTx['payment_id'], eachTx['coin_name'])
                else:
                    user_tx = await store.sql_get_userwallet_by_paymentid(eachTx['payment_id'], eachTx['coin_name'])
                if user_tx:
                    user_found = bot.get_user(id=int(user_tx['user_id']))
                    if user_found:
                        is_notify_failed = False
                        try:
                            msg = None
                            if eachTx['coin_name'] not in ["DOGE", "LTC", "BTC", "DASH", "BCH"]:
                                msg = "You got a new deposit: ```" + "Coin: {}\nTx: {}\nAmount: {}\nHeight: {:,.0f}".format(eachTx['coin_name'], eachTx['txid'], num_format_coin(eachTx['amount'], eachTx['coin_name']), eachTx['height']) + "```"
                            else:
                                msg = "You got a new deposit: ```" + "Coin: {}\nTx: {}\nAmount: {}\nBlock Hash: {}".format(eachTx['coin_name'], eachTx['txid'], num_format_coin(eachTx['amount'], eachTx['coin_name']), eachTx['blockhash']) + "```"
                            await user_found.send(msg)
                        except (discord.Forbidden, discord.errors.Forbidden) as e:
                            is_notify_failed = True
                            pass
                        update_notify_tx = await store.sql_update_notify_tx_table(eachTx['payment_id'], user_tx['user_id'], user_found.name, 'YES', 'NO' if is_notify_failed == False else 'YES')
                    else:
                        print('Can not find user id {} to notification tx: {}'.format(user_tx['user_id'], eachTx['txid']))
        else:
            print('No tx for notification')
        print('Sleep {}s'.format(INTERVAL_EACH))
        await asyncio.sleep(INTERVAL_EACH)


# Notify user
async def notify_new_swap_user():
    INTERVAL_EACH = 10
    while True:
        pending_tx = await store.sql_get_new_swap_table('NO', 'NO')
        if pending_tx and len(pending_tx) > 0:
            # let's notify_new_tx_user
            for eachSwap in pending_tx:
                user_found = bot.get_user(id=int(eachSwap['owner_id']))
                if user_found:
                    is_notify_failed = False
                    try:
                        msg = "You got incoming swap: ```" + "Coin: {}\nAmount: {}\nFrom: {}".format(eachSwap['coin_name'], num_format_coin(eachSwap['amount'], eachSwap['coin_name']), eachSwap['from']) + "```"
                        await user_found.send(msg)
                    except (discord.Forbidden, discord.errors.Forbidden) as e:
                        is_notify_failed = True
                        pass
                    update_notify_tx = await store.sql_update_notify_swap_table(eachSwap['id'], 'YES', 'NO' if is_notify_failed == False else 'YES')
                else:
                    print('Can not find user id {} to notification swap: #{}'.format(eachSwap['owner_id'], eachSwap['id']))
        await asyncio.sleep(INTERVAL_EACH)


def get_cn_coin_from_address(CoinAddress: str):
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
    elif CoinAddress.startswith("T") and (len(CoinAddress) == 97 or len(CoinAddress) == 98 or len(CoinAddress) == 109):
        COIN_NAME = "XEQ"
    elif CoinAddress.startswith("cms") and (len(CoinAddress) == 98 or len(CoinAddress) == 109):
        COIN_NAME = "BLOG"
    elif (CoinAddress.startswith("ar") or CoinAddress.startswith("aR")) and (len(CoinAddress) == 97 or len(CoinAddress) == 98 or len(CoinAddress) == 109):
        COIN_NAME = "ARQ"
    elif (CoinAddress.startswith("5") or CoinAddress.startswith("9")) and (len(CoinAddress) == 95 or len(CoinAddress) == 106):
        COIN_NAME = "MSR"
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
    print('get_cn_coin_from_address return: ')
    print(COIN_NAME)
    return COIN_NAME


@buy.error
async def buy_error(ctx, error):
    prefix = await get_guild_prefix(ctx)
    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f'{EMOJI_RED_NO} {ctx.author.mention} Missing arguments. '
                       '```'
                       f'{prefix}buy ref_number (to buy from opened order)\n'
                       f'{prefix}buy ticker (to list available selling tickers)'
                       '```')
    return


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


def delete_queue_going(userid: int):
    global QUEUE_ADD_LIST, redis_pool, redis_conn
    # Delete redis and remove user from QUEUE
    if userid in QUEUE_ADD_LIST:
        QUEUE_ADD_LIST.remove(userid)
    keys = redis_conn.keys("MarketBOT:MID_" + str(userid))
    if len(keys) > 1:
        for key in keys:
            redis_conn.delete(key)


async def get_guild_prefix(ctx):
    if isinstance(ctx.channel, discord.DMChannel) == True: return ","
    serverinfo = store.sql_info_by_server(str(ctx.guild.id))
    if serverinfo is None:
        return "."
    else:
        return serverinfo['prefix']


def randomString(stringLength=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def get_md5_file(filepath: str):
    with open(filepath, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def encrypted_string(plaintext: str):
    # Thanks to: https://stackoverflow.com/questions/13051293/encrypt-data-with-python-decrypt-in-php
    # the block size for the cipher object; must be 16, 24, or 32 for AES
    BLOCK_SIZE = 32
    BLOCK_SZ = 14

    # the character used for padding--with a block cipher such as AES, the value
    # you encrypt must be a multiple of BLOCK_SIZE in length.  This character is
    # used to ensure that your value is always a multiple of BLOCK_SIZE
    PADDING = '{'

    # one-liner to sufficiently pad the text to be encrypted
    pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * PADDING

    # one-liners to encrypt/encode and decrypt/decode a string
    # encrypt with AES, encode with base64
    EncodeAES = lambda c, s: base64.b64encode(c.encrypt(pad(s)))
    DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).rstrip(PADDING)
    secret = config.Merchant_Setting.secure_secret
    iv = config.Merchant_Setting.iv
    cipher=AES.new(key=secret,mode=AES.MODE_CBC,IV=iv)
    encoded = EncodeAES(cipher, plaintext)
    print('Encrypted string:', encoded)
    return encoded.hex()


@click.command()
def main():
    bot.loop.create_task(update_balance())
    bot.loop.create_task(notify_new_tx_user())
    bot.loop.create_task(notify_new_swap_user())
    bot.loop.create_task(store_action_list())
    bot.run(config.discord.token, reconnect=True)


if __name__ == '__main__':
    main()
