import sqlite3
import datetime
import pytz
import sys
import os
import concurrent.futures
import time
import config
import robin_stocks.robinhood as rh
import pandas as pd

DEFAULT_DATABASE_PATH = os.path.join(config.ROOT_DIR, '.optionhood.sqlite3')

def est_date_time_stamp():
    return datetime.datetime.now(tz=pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")

def est_time_stamp():
    return datetime.datetime.now(tz=pytz.timezone('US/Eastern')).strftime("%H:%M:%S")

def extract_option_id(url):
    oid = url.split('/')[5]
    return oid

def db_connection(db_path=DEFAULT_DATABASE_PATH):
    con = sqlite3.connect(db_path)
    return con

def create_orders_table():
    con = db_connection()
    cur = con.cursor()

    query = 'CREATE TABLE IF NOT EXISTS orders (local_id INT, cancel_url TEXT, canceled_quantity FLOAT, created_at TEXT, direction TEXT, id TEXT, leg_1_bid_size FLOAT, leg_1_bid_price FLOAT, leg_1_ask_size FLOAT, leg_1_ask_price FLOAT, leg_1_id TEXT, leg_1_option TEXT, leg_1_position_effect TEXT, leg_1_ratio_quantity FLOAT, leg_1_side TEXT, leg_1_market_id TEXT, leg_1_expiration TEXT, leg_1_strike FLOAT, leg_1_call_put TEXT, leg_2_bid_size FLOAT, leg_2_bid_price FLOAT, leg_2_ask_size FLOAT, leg_2_ask_price FLOAT, leg_2_id TEXT, leg_2_option TEXT, leg_2_position_effect TEXT, leg_2_ratio_quantity FLOAT, leg_2_side TEXT, leg_2_market_id TEXT, leg_2_expiration TEXT, leg_2_strike FLOAT, leg_2_call_put TEXT, leg_3_bid_size FLOAT, leg_3_bid_price FLOAT, leg_3_ask_size FLOAT, leg_3_ask_price FLOAT, leg_3_id TEXT, leg_3_option TEXT, leg_3_position_effect TEXT, leg_3_ratio_quantity FLOAT, leg_3_side TEXT, leg_3_market_id TEXT, leg_3_expiration TEXT, leg_3_strike FLOAT, leg_3_call_put TEXT, leg_4_bid_size FLOAT, leg_4_bid_price FLOAT, leg_4_ask_size FLOAT, leg_4_ask_price FLOAT, leg_4_id TEXT, leg_4_option TEXT, leg_4_position_effect TEXT, leg_4_ratio_quantity FLOAT, leg_4_side TEXT, leg_4_market_id TEXT, leg_4_expiration TEXT, leg_4_strike FLOAT, leg_4_call_put TEXT, pending_quantity FLOAT, premium FLOAT, processed_premium FLOAT, price FLOAT, processed_quantity FLOAT, quantity FLOAT, ref_id TEXT, state TEXT, time_in_force TEXT, trigger TEXT, type TEXT, updated_at TEXT, chain_id TEXT, chain_symbol TEXT, response_category TEXT, opening_strategy TEXT, closing_strategy TEXT, stop_price TEXT, est TEXT, est_market TEXT, keep_record BOOLEAN, epoch_update_orders FLOAT, epoch_update_instrument_data FLOAT, epoch_update_orders_market_data FLOAT, PRIMARY KEY (id));'

    cur.execute(query)
    con.commit()
    cur.close()
    con.close()

def create_positions_table():
    con = db_connection()
    cur = con.cursor()

    query = "CREATE TABLE IF NOT EXISTS positions (local_id INT, keep_record BOOLEAN, l_option_id TEXT, l_quantity FLOAT, l_average_price FLOAT, l_underlying TEXT, l_longshort TEXT, l_expiration TEXT, l_strike_price FLOAT , l_callput TEXT, l_implied_volatility FLOAT, l_bid_price FLOAT, l_bid_size FLOAT, l_ask_price FLOAT, l_ask_size FLOAT, l_open_interest FLOAT, l_volume FLOAT, l_above_tick FLOAT, l_below_tick FLOAT, l_cutoff_price FLOAT, s_option_id TEXT UNIQUE, s_quantity FLOAT, s_average_price FLOAT, s_underlying TEXT, s_longshort TEXT, s_expiration TEXT, s_strike_price FLOAT, s_callput TEXT, s_implied_volatility FLOAT, s_bid_price FLOAT, s_bid_size FLOAT, s_ask_price FLOAT, s_ask_size FLOAT, s_open_interest FLOAT, s_volume FLOAT, s_above_tick FLOAT, s_below_tick FLOAT, s_cutoff_price FLOAT, est TEXT, l_est_market TEXT, s_est_market TEXT, underlying_price FLOAT, epoch_update_uncapped_shorts FLOAT DEFAULT 1.0, epoch_update_long_position_market_data FLOAT DEFAULT 1.0, epoch_update_short_position_market_data FLOAT DEFAULT 1.0, epoch_update_underlying_market_data FLOAT DEFAULT 1.0, PRIMARY KEY (l_option_id));"

    cur.execute(query)
    con.commit()
    cur.close()
    con.close()

def create_account_table():
    con = db_connection()
    cur = con.cursor()

    query = 'CREATE TABLE IF NOT EXISTS account (id INT, buying_power FLOAT, equity FLOAT, previous_close FLOAT, epoch_update FLOAT DEFAULT 1.0, day_trades INT, equity_expiries TEXT, option_expiries TEXT, PRIMARY KEY (id));' 

    cur.execute(query)
    con.commit()
    cur.close()
    con.close()

def drop_positions_table():
    con = db_connection()
    cur = con.cursor()

    query = "DROP TABLE IF EXISTS positions;"

    cur.execute(query)
    con.commit()
    cur.close()
    con.close()

def drop_account_table():
    con = db_connection()
    cur = con.cursor()

    query = "DROP TABLE IF EXISTS account;"

    cur.execute(query)
    con.commit()
    cur.close()
    con.close()

def drop_orders_table():
    con = db_connection()
    cur = con.cursor()

    query = "DROP TABLE IF EXISTS orders;"

    cur.execute(query)
    con.commit()
    cur.close()
    con.close()

def recreate_all_tables():
    recreate_orders_table()
    recreate_positions_table()
    recreate_account_table()
    
def create_all_tables():
    create_orders_table()
    create_positions_table() 
    create_account_table()
    
def recreate_orders_table():
    drop_orders_table()
    create_orders_table()
    
def recreate_positions_table():
    drop_positions_table()
    create_positions_table() 

def recreate_account_table():
    drop_account_table()
    create_account_table()

def update_account():
    # Robinhood request for buying power.
    buying_power = rh.profiles.load_account_profile()['buying_power']

    # Robihood request for day trades.
    r = rh.account.get_day_trades()
    day_trades = len(r['option_day_trades']) + len(r['equity_day_trades'])
    
    # Build equity expiration dates string.
    equity_expiries = ''
    for t in r['equity_day_trades']:
        equity_expiries = equity_expiries + t['expiry_date'].split('-')[2] + ' '
    
    # Build options expiration dates string.
    option_expiries = ''
    for t in r['option_day_trades']:
        option_expiries = option_expiries + t['expiry_date'].split('-')[2] + ' '
    
    now = time.time()

    insert_data = (1, buying_power, day_trades, now, equity_expiries, option_expiries, buying_power, day_trades, now, equity_expiries, option_expiries)

    con = db_connection()
    cur = con.cursor()
    
    query = 'INSERT INTO account (id, buying_power, day_trades, epoch_update, equity_expiries, option_expiries) VALUES (?,?,?,?,?,?) ON CONFLICT (id) DO UPDATE SET (buying_power, day_trades, epoch_update, equity_expiries, option_expiries) = (?,?,?,?,?);'
    
    cur.execute(query, insert_data)
    con.commit()
    cur.close()
    con.close()

def update_orders():
    res = rh.orders.get_all_open_option_orders()
    res = sorted(res, key=lambda i: i['id'])
    local_id_counter = 1

    #slate all records for deletion 
    con = db_connection()
    cur = con.cursor()

    query = 'UPDATE orders SET keep_record=false;'

    cur.execute(query)
    con.commit() 

    #allow records to stay with matching id
    for o in res:
        query = 'UPDATE orders SET keep_record=true WHERE id=?;'

        cur.execute(query, (o['id'],))
        con.commit()

    #clear non-existent orders
    query = 'DELETE FROM orders WHERE keep_record=false;'
    cur.execute(query)
    con.commit()

    for o in res:
        local_id = local_id_counter
        local_id_counter += 1
        est = est_date_time_stamp()
        leg_1_id = o['legs'][0]['id']
        leg_1_option = o['legs'][0]['option']
        leg_1_position_effect = o['legs'][0]['position_effect']
        leg_1_ratio_quantity = o['legs'][0]['ratio_quantity']
        leg_1_side = o['legs'][0]['side']
        leg_1_market_id = extract_option_id(o['legs'][0]['option'])

        leg_2_id = None
        leg_2_option = None
        leg_2_position_effect = None
        leg_2_ratio_quantity = None
        leg_2_side = None
        leg_2_market_id = None
        leg_3_id = None
        leg_3_option = None
        leg_3_position_effect = None
        leg_3_ratio_quantity = None
        leg_3_side = None
        leg_3_market_id = None
        leg_4_id = None
        leg_4_option = None
        leg_4_position_effect = None
        leg_4_ratio_quantity = None
        leg_4_side = None
        leg_4_market_id = None

        if len(o['legs']) > 1:
            leg_2_id = o['legs'][1]['id']
            leg_2_option = o['legs'][1]['option']
            leg_2_position_effect = o['legs'][1]['position_effect']
            leg_2_ratio_quantity = o['legs'][1]['ratio_quantity']
            leg_2_side = o['legs'][1]['side']
            leg_2_market_id = extract_option_id(o['legs'][1]['option'])
        if len(o['legs']) > 2:
            leg_3_id = o['legs'][2]['id']
            leg_3_option = o['legs'][2]['option']
            leg_3_position_effect = o['legs'][2]['position_effect']
            leg_3_ratio_quantity = o['legs'][2]['ratio_quantity']
            leg_3_side = o['legs'][2]['side']
            leg_3_market_id = extract_option_id(o['legs'][2]['option'])
        if len(o['legs']) > 3:
            leg_4_id = o['legs'][3]['id']
            leg_4_option = o['legs'][3]['option']
            leg_4_position_effect = o['legs'][3]['position_effect']
            leg_4_ratio_quantity = o['legs'][3]['ratio_quantity']
            leg_4_side = o['legs'][3]['side']
            leg_4_market_id = extract_option_id(o['legs'][3]['option'])

        now = time.time()

        values = (local_id, o['cancel_url'], o['canceled_quantity'], o['created_at'], o['direction'], o['id'], leg_1_id, leg_1_option, leg_1_position_effect, leg_1_ratio_quantity, leg_1_side, leg_1_market_id, leg_2_id, leg_2_option, leg_2_position_effect, leg_2_ratio_quantity, leg_2_side, leg_2_market_id, leg_3_id, leg_3_option, leg_3_position_effect, leg_3_ratio_quantity, leg_3_side, leg_3_market_id, leg_4_id, leg_4_option, leg_4_position_effect, leg_4_ratio_quantity, leg_4_side, leg_4_market_id, o['pending_quantity'], o['premium'], o['processed_premium'], o['price'], o['processed_quantity'], o['quantity'], o['ref_id'], o['state'], o['time_in_force'], o['trigger'], o['type'], o['updated_at'], o['chain_id'], o['chain_symbol'], o['response_category'], o['opening_strategy'], o['closing_strategy'], o['stop_price'], est, now, local_id, o['cancel_url'], o['canceled_quantity'], o['created_at'], o['direction'], leg_1_id, leg_1_option, leg_1_position_effect, leg_1_ratio_quantity, leg_1_side, leg_1_market_id, leg_2_id, leg_2_option, leg_2_position_effect, leg_2_ratio_quantity, leg_2_side, leg_2_market_id, leg_3_id, leg_3_option, leg_3_position_effect, leg_3_ratio_quantity, leg_3_side, leg_3_market_id, leg_4_id, leg_4_option, leg_4_position_effect, leg_4_ratio_quantity, leg_4_side, leg_4_market_id, o['pending_quantity'], o['premium'], o['processed_premium'], o['price'], o['processed_quantity'], o['quantity'], o['ref_id'], o['state'], o['time_in_force'], o['trigger'], o['type'], o['updated_at'], o['chain_id'], o['chain_symbol'], o['response_category'], o['opening_strategy'], o['closing_strategy'], o['stop_price'], est, now)

        query = "INSERT INTO orders (local_id, cancel_url, canceled_quantity, created_at, direction, id, leg_1_id, leg_1_option, leg_1_position_effect, leg_1_ratio_quantity, leg_1_side, leg_1_market_id, leg_2_id, leg_2_option, leg_2_position_effect, leg_2_ratio_quantity, leg_2_side, leg_2_market_id, leg_3_id, leg_3_option, leg_3_position_effect, leg_3_ratio_quantity, leg_3_side, leg_3_market_id, leg_4_id, leg_4_option, leg_4_position_effect, leg_4_ratio_quantity, leg_4_side, leg_4_market_id, pending_quantity, premium, processed_premium, price, processed_quantity, quantity, ref_id, state, time_in_force, trigger, type, updated_at, chain_id, chain_symbol, response_category, opening_strategy, closing_strategy, stop_price, est, epoch_update_orders) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET local_id=?, cancel_url=?, canceled_quantity=?, created_at=?, direction=?, leg_1_id=?, leg_1_option=?, leg_1_position_effect=?, leg_1_ratio_quantity=?, leg_1_side=?, leg_1_market_id=?, leg_2_id=?, leg_2_option=?, leg_2_position_effect=?, leg_2_ratio_quantity=?, leg_2_side=?, leg_2_market_id=?, leg_3_id=?, leg_3_option=?, leg_3_position_effect=?, leg_3_ratio_quantity=?, leg_3_side=?, leg_3_market_id=?, leg_4_id=?, leg_4_option=?, leg_4_position_effect=?, leg_4_ratio_quantity=?, leg_4_side=?, leg_4_market_id=?, pending_quantity=?, premium=?, processed_premium=?, price=?, processed_quantity=?, quantity=?, ref_id=?, state=?, time_in_force=?, trigger=?, type=?, updated_at=?, chain_id=?, chain_symbol=?, response_category=?, opening_strategy=?, closing_strategy=?, stop_price=?, est=?, epoch_update_orders=?;"

        cur.execute(query, values)
        con.commit()
        
    cur.close()
    con.close()
    
def update_instrument_data():
    con = db_connection()
    cur = con.cursor()

    query = 'SELECT id, leg_1_option, leg_1_expiration, leg_2_option, leg_3_option, leg_4_option FROM orders;'

    cur.execute(query)
    res = cur.fetchall()
    con.commit()

    for order in res:
        if order[2] != None:
            data = (time.time(), order[0])
            q = 'UPDATE orders SET epoch_update_instrument_data=? WHERE id=?;'
            cur.execute(q, data)
            con.commit()
            continue

        strike1 = None
        exp1 = None
        call_put1 = None
        strike2 = None
        exp2 = None
        call_put2 = None
        strike3 = None
        exp3 = None
        call_put3 = None
        strike4 = None
        exp4 = None
        call_put4 = None

        if order[1] != None:
            instr_id = extract_option_id(order[1])
            r1 = rh.options.get_option_instrument_data_by_id(instr_id)
            strike1 = r1['strike_price']
            exp1 = r1['expiration_date']
            call_put1 = r1['type']

        if order[3] != None:       
            instr_id = extract_option_id(order[3])
            r2 = rh.options.get_option_instrument_data_by_id(instr_id)
            strike1 = r2['strike_price']
            exp1 = r2['expiration_date']
            call_put1 = r2['type']

        if order[4] != None:
            instr_id = extract_option_id(order[4])
            r3 = rh.options.get_option_instrument_data_by_id(instr_id)
            strike1 = r3['strike_price']
            exp1 = r3['expiration_date']
            call_put1 = r3['type']

        if order[5] != None:
            instr_id = extract_option_id(order[5])
            r4 = rh.options.get_option_instrument_data_by_id(instr_id)
            strike1 =r4['strike_price']
            exp1 = r4['expiration_date']
            call_put1 = r4['type']

        q = 'UPDATE orders SET leg_1_strike=?, leg_1_expiration=?, leg_1_call_put=?, leg_2_strike=?, leg_2_expiration=?, leg_2_call_put=?, leg_3_strike=?, leg_3_expiration=?, leg_3_call_put=?, leg_4_strike=?, leg_4_expiration=?, leg_4_call_put=?, epoch_update_instrument_data=? WHERE id=?;'

        insert_data = (strike1, exp1, call_put1, strike2, exp2, call_put2, strike3, exp3, call_put3, strike4, exp4, call_put4, time.time(), order[0])

        cur.execute(q, insert_data)

        con.commit()

    cur.close()
    con.close()


def update_orders_market_data():
    #slate all records for deletion 
    con = db_connection()
    cur = con.cursor()

    query = 'SELECT id, leg_1_market_id, leg_2_market_id, leg_3_market_id, leg_4_market_id FROM orders;'

    cur.execute(query)
    res = cur.fetchall()
    con.commit()
    
    mkt_est = est_date_time_stamp()
    
    for order in res:
        pk = order[0]
        
        leg_1_bid_size = None
        leg_1_bid_price = None
        leg_1_ask_size = None
        leg_1_ask_price = None
        leg_2_bid_size = None
        leg_2_bid_price = None
        leg_2_ask_size = None
        leg_2_ask_price = None
        leg_3_bid_size = None
        leg_3_bid_price = None
        leg_3_ask_size = None
        leg_3_ask_price = None
        leg_4_bid_size = None
        leg_4_bid_price = None
        leg_4_ask_size = None
        leg_4_ask_price = None
        
        if order[1] != None:
            mktdata = rh.options.get_option_market_data_by_id(order[1])
            leg_1_bid_size = mktdata[0]['bid_size']
            leg_1_bid_price = mktdata[0]['bid_price']
            leg_1_ask_size = mktdata[0]['ask_size']
            leg_1_ask_price = mktdata[0]['ask_price']
            
        if order[2] != None:
            mktdata = rh.options.get_option_market_data_by_id(order[2])
            leg_2_bid_size = mktdata[0]['bid_size']
            leg_2_bid_price = mktdata[0]['bid_price']
            leg_2_ask_size = mktdata[0]['ask_size']
            leg_2_ask_price = mktdata[0]['ask_price']
            
        if order[3] != None:
            mktdata = rh.options.get_option_market_data_by_id(order[3])
            leg_3_bid_size = mktdata[0]['bid_size']
            leg_3_bid_price = mktdata[0]['bid_price']
            leg_3_ask_size = mktdata[0]['ask_size']
            leg_3_ask_price = mktdata[0]['ask_price']
            
        if order[4] != None:
            mktdata = rh.options.get_option_market_data_by_id(order[4])
            leg_4_bid_size = mktdata[0]['bid_size']
            leg_4_bid_price = mktdata[0]['bid_price']
            leg_4_ask_size = mktdata[0]['ask_size']
            leg_4_ask_price = mktdata[0]['ask_price']
        
        row = (leg_1_bid_size, leg_1_bid_price, leg_1_ask_size, leg_1_ask_price, leg_2_bid_size, leg_2_bid_price, leg_2_ask_size, leg_2_ask_price, leg_3_bid_size, leg_3_bid_price, leg_3_ask_size, leg_3_ask_price, leg_4_bid_size, leg_4_bid_price, leg_4_ask_size, leg_4_ask_price, mkt_est, time.time(), pk)

        q = 'UPDATE orders SET leg_1_bid_size=?, leg_1_bid_price=?, leg_1_ask_size=?, leg_1_ask_price=?, leg_2_bid_size=?, leg_2_bid_price=?, leg_2_ask_size=?, leg_2_ask_price=?, leg_3_bid_size=?, leg_3_bid_price=?, leg_3_ask_size=?, leg_3_ask_price=?, leg_4_bid_size=?, leg_4_bid_price=?, leg_4_ask_size=?, leg_4_ask_price=?, est_market=?, epoch_update_orders_market_data=? WHERE id=?;'

        cur.execute(q, row)
        con.commit()

def update_underlying_market_data():
    #CONNECT TO DB
    con = db_connection()
    cur = con.cursor()

    q = 'SELECT l_underlying, l_option_id FROM positions;'
    cur.execute(q)
    rows = cur.fetchall()

    for row in rows:
        ticker = row[0]
        res = rh.stocks.get_latest_price(ticker)
        insert_data = (round(float(res[0]), 2), row[1])

        q = 'UPDATE positions SET underlying_price=? WHERE l_option_id=?;'
        cur.execute(q, insert_data)
        con.commit()

        set_positions_epoch_time(row[1], 'epoch_update_underlying_market_data')

    cur.close()
    con.close()

def update_uncapped_shorts():
    con = db_connection()
    cur = con.cursor()

    q = 'SELECT s_option_id, l_option_id FROM positions;'

    cur.execute(q)
    short_option_ids = cur.fetchall()

    for sid in short_option_ids:
        try:
            #INSTRUMENT DATA HTTP REQUEST
            instrument_info = rh.options.get_option_instrument_data_by_id(sid[0])

            #INSTRUMENT DATA
            underlying = instrument_info['chain_symbol']
            expiration = instrument_info['expiration_date']
            strike_price = float(instrument_info['strike_price'])
            callput = instrument_info['type']

            #TICK DATA
            above_tick = instrument_info['min_ticks']['above_tick']
            below_tick = instrument_info['min_ticks']['below_tick']
            cutoff_price = instrument_info['min_ticks']['cutoff_price']

            row = (underlying, expiration, strike_price, callput, above_tick, below_tick, cutoff_price, sid[0])
            q = 'UPDATE positions SET s_underlying=?, s_expiration=?, s_strike_price=?, s_callput=?, s_above_tick=?, s_below_tick=?, s_cutoff_price=? WHERE s_option_id=?;' 
            cur.execute(q, row)
            con.commit()

            set_positions_epoch_time(sid[1], 'epoch_update_uncapped_shorts')

        except:
            continue

    cur.close()
    con.close()

def update_position_info():
    #CONNECT TO DB
    con = db_connection()
    cur = con.cursor()

    #GET CURRENT POSITIONS
    positions = rh.options.get_open_option_positions()
    positions = sorted(positions, key=lambda i: (i['type'], i['option_id']))
    local_id_counter = 1

    #set keep_record switch to false
    delete_true_sql = 'UPDATE positions SET keep_record=false;'
    cur.execute(delete_true_sql)
    con.commit()

    est = est_date_time_stamp()

    #toggle existing positions so they are not deleted
    for option in positions:
        if option['type'] == 'short':
            continue

        local_id = local_id_counter
        local_id_counter += 1

        l_option_id = option['option_id'] 
        l_quantity = float(option['quantity'])
        l_average_price = float(option['average_price'])
        l_underlying = option['chain_symbol']
        l_longshort = option['type']

        #INSTRUMENT DATA HTTP REQUEST
        instrument_info = rh.options.get_option_instrument_data_by_id(l_option_id)

        #INSTRUMENT DATA
        l_expiration = instrument_info['expiration_date']
        l_strike_price = float(instrument_info['strike_price'])
        l_callput = instrument_info['type']

        #TICK DATA
        l_above_tick = instrument_info['min_ticks']['above_tick']
        l_below_tick = instrument_info['min_ticks']['below_tick']
        l_cutoff_price = instrument_info['min_ticks']['cutoff_price']

        row = (local_id, l_option_id, True, l_quantity, l_average_price, l_underlying, l_longshort, l_expiration, l_strike_price, l_callput, l_above_tick, l_below_tick, l_cutoff_price, est, local_id, True, l_quantity, l_average_price, l_above_tick, l_below_tick, l_cutoff_price, est)

        q = 'INSERT INTO positions (local_id, l_option_id, keep_record, l_quantity, l_average_price, l_underlying, l_longshort, l_expiration, l_strike_price, l_callput, l_above_tick, l_below_tick, l_cutoff_price, est) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (l_option_id) DO UPDATE SET (local_id, keep_record, l_quantity, l_average_price, l_above_tick, l_below_tick, l_cutoff_price, est) = (?,?,?,?,?,?,?,?);'

        cur.execute(q, row)
        con.commit()


    #delete rows where keep_record is False
    q = 'DELETE FROM positions WHERE keep_record=false;'
    cur.execute(q)
    con.commit()

    #UPDATE SHORTS
    for option in positions:
        #POSITION DATA
        if option['type'] == 'long':
            continue

        option_id = option['option_id'] 
        quantity = float(option['quantity'])
        average_price = float(option['average_price'])
        underlying = option['chain_symbol']
        longshort = option['type']

        #INSTRUMENT DATA HTTP REQUEST
        instrument_info = rh.options.get_option_instrument_data_by_id(option_id)

        #INSTRUMENT DATA
        expiration = instrument_info['expiration_date']
        strike_price = float(instrument_info['strike_price'])
        callput = instrument_info['type']

        #TICK DATA
        above_tick = instrument_info['min_ticks']['above_tick']
        below_tick = instrument_info['min_ticks']['below_tick']
        cutoff_price = instrument_info['min_ticks']['cutoff_price']

        row = (quantity, average_price, underlying, longshort, expiration, strike_price, callput, above_tick, below_tick, cutoff_price, option_id)

        q = 'UPDATE positions SET s_quantity=?, s_average_price=?, s_underlying=?, s_longshort=?, s_expiration=?, s_strike_price=?, s_callput=?, s_above_tick=?, s_below_tick=?, s_cutoff_price=? WHERE s_option_id=?' 
        cur.execute(q, row)
        con.commit()

    cur.close()
    con.close()

def update_spread_cap_ids():
    con = db_connection()
    cur = con.cursor()
    
    q = 'SELECT * FROM positions WHERE s_option_id IS NULL;'
    cur.execute(q)
    unmatched_longs = cur.fetchall()

    for row in unmatched_longs:
        l_option_id  = row[2]
        ticker     = row[5]
        expiration = row[7]
        callorput  = row[9]
        res = rh.options.find_options_by_expiration(ticker, expiration, optionType=callorput)

        df = pd.DataFrame(res)
        df['strike_price'] = df['strike_price'].astype(float)
        df.sort_values(by=['strike_price'], inplace=True, ignore_index=True)
        long_index = df.loc[df['id'] == l_option_id].index.values.astype(int)[0]

        if callorput == 'call':
            short_index = int(long_index) + 1
        if callorput == 'put':
            short_index = int(long_index) - 1

        s_option_id = None
        try:
            s_option_id = df.iloc[short_index]['id']
        except:
            continue

        insert_data = (s_option_id, l_option_id)
        q = 'UPDATE positions SET s_option_id=? WHERE l_option_id=?'
        cur.execute(q, insert_data)
        con.commit()
    cur.close()
    con.close()
    
def update_long_position_market_data():
    #CONNECT TO DB
    con = db_connection()
    cur = con.cursor()

    q = 'SELECT l_option_id, l_option_id FROM positions;'
    cur.execute(q)
    option_ids = cur.fetchall()

    for oid in option_ids:
        #MARKET DATA HTTP REQUEST
        market_data = rh.options.get_option_market_data_by_id(oid[0])
        market_data = market_data[0]
        #MARKET DATA
        try:
            implied_volatility = float(market_data['implied_volatility'])
        except:
            implied_volatility = 0
        bid_price = float(market_data['bid_price'])
        bid_size = float(market_data['bid_size'])
        ask_price = float(market_data['ask_price'])
        ask_size = float(market_data['ask_size'])
        open_interest = float(market_data['open_interest'])
        volume = float(market_data['volume'])
        est_market = est_date_time_stamp()

        insert_data = (implied_volatility, bid_price, bid_size, ask_price, ask_size, open_interest, volume, est_market, oid[0])

        q = 'UPDATE positions SET l_implied_volatility=?, l_bid_price=?, l_bid_size=?, l_ask_price=?, l_ask_size=?, l_open_interest=?, l_volume=?, l_est_market=? WHERE l_option_id=?'
        cur.execute(q, insert_data)
        con.commit()

        set_positions_epoch_time(oid[1], 'epoch_update_long_position_market_data')


    cur.close()
    con.close()    
    
def update_short_position_market_data():
    #CONNECT TO DB
    con = db_connection()
    cur = con.cursor()

    q = 'SELECT l_option_id, s_option_id FROM positions;'
    cur.execute(q)
    option_ids = cur.fetchall()

    for oid in option_ids:
        #MARKET DATA HTTP REQUEST
        market_data = rh.options.get_option_market_data_by_id(oid[1])
        market_data = market_data[0]
        #MARKET DATA
        try:
            implied_volatility = float(market_data['implied_volatility'])
        except:
            implied_volatility = 0
        bid_price = float(market_data['bid_price'])
        bid_size = float(market_data['bid_size'])
        ask_price = float(market_data['ask_price'])
        ask_size = float(market_data['ask_size'])
        open_interest = float(market_data['open_interest'])
        volume = float(market_data['volume'])
        est_market = est_date_time_stamp()

        insert_data = (implied_volatility, bid_price, bid_size, ask_price, ask_size, open_interest, volume, est_market, oid[1])

        q = 'UPDATE positions SET s_implied_volatility=?, s_bid_price=?, s_bid_size=?, s_ask_price=?, s_ask_size=?, s_open_interest=?, s_volume=?, s_est_market=? WHERE s_option_id=?'
        cur.execute(q, insert_data)
        con.commit()

        set_positions_epoch_time(oid[0], 'epoch_update_short_position_market_data')

    cur.close()
    con.close()

    set_positions_epoch_time(oid[0], 'epoch_update_short_position_market_data')

def set_positions_epoch_time(l_option_id, epoch_field):
    con = db_connection()
    cur = con.cursor()

    insert_data = (time.time(), l_option_id)

    q = 'UPDATE positions SET ' + epoch_field + '=? WHERE l_option_id=?;'

    cur.execute(q, insert_data)
    con.commit()
    cur.close()
    con.close()

def task_runner(task_name):
    print('Running task_runner for ' + task_name)
    home = os.path.expanduser('~')

    while True:
        if not os.path.exists(home + '/.tokens/robinhood.pickle'):
            print("Exiting runners. Robinhood session not found. \nType 'login' in command window, \nand then 'start' to restart.")
            sys.exit()

        try:
            exec(task_name + '()')
        except:
            print('Problem running task: ' + task_name)
        time.sleep(config.RH_REQUEST_INTERVAL)


if __name__ == '__main__':
    create_all_tables()
    rh.login(config.USERNAME, config.PASSWORD)
    home = os.path.expanduser('~')

    print('Logged in!')
    print('Starting task runners...')
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        executor.submit(task_runner, task_name='update_account')
        executor.submit(task_runner, task_name='update_position_info')
        executor.submit(task_runner, task_name='update_spread_cap_ids')
        executor.submit(task_runner, task_name='update_uncapped_shorts')
        executor.submit(task_runner, task_name='update_long_position_market_data')
        executor.submit(task_runner, task_name='update_short_position_market_data')
        executor.submit(task_runner, task_name='update_underlying_market_data')
        executor.submit(task_runner, task_name='update_orders')
        executor.submit(task_runner, task_name='update_orders_market_data')
        executor.submit(task_runner, task_name='update_instrument_data')

        while True:
            print("Updating account information at " + est_date_time_stamp() + ".")
            if not os.path.exists(home + '/.tokens/robinhood.pickle'):
                print("Exiting runners from main script. Robinhood session not found. \nType 'login' in command window, \nand then 'start' to restart.")
                sys.exit()

            time.sleep(config.RH_REQUEST_INTERVAL)


