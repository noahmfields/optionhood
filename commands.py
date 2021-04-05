import cmd
import robin_stocks.robinhood as robin_stocks
import config
import os
import db
import time
import datetime

def cinput():
    i = input(config.submenu_prompt)
    if i == 'exit':
        return('return')
    return(i)

def get_options_table(ticker, strike_depth, number_of_expirations):
    #UPPER CASE TICKER
    ticker = ticker.upper()
    
    #SELECT LIST OF EXPIRATION DATES FROM THE NEAREST CHAIN
    chains = robin_stocks.options.get_chains(ticker)
    nearest_chain = chains['expiration_dates'][0]
    valid_chains = chains['expiration_dates'][0:number_of_expirations]
    
    #GET RELEVANT STRIKES
    current_price = robin_stocks.stocks.get_latest_price(ticker)
    l = robin_stocks.options.find_tradable_options(ticker, expirationDate=nearest_chain, optionType='call')
    sorted_l = sorted(l, key=lambda k: float(k['strike_price']))
    current_price = float(robin_stocks.stocks.get_latest_price(ticker)[0])
    index_below_current_price = 0
    counter = 0
    for i in sorted_l:
        if float(i['strike_price']) > current_price:
            index_below_current_price = counter - 1
            break
        counter += 1    
    start_selection = index_below_current_price - (strike_depth - 1)
    end_selection = start_selection + (strike_depth * 2)
    selected_items = sorted_l[start_selection:end_selection]
    selected_strikes = []
    for i in selected_items:
        selected_strikes.append(i['strike_price'])
    
    selected_strikes.reverse()
    # NOW WE HAVE
    # selected_strikes (high to low) &
    # valid_chains
    
    table = {}
    
    #def get_option_market_data(inputSymbols, expirationDate, strikePrice, optionType, info=None):
    
    for strike in selected_strikes:
        strike_row = []
        for exp_date in valid_chains:
            try:
                call = robin_stocks.options.get_option_market_data(ticker, exp_date, strike, 'call')[0][0]['mark_price']
            except:
                #print('EXCEPTION CALL')
                call = ''
            try:
                put = robin_stocks.options.get_option_market_data(ticker, exp_date, strike, 'put')[0][0]['mark_price']
            except:
                #print('EXCEPTION PUT')
                put = ''
                
            entry = {exp_date: {'call': call, 'put': put}}
            strike_row.append(entry)
        table[strike] = strike_row
     
    return(table)

def console_print_options_table(table):
    expiration_dates = []
    #GET EXPIRATION DATES
    for key, exp_item in table.items():
        for exp_set in exp_item:
            for exp, cp_pair in exp_set.items():
                expiration_dates.append(exp)
        break
    header1 = '\t'
    for exp in expiration_dates:
        header1 = header1 + exp + '\t'
    
    header2 = 'STRIKE\t'
    header2 = header2 + 'CALL\tPUT\t' * len(expiration_dates)
    
    rows = []
    for key, exp_item in table.items():
        row = (str(round(float(key), 1)) + '\t').expandtabs(8)
        for exp_set in exp_item:
            for exp, cp_pair in exp_set.items():
                try:
                    c = str(round(float(cp_pair['call']), 2))
                except:
                    c = ''
                try:
                    p = str(round(float(cp_pair['put']), 2))
                except:
                    p = ''
                            
                row = row + (c + '\t').expandtabs(8)
                row = row + (p + '\t').expandtabs(8)
        rows.append(row)
        
    rows.insert((int(len(rows) / 2)), '')
        
    header1 = '\t'
    for exp in expiration_dates:
        header1 = header1 + exp + '\t'
        
    print(header1.expandtabs(8))
    print(header2)
    for row in rows:
        print(row)


class OptionHoodCmd(cmd.Cmd):
    #def precmd(self, line):
        #print('FAFA')
        #return(line)
        
    def do_wipe(self, args):
        """Wipe positions and orders table."""
        db.recreate_account_table()
        db.recreate_orders_table()
        db.recreate_positions_table()

    def do_cancel(self, args):
        """Cancel all or a specific order. Enter 'all' or an order number ID."""
        print("Enter 'all' or a specific number.")

    def do_login(self, args):
        """Login to Robinhood. To start data scripts use 'start' command."""
        
        try:
            home = os.path.expanduser('~')
            os.remove(home + '/.tokens/robinhood.pickle')
        except:
            print('Pickle login already removed...')
        robin_stocks.login(config.username, config.password)
        
    def do_start(self, args):
        """Starts data requests from Robinhood. You will see this activity in the left bottom pane."""
        os.system('tmux send-keys -t2 C-c Enter')
        os.system('tmux send-keys -t2 python3\ db.py Enter')
        
    def do_stop(self, args):
        """Stops data requests to Robinhood."""
        os.system('tmux send-keys -t2 C-c C-c C-c')
        
    def do_logout(self, args):
        """Deletes the current session."""
        try:
            home = os.path.expanduser('~')
            os.remove(home + '/.tokens/robinhood.pickle')
        except:
            print('Pickle login already removed...')

    def do_cancel(self, args):
        """Cancel a specific order or all orders."""
        print("Enter 'all' or order number.")
        i = input(config.submenu_prompt)
        if i =='all':
            print('Cancelling all orders...')
            robin_stocks.cancel_all_option_orders()
            return

        con = db.db_connection()
        cur = con.cursor()

        q = 'SELECT id FROM orders WHERE local_id=?;'

        try:
            i = int(i)
        except:
            print("Not an integer selection. No orders cancelled.")
            return

        try:
            print('Attempting to cancel order # ' + str(i))
            cur.execute(q, (i,))
            res = cur.fetchall()
            robin_stocks.orders.cancel_option_order(res[0][0])
            print('Success.')
        except:
            print('Failure.')
            return

    def do_exit(self, args):
        """Terminate optionhood tmux session. This stops all https requests to Robinhood. It does not erase the sqlite database and it does not log you out from your current session with Robinhood. You may restart Optionhood after without the need to login again, as long as your session hasn't expired."""
        os.system('tmux kill-session -t optionhood')    
        return

    def do_buy(self, args):
        """Buy specific option."""
        print("Buy Option")
        print('quantity symbol call/put strike expiration limit')
        res = input(config.submenu_prompt)
        res = res.split()

        res = robin_stocks.orders.order_buy_option_limit('open', 'debit', res[5], res[1], res[0], res[4], res[3], optionType=res[2], timeInForce='gtc', jsonify=True)
        print(res)
        return

    def do_sell(self, args):
        """Sell specific option."""
        print('Sell Option')
        print('open/close quantity symbol call/put strike expiration limit')
        res = input(config.submenu_prompt)
        res = res.split()
        res = robin_stocks.orders.order_sell_option_limit(res[0], 'credit', res[6], res[2], res[1], res[5], res[4], optionType=res[3], timeInForce='gtc', jsonify=True)
        print(res)
        return

    def do_cap(self, args):
        """Short cap an existing long position by position number."""
        print("Cap Debit Spread")
        print("Enter position number to cap:")
        pos = input(config.submenu_prompt)
        print("Quantity to cap: ")
        qty = input(config.submenu_prompt)
        print("Limit price: ")
        limit = input(config.submenu_prompt)

        con = db.db_connection()
        cur = con.cursor()

        q = 'SELECT l_underlying, s_expiration, s_strike_price, s_callput FROM positions WHERE local_id=?;'

        cur.execute(q, (pos,))
        res = cur.fetchall()

        ticker = res[0][0]
        exp = res[0][1]
        strike = res[0][2]
        callput = res[0][3]

        r = robin_stocks.orders.order_sell_option_limit('open', 'credit', limit, ticker, qty, exp, strike, optionType=callput, timeInForce='gtc', jsonify=True)
        return
    
    def do_increase(self, args):
        """Increase an existing long position."""
        print("Increase Position")
        print("Position: ")
        pos = input(config.submenu_prompt)
        print("Quantity to add: ")
        qty = input(config.submenu_prompt)
        print("Limit price: ")
        limit = input(config.submenu_prompt)
        
        con = db.db_connection()
        cur = con.cursor()

        q = 'SELECT l_underlying, l_expiration, l_strike_price, l_callput FROM positions WHERE local_id=?;'

        cur.execute(q, (pos,))
        res = cur.fetchall()

        ticker = res[0][0]
        exp = res[0][1]
        strike = res[0][2]
        callput = res[0][3]

        r = robin_stocks.orders.order_buy_option_limit('open', 'debit', limit, ticker, qty, exp, strike, optionType=callput, timeInForce='gtc', jsonify=True)

        print(r)
        return
        
    def do_decrease(self, args):
        """Decrease an existing long position."""
        print("Decrease Position")
        print("Position: ")
        pos = input(config.submenu_prompt)
        print("Quantity to sell: ")
        qty = input(config.submenu_prompt)
        print("Limit price: ")
        limit = input(config.submenu_prompt)
        
        con = db.db_connection()
        cur = con.cursor()

        q = 'SELECT l_underlying, l_expiration, l_strike_price, l_callput FROM positions WHERE local_id=?;'

        cur.execute(q, (pos,))
        res = cur.fetchall()

        ticker = res[0][0]
        exp = res[0][1]
        strike = res[0][2]
        callput = res[0][3]

        r = robin_stocks.orders.order_sell_option_limit('close', 'credit', limit, ticker, qty, exp, strike, optionType=callput, timeInForce='gtc', jsonify=True)

        print(r)
        return
    
    def do_atmtable(self, args):
        """Print ATM data for a ticker."""
        num_of_strikes = 5
        num_of_expirations = 6
        
        print("Ticker:")
        ticker = input(config.submenu_prompt)
        ticker = ticker.upper()
        table = get_options_table(ticker, num_of_strikes, num_of_expirations)
        console_print_options_table(table)
        
    def do_cds(self, args):
        """Close debit spread."""
        print("Close debit spread.")
        print("Position: ")
        pos = input(config.submenu_prompt)
        print("Quantity to close: ")
        qty = input(config.submenu_prompt)
        print("Limit price: ")
        limit = input(config.submenu_prompt)
        
        con = db.db_connection()
        cur = con.cursor()
        q = 'SELECT l_underlying, l_expiration, l_strike_price, l_callput, s_expiration, s_strike_price, s_callput FROM positions WHERE local_id=?;'
        cur.execute(q, (pos,))
        res = cur.fetchall()
        
        print(res)
            
        ticker = res[0][0]
        spread = []
        spread.append({'expirationDate': res[0][1], 'strike': res[0][2], 'optionType': res[0][3], 'effect': 'close', 'action': 'sell'})
        spread.append({'expirationDate': res[0][4], 'strike': res[0][5], 'optionType': res[0][6], 'effect': 'close', 'action': 'buy'})
        r = robin_stocks.orders.order_option_spread('credit', limit, ticker, qty, spread, timeInForce='gtc', jsonify=True)
        print(r)
    
    def do_atmbuy(self, args):
        """Buy weekly options just out of the money."""
        
        print("Buy weeklies at the money.")
        print("Ticker: ")
        ticker = input(config.submenu_prompt)
        
        if ticker == 'exit':
            return
        
        ticker = ticker.upper()
        
        chains = robin_stocks.options.get_chains(ticker)['expiration_dates']
        s = ''
        i = 0
        for c in chains:
            n = '[' + str(i) + ']\t'
            n = n.expandtabs(7)
            d = c + '\t'
            d = d.expandtabs(8)
            s = s + n + d
            i = i + 1
            if i % 4 == 0:
                s = s + '\n'
        print(s)
        
        print("Select expiration (0,1,2,3...): ")
        exp_sel = int(input(config.submenu_prompt))
        exp_date = chains[exp_sel]
        print(exp_date)
        
        print("Type 'call' or 'put': ")
        call_put = input(config.submenu_prompt)

        print("Maximum amount to spend: $$$.$$")
        max_spend = input(config.submenu_prompt)
        

if __name__ == '__main__':
    robin_stocks.login(config.username, config.password)
    
    os.system('tmux send-keys -t2 python3\ db.py Enter')
    
    prompt = OptionHoodCmd() 
    prompt.prompt = '> '
    prompt.cmdloop('OPTIONHOOD')