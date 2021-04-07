import urwid
import sys
import time
import config
import db

palette = [
    ('titlebar', 'light blue,bold', ''),
    ('headers', 'yellow,bold', ''),
    ('subheader', 'black', 'light gray'),
    ('time', 'dark green', ''),
    ('main', 'white', ''),
    ('bid', 'light green, bold', ''),
    ('ask', 'light red, bold', ''),
    ('leg1', 'black', 'light gray'),
    ('leg2', 'black', 'dark cyan'),
    ('ah', 'yellow,bold', ''),
    ('sh', 'light blue', 'white')]

def account_line():
    act_line = []
    
    con = db.db_connection()
    cur = con.cursor()

    q = 'SELECT buying_power, epoch_update, day_trades, equity_expiries, option_expiries FROM account WHERE id=1;'

    cur.execute(q)
    d = cur.fetchall()

    buying_power = str(round(float(d[0][0]), 2))
    day_trades = str(d[0][2])
    last_update = d[0][1]
    equity_expiries = d[0][3]
    option_expiries = d[0][4]

    act_line.append(('ah', 'BP: '))
    act_line.append(('ac', buying_power))
    act_line.append(('ah', '   DT: '))
    act_line.append(('ac', day_trades))
    if equity_expiries != '':
        act_line.append(('ah', '  EDT: '))
        act_line.append(('ac', equity_expiries))
    if option_expiries != '':
        act_line.append(('ah', '  ODT: '))
        act_line.append(('ac', option_expiries))
        
    time_dif = time.time() - last_update
    if time_dif < (config.RH_REQUEST_INTERVAL):
        act_line.append(('bid', '  \u2589'))
    else:
        act_line.append(('ask', '  \u2589'))

    act_line.append(('main','\n'))

    cur.close()
    con.close()
    
    return(act_line)

class Panel:
    def __init__(self, panel, title):
        try:
            self.data_routine = eval('self.' + panel)
        except AttributeError:
            sys.exit()
            
        self.last_refresh_time = time.time()

        #build window
        self.header_text = urwid.Text(' ' + title)
        self.header = urwid.AttrMap(self.header_text, 'titlebar')
        self.quote_text = urwid.Text('Something is wrong.')
        self.quote_color = urwid.AttrMap(self.quote_text, 'main')
        self.quote_filler = urwid.Filler(self.quote_color, valign='top', top=0, bottom=0)
        self.filler_color = urwid.AttrMap(self.quote_filler, 'main')
        self.v_padding = urwid.Padding(self.filler_color, left=1, right=1)
        self.v_color = urwid.AttrMap(self.v_padding, 'main')
        self.layout = urwid.Frame(header=self.header, body=self.v_color)

        #main loop setup and run
        self.main_loop = urwid.MainLoop(self.layout, palette, unhandled_input=self.handle_input)
        self.main_loop.set_alarm_in(0, self.refresh)
        self.main_loop.run()

    def refresh(self, _loop, _data):
        self.main_loop.draw_screen()
        self.v_padding.base_widget.set_text(self.data_routine())
        self.main_loop.set_alarm_in(config.TMUX_REFRESH_INTERVAL, self.refresh)

    def handle_input(self, key):
        if key == 'Q' or key == 'q':
            raise urwid.ExitMainLoop()

    def positions(self):
        try:
            updates = account_line()

            updates.append(('headers', 'ID\t'.expandtabs(5)))
            updates.append(('headers', 'TICKER\t'.expandtabs(8)))
            updates.append(('headers', 'PRICE\t'.expandtabs(8)))
            updates.append(('headers', 'TYPE\t'.expandtabs(6)))
            updates.append(('headers', 'EXP.\t'.expandtabs(12)))
            updates.append(('headers', 'STRIKE\t'.expandtabs(8)))
            updates.append(('headers', 'QTY\t'.expandtabs(5)))
            updates.append(('headers', 'COST\t'.expandtabs(7)))
            updates.append(('headers', 'BID\t'.expandtabs(12)))
            updates.append(('headers', 'ASK\t'.expandtabs(12)))
            updates.append(('headers', 'IV\t'.expandtabs(6)))
            updates.append(('subheader', ' STRIKE\t'.expandtabs(8)))
            updates.append(('subheader', 'QTY\t'.expandtabs(5)))
            updates.append(('subheader', 'COST\t'.expandtabs(6)))
            updates.append(('subheader', 'BID\t'.expandtabs(12)))
            updates.append(('subheader', 'ASK\t'.expandtabs(12)))
            updates.append(('sh', ' BID\t'.expandtabs(8)))
            updates.append(('sh', 'MARK\t'.expandtabs(8)))
            updates.append(('sh', 'ASK\t\n'.expandtabs(8)))
            #updates.append(('headers', ' STATUS\t'.expandtabs(4) + '\n'))

            #data processing
            con = db.db_connection()
            cur = con.cursor()

            query = "SELECT local_id, l_underlying, l_callput, l_expiration, l_strike_price, l_quantity, l_average_price, l_bid_price, l_bid_size, l_ask_price, l_ask_size, l_implied_volatility, l_above_tick, l_below_tick, l_cutoff_price, s_quantity, s_average_price, s_bid_price, s_bid_size, s_ask_price, s_ask_size, s_above_tick, s_below_tick, s_cutoff_price, underlying_price, epoch_update_underlying_market_data, epoch_update_uncapped_shorts, epoch_update_long_position_market_data, epoch_update_short_position_market_data, s_strike_price FROM positions;"
            
            cur.execute(query)
            rows=cur.fetchall()

            #sort tasks numberically by local_id
            rows.sort(key=lambda x: x[0])
                
            for row in rows:
                try:
                    pid = str(row[0]) + '\t'
                except:
                    pid = '\t'
                try:
                    l_underlying = str(row[1]) + '\t'
                except:
                    l_underlying = '\t'
                try:
                    underlying_price = str(round(float(row[24]),2)) + '\t'
                except:
                    underlying_price = "\t"
                try:
                    l_callput = str(row[2]) + '\t'
                except:
                    l_callput = '\t'
                try:
                    l_expiration = str(row[3]) + '\t'
                except:
                    l_expiration = '\t'
                try:
                    l_strike = str(round(float(row[4]), 1)) + '\t'
                except:
                    l_strike = '\t'
                try:
                    l_quantity = str(int(row[5])) + '\t'
                except:
                    l_quantity = '\t'
                try:
                    l_average_price = str(round((row[6]*.01), 2)) + '\t'
                except:
                    l_average_price = '\t'
                try:
                    l_bid_price = str(row[7])
                except:
                    l_bid_price = ''
                try:
                    l_bid = l_bid_price + 'x' + str(int(row[8])) + '\t'
                except:
                    l_bid = '\t'
                try:
                    l_ask_price = str(row[9])
                except:
                    l_ask_price = ''
                try:
                    l_ask = l_ask_price + 'x' + str(int(row[10])) + '\t'
                except:
                    l_ask = '\t'
                try:
                    l_implied_volatility = str(round(row[11],2)) + '\t'
                except:
                    l_implied_volatility = '\t'
                try:
                    l_above_tick = str(row[12]) + '\t'
                except:
                    l_above_tick = '\t'
                try:
                    l_below_tick = str(row[13]) + '\t'
                except:
                    l_below_tick = '\t'
                try:
                    l_cutoff_price = str(row[14]) + '\t'
                except:
                    l_cutoff_price = '\t'
                if row[15] == None:
                    s_quantity = str(0) + '\t'
                else:
                    s_quantity = str(int(row[15])) + '\t'

                try:
                    s_average_price = str(round((row[16]*.01), 2)) + '\t'
                except:
                    s_average_price = "\t"
                try:   
                    s_bid_price = str(row[17])
                except:
                    s_bid_price = ''
                try:
                    s_bid = s_bid_price + 'x' + str(int(row[18])) + '\t'
                except:
                    s_bid = '\t'
                try:
                    s_ask_price = str(row[19])
                except:
                    s_ask_price = ''
                try:
                    s_ask = s_ask_price + 'x' + str(int(row[20])) + '\t'
                except:
                    s_ask = '\t'
                try:
                    s_above_tick = str(row[21]) + '\t'
                except:
                    s_above_tick = '\t'
                try:
                    s_below_tick = str(row[22]) + '\t'
                except:
                    s_below_tick = '\t'
                try:
                    s_cutoff_price = str(row[23]) + '\t'
                except:
                    s_cutoff_price = '\t'
                    
                try:
                    s_strike = ' ' + str(round(float(row[29]), 1)) + '\t'
                except:
                    s_strike = ' \t'

                try:
                    s_low = ' ' + str(round(row[7] - row[19], 2)) + '\t'
                except:
                    s_low = ' \t'

                try:
                    low = row[7] - row[19]
                    high = row[9] - row[17]
                    mid = (low + high) / 2
                    s_mid = str(round(mid, 2)) + '\t'
                except:
                    s_mid = '\t'

                try:
                    s_high = str(round(row[9] - row[17], 2)) + '\t'
                except:
                    s_high = '\t'


                updates.append(('main', pid.expandtabs(5)))
                updates.append(('main', l_underlying.expandtabs(8)))
                updates.append(('main', underlying_price.expandtabs(8)))
                updates.append(('main', l_callput.expandtabs(6)))
                updates.append(('main', l_expiration.expandtabs(12)))
                updates.append(('main', l_strike.expandtabs(8)))
                updates.append(('main', l_quantity.expandtabs(5)))
                updates.append(('main', l_average_price.expandtabs(7)))
                updates.append(('bid', l_bid.expandtabs(12)))
                updates.append(('ask', l_ask.expandtabs(12)))
                updates.append(('main', l_implied_volatility.expandtabs(6)))
                updates.append(('main', s_strike.expandtabs(8)))
                updates.append(('main', s_quantity.expandtabs(5)))
                updates.append(('main', s_average_price.expandtabs(6)))
                updates.append(('bid', s_bid.expandtabs(12)))
                updates.append(('ask', s_ask.expandtabs(12)))
                updates.append(('main', s_low.expandtabs(8)))
                updates.append(('main', s_mid.expandtabs(8)))
                updates.append(('main', s_high.expandtabs(8)))
            
                signal = config.RH_REQUEST_INTERVAL - 1
                now = time.time()
                if (now - row[25]) > signal:
                    updates.append(('ask', ' \u2589'))
                else:
                    updates.append(('bid', ' \u2589'))
                if (now - row[26]) > signal:
                    updates.append(('ask', '\u2589'))
                else:
                    updates.append(('bid', '\u2589'))
                if (now - row[27]) > signal:
                    updates.append(('ask', '\u2589'))
                else:
                    updates.append(('bid', '\u2589'))
                if (now - row[28]) > signal:
                    updates.append(('ask', '\u2589\n'))
                else:
                    updates.append(('bid', '\u2589\n'))
                
            return(updates)
        except:
            return('')

    def orders(self):
        try:
            #data processing
            con = db.db_connection()
            cur = con.cursor()
            query = "SELECT local_id, chain_symbol, stop_price, price, quantity, pending_quantity, processed_quantity, leg_1_call_put, leg_1_strike, leg_1_expiration, leg_1_bid_price, leg_1_bid_size, leg_1_ask_price, leg_1_ask_size, leg_2_call_put, leg_2_strike, leg_2_expiration, leg_2_bid_price, leg_2_bid_size, leg_2_ask_price, leg_2_ask_size, time_in_force, opening_strategy, closing_strategy, epoch_update_orders, epoch_update_instrument_data, epoch_update_orders_market_data FROM orders;"
            cur.execute(query)
            rows=cur.fetchall()

            #sort tasks numberically by local_id
            rows.sort(key=lambda x: x[0])
            
            updates = [
                ('headers', 'ID\t'.expandtabs(3)),
                ('headers', 'TICKER\t'.expandtabs(7)),
                ('headers', 'STOP\t'.expandtabs(5)),
                ('headers', 'LIMIT\t'.expandtabs(6)),
                ('headers', 'QTY\t'.expandtabs(4)),
                ('headers', 'PEND\t'.expandtabs(5)),
                ('headers', 'PROC\t'.expandtabs(5)),
                ('leg1', 'TYPE1\t'.expandtabs(6)),
                ('leg1', 'STRK1\t'.expandtabs(8)),
                ('leg1', 'EXP1\t'.expandtabs(11)),
                ('leg1', 'BID1\t'.expandtabs(12)),
                ('leg1', 'ASK1\t'.expandtabs(12)),
                ('leg2', 'TYPE2\t'.expandtabs(6)),
                ('leg2', 'STRK2\t'.expandtabs(8)),
                ('leg2', 'EXP2\t'.expandtabs(11)),
                ('leg2', 'BID2\t'.expandtabs(12)),
                ('leg2', 'ASK2\t'.expandtabs(12)),
                ('headers', ' TIF\t'.expandtabs(4)),
                ('headers', 'OPEN\t'.expandtabs(13)),
                ('headers', 'CLOSE\t\n'.expandtabs(13))]

            for row in rows:
                try:
                    oid = str(row[0]) + '\t'
                except:
                    oid = '\t'

                try:
                    ticker = str(row[1]) + '\t'
                except:
                    ticker = '\t'

                if row[2] != None:
                    stop = str(row[2]) + '\t'
                else:
                    stop = '\t'

                try:
                    limit = str(row[3]) + '\t'
                except:
                    limit = '\t'

                try:
                    qty = str(int(row[4])) + '\t'
                except:
                    qty = '\t'

                try:
                    pend = str(int(row[5])) + '\t'
                except:
                    pend = '\t'

                try:
                    proc = str(int(row[6])) + '\t'
                except:
                    proc = '\t'

                l1type      = '\t'
                l1strike    = '\t'
                l1exp       = '\t'
                l1bid       = '\t'
                l1ask       = '\t'

                if row[10] != None:
                    l1type      = str(row[7]) + '\t'
                    l1strike    = str(row[8]) + '\t'
                    l1exp       = str(row[9]) + '\t'
                    l1bid       = str(row[10]) + 'x' + str(int(row[11])) + '\t'
                    l1ask       = str(row[12]) + 'x' + str(int(row[13])) + '\t'

                l2type      = '\t'
                l2strike    = '\t'
                l2exp       = '\t'
                l2bid       = '\t'
                l2ask       = '\t'

                if row[17] != None:
                    l2type      = str(row[14]) + '\t'
                    l2strike    = str(row[15]) + '\t'
                    l2exp       = str(row[16]) + '\t'
                    l2bid       = str(row[17]) + 'x' + str(int(row[18])) + '\t'
                    l2ask       = str(row[19]) + 'x' + str(int(row[20])) + '\t'

                try:
                    timeif      = ' ' + str(row[21]) + '\t'
                except:
                    timeif = '\t'

                try:
                    open_strat  = str(row[22]) + '\t' 
                except:
                    open_strat = '\t'

                try:
                    close_strat = str(row[23]) + '\t'
                except:
                    close_start = '\t'

                status = ''

                updates.append(('main', oid.expandtabs(3)))
                updates.append(('main', ticker.expandtabs(7)))
                updates.append(('main', stop.expandtabs(5)))
                updates.append(('main', limit.expandtabs(6)))
                updates.append(('main', qty.expandtabs(4)))
                updates.append(('main', pend.expandtabs(5)))
                updates.append(('main', proc.expandtabs(5)))
                updates.append(('main', l1type.expandtabs(6)))
                updates.append(('main', l1strike.expandtabs(8)))
                updates.append(('main', l1exp.expandtabs(11)))
                updates.append(('bid', l1bid.expandtabs(12)))
                updates.append(('ask', l1ask.expandtabs(12)))
                updates.append(('main', l2type.expandtabs(6)))
                updates.append(('main', l2strike.expandtabs(8)))
                updates.append(('main', l2exp.expandtabs(11)))
                updates.append(('bid', l2bid.expandtabs(12)))
                updates.append(('ask', l2ask.expandtabs(12)))
                updates.append(('main', timeif.expandtabs(4)))
                updates.append(('main', open_strat.expandtabs(13)))
                updates.append(('main', close_strat.expandtabs(13)))

                signal = config.RH_REQUEST_INTERVAL
                now = time.time()
                if (now - row[24]) > signal:
                    updates.append(('ask', '\u2589'))
                else:
                    updates.append(('bid', '\u2589'))
                if (now - row[25]) > signal:
                    updates.append(('ask', '\u2589'))
                else:
                    updates.append(('bid', '\u2589'))
                if (now - row[26]) > signal:
                    updates.append(('ask', '\u2589\n'))
                else:
                    updates.append(('bid', '\u2589\n'))
                
            return updates
        except:
            return 'Something went wrong....'

if __name__ == "__main__":
    if sys.argv[1] == 'positions':
        p = Panel('positions', 'POSITIONS')
    if sys.argv[1] == 'orders':
        p = Panel('orders', 'ORDERS')