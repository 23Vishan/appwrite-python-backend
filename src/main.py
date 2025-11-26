import json
import os
import gzip
import zipfile
import numpy as np
from datetime import datetime

MAX_RANGE = 50
BUFFER = 0
BASE_PATH = "function/src/data/"
#BASE_PATH = "src/data/"

search_bounds = {
    "20240201": {
        "lower": 4860-BUFFER,
        "upper": 4860+BUFFER
    },
    "20240202": {
        "lower": 4915-BUFFER,
        "upper": 4915+BUFFER
    },
    "20240205": {
        "lower": 4955-BUFFER,
        "upper": 4955+BUFFER
    },
    "20240206": {
        "lower": 4950-BUFFER,
        "upper": 4950+BUFFER
    },
    "20240207": {
        "lower": 4970-BUFFER,
        "upper": 4970+BUFFER
    },
    "20240208": {
        "lower": 4995-BUFFER,
        "upper": 4995+BUFFER
    },
    "20240209": {
        "lower": 5000-BUFFER,
        "upper": 5000+BUFFER
    },
    "20240212": {
        "lower": 5025-BUFFER,
        "upper": 5025+BUFFER
    },
    "20240213": {
        "lower": 4965-BUFFER,
        "upper": 4965+BUFFER
    },
    "20240214": {
        "lower": 4975-BUFFER,
        "upper": 4975+BUFFER
    },
    "20240215": {
        "lower": 5000-BUFFER,
        "upper": 5000+BUFFER
    },
    "20240216": {
        "lower": 5030-BUFFER,
        "upper": 5030+BUFFER
    },
    "20240220": {
        "lower": 4990-BUFFER,
        "upper": 4990+BUFFER
    },
    "20240221": {
        "lower": 4965-BUFFER,
        "upper": 4965+BUFFER
    },
    "20240222": {
        "lower": 5040-BUFFER,
        "upper": 5050+BUFFER
    },
    "20240223": {
        "lower": 5100-BUFFER,
        "upper": 5100+BUFFER
    },
    "20240226": {
        "lower": 5095-BUFFER,
        "upper": 5095+BUFFER
    },
    "20240227": {
        "lower": 5075-BUFFER,
        "upper": 5075+BUFFER
    },
    "20240228": {
        "lower": 5065-BUFFER,
        "upper": 5065+BUFFER
    },
    "20240229": {
        "lower": 5085-BUFFER,
        "upper": 5085+BUFFER
    },
}

class Spread:
    def __init__(self, short_strike, long_strike, credit, call_or_put):
        self.short_strike = short_strike
        self.long_strike = long_strike
        self.credit = credit
        self.call_or_put = call_or_put
    
    def __str__(self):
        return f"{self.call_or_put} spread: {self.short_strike}/{self.long_strike} @ {self.credit:.2f}"

class TradeStats:
    def __init__(self):
        # overall stats
        self.total_profit = 0.0
        self.total_trades = 0
        self.win_count = 0
        self.lose_count = 0
        self.win_rate = 0.0
        self.max_daily_win = 0.0
        self.max_daily_loss = 0.0
        
        # graph data
        self.dates = []
        self.profit_over_time = []
        self.daily_losses = []
        self.daily_profits = []
        
        # spread data
        self.spread_date = []
        self.spread_type = []
        self.spread_spread = []
        self.spread_credit_at_open = []
        self.spread_execution_time = []
        self.spread_execution_credit = []
        self.spread_stop_out_time = []
        self.spread_stop_out_price = []
        self.spread_profit = []
        
    def update_spread_stats(self, date, type, spread, credit_at_open, execution_time, execution_credit, stop_out_time, stop_out_price, profit):
        # format date
        date_obj = datetime.strptime(date, "%Y%m%d")
        formatted_date = date_obj.strftime("%b %d, %Y")
    
        self.spread_date.append(formatted_date)
        self.spread_type.append(type)
        self.spread_spread.append(spread)
        self.spread_credit_at_open.append(credit_at_open)
        self.spread_execution_time.append(f"{execution_time[0:2]}:{execution_time[2:4]}")
        self.spread_execution_credit.append(execution_credit)
        self.spread_stop_out_time.append(f"{stop_out_time[0:2]}:{stop_out_time[2:4]}" if stop_out_time != "None" else "None")
        self.spread_stop_out_price.append(stop_out_price)
        self.spread_profit.append(round(profit, 2))
    
    def update_daily_stats(self, daily_loss, daily_profit, date):
        if (daily_loss) < self.max_daily_loss:
            self.max_daily_loss = round(daily_loss, 2)
        if (daily_profit) > self.max_daily_win:
            self.max_daily_win = round(daily_profit, 2)
        
        self.profit_over_time.append(round(self.total_profit, 2))
        self.daily_losses.append(round(daily_loss, 2))
        self.daily_profits.append(round(daily_profit, 2))
        self.dates.append(date)        
        
    def update_final_stats(self):
        self.win_rate = round((self.win_count / self.total_trades) * 100, 2) if self.total_trades > 0 else 0.0
        self.total_profit = round(self.total_profit, 2)
    
    def __str__(self):
        return (f"Total Profit: {self.total_profit}, "
                f"Total Trades: {self.total_trades}, "
                f"Wins: {self.win_count}, "
                f"Losses: {self.lose_count}, "
                f"Win Rate: {self.win_rate:.2f}%, "
                f"Max Daily Win: {self.max_daily_win}, "
                f"Max Daily Loss: {self.max_daily_loss}, "
                f"\n\nDates: {self.dates}, "
                f"\n\nProfit Over Time: {self.profit_over_time}, "
                f"\n\nDaily Losses: {self.daily_losses}, "
                f"\n\nDaily Profits: {self.daily_profits}, "
                
                f"\n\nSpread Dates: {self.spread_date}, "
                f"\n\nSpread Types: {self.spread_type}, "
                f"\n\nSpread Spreads: {self.spread_spread}, "
                f"\n\nSpread Credit at Open: {self.spread_credit_at_open}, "
                f"\n\nSpread Execution Times: {self.spread_execution_time}, "
                f"\n\nSpread Execution Credits: {self.spread_execution_credit}, "
                f"\n\nSpread Stop Out Times: {self.spread_stop_out_time}, "
                f"\n\nSpread Stop Out Prices: {self.spread_stop_out_price}, "
                f"\n\nSpread Profits: {self.spread_profit}")

'''
finds mid price closest to the timestamp
'''
def get_mid_price(date, file_name, timestamp, call_or_put):
    try:
        # open file
        with zipfile.ZipFile(f'{BASE_PATH}{date}.zip', 'r') as zf:
            prefix = 'C' if call_or_put == 'call' else 'P'
            with zf.open(f'{date}/{prefix}{file_name}') as cf:
                with gzip.GzipFile(fileobj=cf) as gz:
                    data = np.frombuffer(gz.read(), dtype=np.dtype([('time', 'i4'), ('mid', 'f4')]))
                    times = data['time']
                    mids = data['mid']
                    
                    # find price closed to timestamp
                    for i in range(len(times)):
                        if times[i] >= timestamp:
                            return mids[i]
    # if file not found
    except KeyError:
        return None

'''
short call: sell, lower strike, want to expire worthless
long call: buy, upper strike, caps max loss if price rises
'''
def find_bearish_call_spreads(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, upper_bound):
    spreads = []
    
    for short_strike in range(upper_bound, upper_bound+MAX_RANGE, 5):
        long_strike = short_strike + spread_width
        
        # get mid price    
        short_strike_price = get_mid_price(date, short_strike, timestamp_of_entry, 'call')
        long_strike_price = get_mid_price(date, long_strike, timestamp_of_entry, 'call')

        # calculate credit received
        credit_received = None
        if short_strike_price is not None and long_strike_price is not None:
            credit_received = round(short_strike_price - long_strike_price, 3)
        
        # check if credit received meets entry credit
        if credit_received is not None and credit_received >= entry_credit:
            spreads.append(Spread(short_strike, long_strike, credit_received, 'call'))
            if len(spreads) == num_spreads:
                return spreads
    return spreads

'''
short put: sell, higher strike, want to expire worthless
long put: buy, lower strike, caps max loss if price crashes
'''
def find_bullish_put_spreads(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, lower_bound):
    spreads = []
    
    for short_strike in range(lower_bound, lower_bound-MAX_RANGE, -5):
        long_strike = short_strike - spread_width
        
        # get mid price
        long_strike_price = get_mid_price(date, long_strike, timestamp_of_entry, 'put')
        short_strike_price = get_mid_price(date, short_strike, timestamp_of_entry, 'put')
        
        # calculate credit received
        credit_received = None
        if long_strike_price is not None and short_strike_price is not None:
            credit_received = round(short_strike_price - long_strike_price, 3)
        
        # check if credit received meets entry credit        
        if credit_received is not None and credit_received >= entry_credit:
            spreads.append(Spread(short_strike, long_strike, credit_received, 'put'))
            if len(spreads) == num_spreads:
                return spreads
    return spreads

'''
stop limit order for entry
'''
def stop_limit_order(date, lower_strike, upper_strike, entry_time, stop_price, limit_price, option_type):
    # load data from files
    def load_option(strike, option_type):
        prefix = 'C' if option_type == 'call' else 'P'
        with zipfile.ZipFile(f'{BASE_PATH}{date}.zip', 'r') as zf:
            with zf.open(f'{date}/{prefix}{strike}') as cf:
                with gzip.GzipFile(fileobj=cf) as gz:
                    data = np.frombuffer(gz.read(), dtype=np.dtype([('time', 'i4'), ('mid', 'f4')]))
                    return data['time'], data['mid']
    times1, mids1 = load_option(lower_strike, option_type)
    times2, mids2 = load_option(upper_strike, option_type)

    # exit if no data
    if len(times1) == 0 or len(times2) == 0:
        return None, None

    # initalize pointers
    index1 = index2 = 0
    cur_time1 = times1[0]
    cur_time2 = times2[0]
    cur_mid1 = mids1[0]
    cur_mid2 = mids2[0]

    # increment
    index1 = 1 if len(times1) > 1 else len(times1)
    index2 = 1 if len(times2) > 1 else len(times2)

    stop_triggered = False

    # iterate through both time series
    while index1 < len(times1) or index2 < len(times2):
        # get next timestamps
        next_time1 = times1[index1] if index1 < len(times1) else float('inf')
        next_time2 = times2[index2] if index2 < len(times2) else float('inf')

        # increment the earliest time
        if next_time1 <= next_time2:
            cur_time1 = next_time1
            cur_mid1 = mids1[index1]
            index1 += 1
        if next_time2 <= next_time1:
            cur_time2 = next_time2
            cur_mid2 = mids2[index2]
            index2 += 1
        
        # only start after entry time
        if cur_time1 > entry_time and cur_time2 > entry_time:
            # calculate current position
            current_pos = cur_mid1 - cur_mid2

            # stop limit triggered
            if current_pos > stop_price:
                stop_triggered = True

            # exit condition
            if stop_triggered and current_pos < stop_price and current_pos > limit_price:
                current_time = min(cur_time1, cur_time2)
                return current_time, round(current_pos, 3)
    return None, None

'''
stop loss for loss reduction
'''
def stop_loss(date, lower_strike, upper_strike, timestamp, entry_credit, stop_multiplier, option_type):
    # load data from files
    def load_option(strike, option_type):
        prefix = 'C' if option_type == 'call' else 'P'
        with zipfile.ZipFile(f'{BASE_PATH}{date}.zip', 'r') as zf:
            with zf.open(f'{date}/{prefix}{strike}') as cf:
                with gzip.GzipFile(fileobj=cf) as gz:
                    data = np.frombuffer(gz.read(), dtype=np.dtype([('time', 'i4'), ('mid', 'f4')]))
                    return data['time'], data['mid']
    times1, mids1 = load_option(lower_strike, option_type)
    times2, mids2 = load_option(upper_strike, option_type)

    # exit if no data
    if len(times1) == 0 or len(times2) == 0:
        return None, None

    # initalize pointers
    index1 = index2 = 0
    cur_time1 = times1[0]
    cur_time2 = times2[0]
    cur_mid1 = mids1[0]
    cur_mid2 = mids2[0]

    # increment
    index1 = 1 if len(times1) > 1 else len(times1)
    index2 = 1 if len(times2) > 1 else len(times2)
    starting_pos = entry_credit * stop_multiplier

    # iterate through both time series
    while index1 < len(times1) or index2 < len(times2):
        # get next timestamps
        next_time1 = times1[index1] if index1 < len(times1) else float('inf')
        next_time2 = times2[index2] if index2 < len(times2) else float('inf')

        # increment the earliest time
        if next_time1 <= next_time2:
            cur_time1 = next_time1
            cur_mid1 = mids1[index1]
            index1 += 1
        if next_time2 <= next_time1:
            cur_time2 = next_time2
            cur_mid2 = mids2[index2]
            index2 += 1
        
        # only start after timestamp
        if cur_time1 > timestamp and cur_time2 > timestamp:
            # calculate current position
            current_pos = cur_mid1 - cur_mid2

            # check if stop loss triggered
            if current_pos > starting_pos:
                current_time = min(cur_time1, cur_time2)
                return current_time, round(current_pos, 3)
    return None, None

'''
progressive wing variable entry iron condor
'''
def pw_veic(monitor_time, spread_width, monitor_credit, num_spreads, stop_price, limit_price, sl_mult):
    trade_stats = TradeStats()
    
    for zip_file in sorted(os.listdir(f"{BASE_PATH}")):
        # extract date
        date = zip_file.replace(".zip", "")
        
        # load bounds
        lower_bound = int(search_bounds[date]["lower"])
        upper_bound = int(search_bounds[date]["upper"])
        
        # build spreads
        call_spreads = find_bearish_call_spreads(date, monitor_time, monitor_credit, spread_width, num_spreads, upper_bound)
        put_spreads = find_bullish_put_spreads(date, monitor_time, monitor_credit, spread_width, num_spreads, lower_bound)

        # ensure number of call and put spreads are equal
        min_length = min(len(call_spreads), len(put_spreads))
        call_spreads = call_spreads[:min_length]
        put_spreads = put_spreads[:min_length]
        spreads = call_spreads + put_spreads
        
        # process spreads
        daily_profit, daily_loss = 0, 0
        for i, spread in enumerate(spreads):
            is_call = i < len(call_spreads)
            
            entry_time, entry_credit = stop_limit_order(date, int(spread.short_strike), int(spread.long_strike), monitor_time, stop_price, limit_price, 'call' if is_call else 'put')
            
            if entry_time is not None:
                sl_et, sl_ec = stop_loss(date, int(spread.short_strike), int(spread.long_strike), entry_time, entry_credit, sl_mult, 'call' if is_call else 'put')
                trade_stats.total_trades += 1

                profit = 0
                if sl_ec is not None:
                    # loss
                    profit = entry_credit - sl_ec
                    trade_stats.total_profit += profit * 100

                    trade_stats.lose_count += 1
                    daily_loss += profit * 100
                else:
                    # win
                    profit = entry_credit
                    trade_stats.total_profit += profit * 100
                    
                    trade_stats.win_count += 1
                    daily_profit += profit * 100
                    
                # update spread stats
                trade_stats.update_spread_stats(str(date), 'Call' if is_call else 'Put', f"{spread.short_strike} / {spread.long_strike}", str(spread.credit), str(entry_time), str(entry_credit), str(sl_et) if sl_et is not None else "None", str(sl_ec) if sl_ec is not None else "None", profit * 100)
        
        # update stats
        trade_stats.update_daily_stats(daily_loss, daily_profit, date)
    trade_stats.update_final_stats()
    return trade_stats

def main(context):
    try:
        print("\n\nfunction started")
        print(f"\nraw body: '{context.req.body_raw}'")

        # check if empty
        if not context.req.body_raw or context.req.body_raw.strip() == "":
            print("\nempty request body")
            return context.res.json({"error": "Empty request body"})

        # parsed data
        data = json.loads(context.req.body_raw)
        print(f"\nparsed data: {data}")
        
        # extract data
        entry_time = data.get("entryTime")
        spread_width = data.get("spreadWidth")
        entry_credit = data.get("entryCredit")
        number_of_spreads = data.get("numberOfSpreads")
        stop_price = data.get("stopPrice")
        limit_price = data.get("limitPrice")
        stop_loss_multiplier = data.get("stopLossMultiplier")

        # log extracted values
        print(f"\nentryTime: {entry_time}, spreadWidth: {spread_width}, entryCredit: {entry_credit}")
        print(f"numberOfSpreads: {number_of_spreads}, stopPrice: {stop_price}, limitPrice: {limit_price}, stopLossMultiplier: {stop_loss_multiplier}")
        
        # reformat entry time, 9:30 AM -> 93000000
        entry_time = entry_time.replace(":", "")
        entry_time = entry_time + "00000"
        
        # call veic
        trade_stats = pw_veic(int(entry_time), int(spread_width), float(entry_credit), int(number_of_spreads), float(stop_price), float(limit_price), float(stop_loss_multiplier))
        print(trade_stats)
        
        # return response
        return context.res.json({
            "response": {
                "totalProfit": trade_stats.total_profit,
                "dates": trade_stats.dates,
                "profitOverTime": trade_stats.profit_over_time,
                "totalTrades": trade_stats.total_trades,
                "winCount": trade_stats.win_count,
                "loseCount": trade_stats.lose_count,
                "winRate": trade_stats.win_rate,
                "maxDailyWin": trade_stats.max_daily_win,
                "maxDailyLoss": trade_stats.max_daily_loss,
                "dailyLosses": trade_stats.daily_losses,
                "dailyProfits": trade_stats.daily_profits,
                "spreadData": [
                    {
                        "spreadDate": a,
                        "spreadType": b,
                        "spreadSpread": c,
                        "spreadCreditAtOpen": d,
                        "spreadExecutionTime": e,
                        "spreadExecutionCredit": f,
                        "spreadStopOutTime": g,
                        "spreadStopOutPrice": h,
                        "spreadProfit": i
                    }
                    
                    for a, b, c, d, e, f, g, h, i in zip(
                        trade_stats.spread_date,
                        trade_stats.spread_type,
                        trade_stats.spread_spread,
                        trade_stats.spread_credit_at_open,
                        trade_stats.spread_execution_time,
                        trade_stats.spread_execution_credit,
                        trade_stats.spread_stop_out_time,
                        trade_stats.spread_stop_out_price,
                        trade_stats.spread_profit
                    )
                ]
            }
        })
    except Exception as e:
        print(f"error: {str(e)}")
        return context.res.json({"error": str(e)})

#print(pw_veic(90000000, 30, 1.3, 3, 1.2, 1.0, 2.0))
#trade_stats = veic(93000000, 10, 1.0, 2, 3.0, 2.5, 1.5)
#print(trade_stats)