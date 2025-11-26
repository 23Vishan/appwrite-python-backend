from datetime import date
import os
import gzip
import zipfile
import numpy as np
from io import BytesIO

MAX_RANGE = 50
BUFFER = 0

# json object
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

def remove_out_of_bound_files():
    acceptable_range = 500
    
    # for each folder in the directory
    for folder in os.listdir("src/data/"):
        print(folder)
        
        # for each file in the folder
        for file in os.listdir(os.path.join("src/data/", folder)):
            # remove first character
            strike_price = file[1:]
            
            # get search bounds for date
            price = search_bounds[folder]["lower"]
            
            # remove if out of bounds
            if (int(strike_price) < price - acceptable_range or int(strike_price) > price + acceptable_range):
                file_path = os.path.join("src/data/", folder, file)
                os.remove(file_path)
                print(f"Removed {file_path}")

def reduce_file_size():
    base_path = "src/data/"
    filtered_path = "src/filtered_data/"
    os.makedirs(filtered_path, exist_ok=True)
    
    # for each date folder
    for folder in os.listdir(base_path):
        folder_path = os.path.join(base_path, folder)
        
        # skip non-folders
        if not os.path.isdir(folder_path):
            continue
        print(folder)

        # create filtered folder
        filtered_folder_path = os.path.join(filtered_path, folder)
        os.makedirs(filtered_folder_path, exist_ok=True)

        # for each binary file
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)

            # open raw binary file
            with gzip.open(file_path, 'rb') as gz_in:
                data = np.frombuffer(gz_in.read(), dtype=[('time', 'i4'), ('mid', 'f4')])
                times = data['time']
                mids = data['mid']

                # async filtering
                keep = [0]
                last_time = times[0]
                for i in range(1, len(times)):
                    # 60 sec async data
                    if times[i] - last_time >= 60000:
                        keep.append(i)
                        last_time = times[i]

                filtered_data = np.zeros(len(keep), dtype=[('time', 'i4'), ('mid', 'f4')])
                filtered_data['time'] = times[keep]
                filtered_data['mid'] = mids[keep]
                
            # write filtered binary file
            filtered_file_path = os.path.join(filtered_folder_path, file)
            with gzip.open(filtered_file_path, mode='wb') as gz_out:
                gz_out.write(filtered_data.tobytes())

class Spread:
    def __init__(self, short_strike, long_strike, credit, call_or_put):
        self.short_strike = short_strike
        self.long_strike = long_strike
        self.credit = credit
        self.call_or_put = call_or_put
    
    def __str__(self):
        return f"{self.call_or_put} spread: {self.short_strike}/{self.long_strike} @ {self.credit:.2f}"
        
'''
finds mid price closest to the timestamp
'''
def get_mid_price(date, file_name, timestamp, call_or_put):
    try:
        # open file
        with zipfile.ZipFile(f'src/data/{date}.zip', 'r') as zf:
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
def stop_limit_order(date, short_strike, long_strike, entry_time, stop_price, limit_price, option_type):
    # load data from files
    def load_option(strike, option_type):
        prefix = 'C' if option_type == 'call' else 'P'
        with zipfile.ZipFile(f'src/data/{date}.zip', 'r') as zf:
            with zf.open(f'{date}/{prefix}{strike}') as cf:
                with gzip.GzipFile(fileobj=cf) as gz:
                    data = np.frombuffer(gz.read(), dtype=np.dtype([('time', 'i4'), ('mid', 'f4')]))
                    return data['time'], data['mid']
    times1, mids1 = load_option(short_strike, option_type)
    times2, mids2 = load_option(long_strike, option_type)

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
def stop_loss(date, short_strike, long_strike, timestamp, entry_credit, stop_multiplier, option_type):
    # load data from files
    def load_option(strike, option_type):
        prefix = 'C' if option_type == 'call' else 'P'
        with zipfile.ZipFile(f'src/data/{date}.zip', 'r') as zf:
            with zf.open(f'{date}/{prefix}{strike}') as cf:
                with gzip.GzipFile(fileobj=cf) as gz:
                    data = np.frombuffer(gz.read(), dtype=np.dtype([('time', 'i4'), ('mid', 'f4')]))
                    return data['time'], data['mid']
    times1, mids1 = load_option(short_strike, option_type)
    times2, mids2 = load_option(long_strike, option_type)

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
    total_profit = 0
    daily_profits = []
    profit_over_time, dates = [], []
    
    for zip_file in os.listdir("src/data/"):
        # extract date
        date = zip_file.replace(".zip", "")
        #print(date)
        
        # load bounds
        lower_bound = search_bounds[date]["lower"]
        upper_bound = search_bounds[date]["upper"]
        
        # build spreads
        call_spreads = find_bearish_call_spreads(date, monitor_time, monitor_credit, spread_width, num_spreads, upper_bound)
        put_spreads = find_bullish_put_spreads(date, monitor_time, monitor_credit, spread_width, num_spreads, lower_bound)
        
        # ensure number of call and put spreads are equal
        min_length = min(len(call_spreads), len(put_spreads))
        call_spreads = call_spreads[:min_length]
        put_spreads = put_spreads[:min_length]
        spreads = call_spreads + put_spreads
        
        '''
        for spread in spreads:
            print(spread)
        '''
        
        # process spreads
        todays_profit = 0
        for i, spread in enumerate(spreads):
            is_call = i < len(call_spreads)
            
            # enter position
            entry_time, entry_credit = stop_limit_order(date, spread.short_strike, spread.long_strike, monitor_time, stop_price, limit_price, 'call' if is_call else 'put')

            # if entered, attempt stop loss
            if entry_time is not None:
                sl_et, sl_ec = stop_loss(date, spread.short_strike, spread.long_strike, entry_time, entry_credit, sl_mult, 'call' if is_call else 'put')
                
                profit = 0
                if sl_ec is not None:
                    # loss
                    profit = entry_credit - sl_ec
                    total_profit += profit * 100
                    todays_profit += profit * 100
                else:
                    # profit
                    profit = entry_credit
                    total_profit += profit * 100
                    todays_profit += profit * 100
        
        daily_profits.append(round(todays_profit, 2))
        profit_over_time.append(round(total_profit, 2))
        dates.append(date)
    
    print("Dates:", dates)
    print("Profits:", daily_profits)
    print("Profit over time:", profit_over_time)
    return round(total_profit, 2)

#print(pw_veic(120000000, 30, 1.5, 5, 1.1, 0.8, 2.0))
print(pw_veic(90000000, 30, 1.3, 3, 1.2, 1.0, 2.0))
#print(stop_loss("20240212", 5045, 5075, 100019583, 1.525, 2.0, 'call'))