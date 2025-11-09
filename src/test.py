import os
import gzip
import zipfile
import numpy as np

# json object
search_bounds = {
    "20240212": {
        "lower": "4520",
        "upper": "5225"
    },
    "20240213": {
        "lower": "4520",
        "upper": "5225"
    },
    "20240214": {
        "lower": "4520",
        "upper": "5225"
    },
    "20240215": {
        "lower": "4570",
        "upper": "5225"
    },
    "20240216": {
        "lower": "3520",
        "upper": "5330"
    },
}

def remove_out_of_bound_files():
    # for each folder in the directory
    for folder in os.listdir("data/"):
        print(folder)
        files = []
        
        # for each file in the folder
        for file in os.listdir(os.path.join("data/", folder)):
            # remove first character
            file_name = file[1:]
            files.append(file_name)
        
        # remove duplicates
        print(f"Total files in {folder}: {len(files)}")
        files = list(set(files))
        print(f"Total files after removing duplicates in {folder}: {len(files)}")
        
        # sort files
        files = sorted(files)
        #print(files)
        
        # find lower bound
        lower_bound = int(files[0])
        for file in files:
            if (int(file) - lower_bound) != 5:
                lower_bound = int(file)
            else:
                break
        
        # find upper bound
        upper_bound = int(files[-1])
        for file in reversed(files):
            if (upper_bound - int(file)) != 5:
                upper_bound = int(file)
            else:
                break
        print(f"lower bound: {lower_bound}, upper bound: {upper_bound}")
        
        # for each file in folder
        for file in os.listdir(os.path.join("data/", folder)):
            # remove first character
            file_name = file[1:]
            files.append(file_name)
            
            # if file is not in bounds, delete it
            if int(file_name) < lower_bound or int(file_name) > upper_bound:
                os.remove(os.path.join("data/", folder, file))
                
'''
finds mid price closest to the timestamp
'''
def get_mid_price(date, file_name, timestamp):
    try:
        # open file
        with zipfile.ZipFile(f'data/{date}.zip', 'r') as zf:
            with zf.open(f'{date}/{file_name}') as cf:
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
def find_bearish_call_spreads(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, lower_bound, upper_bound):
    spreads = []
    
    for short_strike in range(upper_bound, lower_bound, -5):
        long_strike = short_strike + spread_width
        
        # get mid price    
        short_strike = ('C' + str(short_strike))
        long_strike = ('C' + str(long_strike))
        short_strike_price = get_mid_price(date, short_strike, timestamp_of_entry)
        long_strike_price = get_mid_price(date, long_strike, timestamp_of_entry)

        # calculate credit received
        credit_received = None
        if short_strike_price is not None and long_strike_price is not None:
            credit_received = round(short_strike_price - long_strike_price, 3)
        
        # check if credit received meets entry credit
        if credit_received is not None and credit_received >= entry_credit:
            spreads.append((short_strike, long_strike, credit_received))
            if len(spreads) == num_spreads:
                return spreads
    return spreads

'''
short put: sell, higher strike, want to expire worthless
long put: buy, lower strike, caps max loss if price crashes
'''
def find_bullish_put_spreads(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, lower_bound, upper_bound):
    spreads = []
    
    for long_strike in range(lower_bound, upper_bound, 5):
        short_strike = long_strike + spread_width
        
        # get mid price
        long_strike = ('P' + str(long_strike))
        short_strike = ('P' + str(short_strike))
        long_strike_price = get_mid_price(date, long_strike, timestamp_of_entry)
        short_strike_price = get_mid_price(date, short_strike, timestamp_of_entry)
        
        # calculate credit received
        credit_received = None
        if long_strike_price is not None and short_strike_price is not None:
            credit_received = round(short_strike_price - long_strike_price, 3)
        
        # check if credit received meets entry credit        
        if credit_received is not None and credit_received >= entry_credit:
            spreads.append((long_strike, short_strike, credit_received))
            if len(spreads) == num_spreads:
                return spreads
    return spreads

def stop_limit_order(date, lower_strike, upper_strike, entry_time, stop_price, limit_price, option_type):
    # load data from files
    def load_option(strike, option_type):
        prefix = 'C' if option_type == 'call' else 'P'
        with zipfile.ZipFile(f'data/{date}.zip', 'r') as zf:
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
            if option_type == 'call':
                current_pos = cur_mid1 - cur_mid2  # lower - upper
            elif option_type == 'put':
                current_pos = cur_mid2 - cur_mid1  # upper - lower

            # stop limit triggered
            if current_pos > stop_price:
                stop_triggered = True

            # exit condition
            if stop_triggered and current_pos < stop_price and current_pos > limit_price:
                current_time = min(cur_time1, cur_time2)
                return current_time, round(current_pos, 3)
    return None, None

def stop_loss(date, lower_strike, upper_strike, timestamp, entry_credit, stop_multiplier, option_type):
    # load data from files
    def load_option(strike, option_type):
        prefix = 'C' if option_type == 'call' else 'P'
        with zipfile.ZipFile(f'data/{date}.zip', 'r') as zf:
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
            if option_type == 'call':
                current_pos = cur_mid1 - cur_mid2  # lower - upper
            elif option_type == 'put':
                current_pos = cur_mid2 - cur_mid1  # upper - lower

            # check if stop loss triggered
            if current_pos > starting_pos:
                current_time = min(cur_time1, cur_time2)
                return current_time, round(current_pos, 3)
    return None, None

def veic(timestamp_of_entry, spread_width, entry_credit, num_spreads, stop_price, limit_price, sl_mult):
    total_profit = 0
    
    for zip_file in os.listdir("data/"):
        # extract date
        date = zip_file.replace(".zip", "")
        print(date)
        
        # load bounds
        lower_bound = int(search_bounds[date]["lower"])
        upper_bound = int(search_bounds[date]["upper"])
        
        # build spreads
        call_spreads = find_bearish_call_spreads(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, lower_bound, upper_bound)
        put_spreads = find_bullish_put_spreads(date, timestamp_of_entry, entry_credit, spread_width, num_spreads, lower_bound, upper_bound)
        spreads = call_spreads + put_spreads
        
        # process spreads
        for i, (lower_strike, upper_strike, spread_credit) in enumerate(spreads):
            is_call = i < len(call_spreads)
            
            slo_et, slo_ec = stop_limit_order(date, int(lower_strike[1:]), int(upper_strike[1:]), timestamp_of_entry, stop_price, limit_price, 'call' if is_call else 'put')
            
            if slo_et is not None:
                sl_et, sl_ec = stop_loss(date, int(lower_strike[1:]), int(upper_strike[1:]), slo_et, slo_ec, sl_mult, 'call' if is_call else 'put')
                                
                profit = 0
                if sl_ec is not None:
                    profit = slo_ec - sl_ec
                    total_profit += profit * 100
                else:
                    profit = slo_ec
                    total_profit += profit * 100
    return round(total_profit, 2)

print(veic(120000000, 30, 1.5, 5, 1.1, 0.8, 2.0))
#print(stop_loss("20240212", 5045, 5075, 100019583, 1.525, 2.0, 'call'))