import json

import os
import gzip
import zipfile
import numpy as np

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
                
        # return response
        return context.res.json({
            "response": "hello",
        })
    except Exception as e:
        print(f"error: {str(e)}")

        return context.res.json({"error": str(e)})    
