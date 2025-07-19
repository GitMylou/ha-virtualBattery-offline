import argparse
from datetime import datetime, timedelta, timezone
from dateutil import parser
import json
import os
import requests
import time

##################################################################################
################################## Configuration #################################
##################################################################################

# Home assistant configuration
HA_TOKEN = os.getenv("HA_TOKEN")
HA_URL = os.getenv("HA_URL")

HA_STAT_BATTERY_STOCK = "sensor.urbansolar_battery_stock"
HA_STAT_BATTERY_STOCK_NAME = "Urbansolar Battery Stock"

HA_STAT_BATTERY_OUT = "sensor.urbansolar_battery_out"
HA_STAT_BATTERY_OUT_NAME = "Urbansolar Battery Out"

HA_STAT_ENEDIS_OUT = "sensor.urbansolar_enedis_out"
HA_STAT_ENEDIS_OUT_NAME = "Urbansolar Enedis Out"

HA_STAT_CONSUMPTION_CURVE = "sensor.linky_hourly_consumption"
HA_STAT_CONSUMPTION_CURVE_NAME = "Linky Hourly Consumption"

HA_STAT_PROD_CURVE = "sensor.linky_hourly_injection"
HA_STAT_PROD_CURVE_NAME = "Linky Hourly Injection"

GMT_INT = 3
GMT="+03:00"

##################################################################################
################################ Helper functions ################################
##################################################################################

# Get long term statistics for specific date
#   sensorId: The Sensor ID
#   date: The date to get the statistics
#   endate: (Optional) The end date. If none is given, get only on statistic for date
def getStatistics(sensorId, date, endate = None):
    url = f"{HA_URL}/api/long_term_stats"
    headers = {
        "Authorization": f"Bearer {HA_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {
        "entity_id": sensorId,
        "datetime": str(date.isoformat(sep=' ')+GMT),
    }
    if endate:
        params["end_datetime"] = str(endate.isoformat(sep=' ')+GMT)

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        print("Found previous statistic for ", sensorId)
        return data["message"]
    else:
        print("No previous statistic found for ", sensorId)
        return None

##################################################################################
################################# Input Arguments ################################
##################################################################################
argparser = argparse.ArgumentParser(description="Update Virtual Battery statistics in HA for specified dates. If no date given, process data from yesterday.")
argparser.add_argument("--startDate", required=False, help="Start date in format YYYY-MM-DD")
argparser.add_argument("--endDate", required=False, help="End date in format YYYY-MM-DD")
args = argparser.parse_args()

##################################################################################
################################## Main script ###################################
##################################################################################

# Sanity checks
if not HA_TOKEN or not HA_URL:
    print("HA_TOKEN or HA_URL missing")
    exit(1)

# Get range of date to process
startDate = ""
endDate = ""
if args.startDate and args.endDate:
    try:
        startDate = datetime.strptime(args.startDate, "%Y-%m-%d")
        endDate = datetime.strptime(args.endDate, "%Y-%m-%d")
    except ValueError as e:
        print("Error parsing dates:", e)
        exit(1)
else:
    print("No date given. Processing data from yesterday.")
    yesterday = datetime.now() - timedelta(days=1)
    startDate = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    endDate = startDate + timedelta(days=1)
print("processing Data from ", startDate, " to ", endDate)

print()
print("Retrieving last stock of the virtual battery")
previousBatteryStock = getStatistics(HA_STAT_BATTERY_STOCK, startDate)
if not previousBatteryStock or not previousBatteryStock["state"]:
    print("ERROR: Battery stock not found")
    exit(1)
previousBatteryStock = previousBatteryStock["state"] * 1000
print("Found battery stock of ", previousBatteryStock, "Wh")

print()
print("Retrieving last battery out index")
previousBatteryOut = getStatistics(HA_STAT_BATTERY_OUT, startDate)
if not previousBatteryOut or not previousBatteryOut["sum"]:
    print("ERROR: Battery out not found. Starting from 0")
    previousBatteryOut = 0
else:
    previousBatteryOut = previousBatteryOut["sum"]
    print("Found battery out index ", previousBatteryOut, "Wh")

print()
print("Retrieving last Enedis out index")
previousEnedisOut = getStatistics(HA_STAT_ENEDIS_OUT, startDate)
if not previousEnedisOut or not previousEnedisOut["sum"]:
    print("ERROR: Enedis out not found. Starting from 0")
    previousEnedisOut = 0
else:
    previousEnedisOut = previousEnedisOut["sum"]
    print("Found Enedis out index ", previousEnedisOut, "Wh")

print()
print("Retrieving last injection data")
previousInjectionIndex = getStatistics(HA_STAT_PROD_CURVE, startDate)
if not previousInjectionIndex or not previousInjectionIndex["sum"]:
    print("ERROR: Previous Injection Index not found. Starting from 0")
    previousInjectionIndex = 0
else:
    previousInjectionIndex = previousInjectionIndex["sum"]
    print("Found Injection index ", previousInjectionIndex, "Wh")

injectionDataList = getStatistics(HA_STAT_PROD_CURVE, startDate + timedelta(hours=1), endDate + timedelta(hours=1))
if not injectionDataList:
    print("ERROR: Injection data not found")
    exit(1)

print()
print("Retrieving last consumption data")
previousConsumtionIndex = getStatistics(HA_STAT_CONSUMPTION_CURVE, startDate)
if not previousConsumtionIndex or not previousConsumtionIndex["sum"]:
    print("ERROR: Previous Injection Index not found. Starting from 0")
    previousConsumtionIndex = 0
else:
    previousConsumtionIndex = previousConsumtionIndex["sum"]
    print("Found Injection index ", previousConsumtionIndex, "Wh")
consumptionDataList = getStatistics(HA_STAT_CONSUMPTION_CURVE, startDate + timedelta(hours=1), endDate + timedelta(hours=1))
if not consumptionDataList:
    print("ERROR: Consumption data not found")
    exit(1)

print()
statsBatteryStockList = []
statsBatteryOutList = []
statsEnedisOutList = []
print("Starting from battery stock ", previousBatteryStock, "Wh")
# Loop on all dates
for injectionData, consumptionData in zip(injectionDataList, consumptionDataList):
    injectionDate = datetime.fromtimestamp(injectionData["start_ts"], tz=timezone(timedelta(hours=GMT_INT)))
    consumptionDate = datetime.fromtimestamp(consumptionData["start_ts"], tz=timezone(timedelta(hours=GMT_INT)))

    if injectionDate != consumptionDate:
        print("Error, injection and consumption date does not match")
        exit(1)
    
    print()
    print("Processing Date: ", injectionDate.strftime('%Y-%m-%d %H:%M:%S'))

    # Get injected and consumed data based on index
    injectedEnergy = injectionData["sum"] - previousInjectionIndex
    previousInjectionIndex = injectionData["sum"]
    consumedEnergy = consumptionData["sum"] - previousConsumtionIndex
    previousConsumtionIndex = consumptionData["sum"]
    print("Injected from Linky :", injectedEnergy, "Wh")
    print("Consumed from Linky :", consumedEnergy, "Wh")

    # Battery stock increase because of the injection
    previousBatteryStock += injectedEnergy
    print("Battery Stock after injection: ", previousBatteryStock)

    # Remove from battery stock, what we have consumed today.
    # If we have enough energy in the battery, just decrease stock
    if previousBatteryStock >= consumedEnergy:
        previousBatteryStock -= consumedEnergy
        previousBatteryOut += consumedEnergy
        print("Battery Stock after consumption: ", previousBatteryStock)

    # Else, we don't have enough energy in the battery. 
    # Takes what is available, and put the rest in enedis consumtion 
    else:
        energyTakenFromEnedis = consumedEnergy - previousBatteryStock
        previousBatteryOut += previousBatteryStock
        print("Energy taken from Enedis: ", energyTakenFromEnedis)
        previousEnedisOut += energyTakenFromEnedis
        previousBatteryStock = 0
        print("Battery Stock after consumption: ", previousBatteryStock)

    statsBatteryStockList.append({"start" : str(injectionDate.strftime('%Y-%m-%d %H:%M:%S')+GMT),
                                  "state" : previousBatteryStock})

    statsBatteryOutList.append({"start" : str(injectionDate.strftime('%Y-%m-%d %H:%M:%S')+GMT),
                                "sum" : previousBatteryOut})
    
    statsEnedisOutList.append({"start" : str(injectionDate.strftime('%Y-%m-%d %H:%M:%S')+GMT),
                               "sum" : previousEnedisOut})


print()
print("Push data to HA")
url = f"{HA_URL}/api/services/recorder/import_statistics"
headers = {
    "Authorization": f"Bearer {HA_TOKEN}",
    "Content-Type": "application/json"
}
payload = {
    "has_mean": False,
    "has_sum": True,
    "source": "recorder",
    "name": HA_STAT_BATTERY_OUT_NAME,
    "statistic_id": HA_STAT_BATTERY_OUT,
    "unit_of_measurement": "Wh",
    "stats": statsBatteryOutList
}
res = requests.post(url, headers=headers, json=payload)
if res.status_code in (200, 201):
    print(f"Data sent to Home Assistant ({HA_STAT_BATTERY_OUT})")
else:
    print(f"Error while sending data to Home Assistant ({HA_STAT_BATTERY_OUT}): {res.status_code} - {res.text}")

payload = {
    "has_mean": False,
    "has_sum": True,
    "source": "recorder",
    "name": HA_STAT_ENEDIS_OUT_NAME,
    "statistic_id": HA_STAT_ENEDIS_OUT,
    "unit_of_measurement": "Wh",
    "stats": statsEnedisOutList
}
res = requests.post(url, headers=headers, json=payload)
if res.status_code in (200, 201):
    print(f"Data sent to Home Assistant ({HA_STAT_ENEDIS_OUT})")
else:
    print(f"Error while sending data to Home Assistant ({HA_STAT_ENEDIS_OUT}): {res.status_code} - {res.text}")

payload = {
    "has_mean": False,
    "has_sum": False,
    "source": "recorder",
    "name": HA_STAT_BATTERY_STOCK_NAME,
    "statistic_id": HA_STAT_BATTERY_STOCK,
    "unit_of_measurement": "kWh",
    "stats": statsBatteryStockList
}
res = requests.post(url, headers=headers, json=payload)
if res.status_code in (200, 201):
    print(f"Data sent to Home Assistant ({HA_STAT_BATTERY_STOCK})")
else:
    print(f"Error while sending data to Home Assistant ({HA_STAT_BATTERY_STOCK}): {res.status_code} - {res.text}")

