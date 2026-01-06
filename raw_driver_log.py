from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import json

# -------- LOAD ENV VARIABLES --------

load_dotenv()

# ---------------- SSH CONFIG ----------------
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")

# ---------------- MONGO CONFIG ----------------
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = urllib.parse.quote_plus(os.getenv("MONGO_PASSWORD"))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT"))


# -------- INPUT DRIVER ID --------
DRIVER_ID = ObjectId("68e4dc0fb56bc4691e8bdff4")
# DRIVER_ID=ObjectId("68496a5ff38dc2543d248051")

# -------- HELPER FUNCTION --------
def parse_event_date(event_date_str):
    """
    Converts 'MMDDYY' -> datetime
    """
    try:
        return datetime.strptime(event_date_str, "%m%d%y")
    except Exception:
        return None


with SSHTunnelForwarder(
    (SSH_HOST, SSH_PORT),
    ssh_username=SSH_USER,
    ssh_password=SSH_PASSWORD,
    remote_bind_address=(MONGO_HOST, MONGO_PORT),
    local_bind_address=("localhost", 0),
) as tunnel:

    mongo_uri = (
        f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}"
        f"@localhost:{tunnel.local_bind_port}/{MONGO_DB}"
        f"?authSource=driverbookv2_stage"
    )

    client = MongoClient(mongo_uri)
    db = client[MONGO_DB]


    # -------- ALL RAW DRIVER LOGS --------
    raw_cursor = db.rawdriverlogs.find(
        {"driverId": DRIVER_ID},
        projection={
            "_id": 1,
            "driverId": 1,
            "rawData": 1,
            "date": 1,
            "expireAt": 1,
            "createdAt": 1,
            "updatedAt": 1,
        }
    ).sort("createdAt", ASCENDING)

    records = []

    def oid_to_str(oid):
        try:
            return str(oid)
        except Exception:
            return None

    for raw in raw_cursor:
        # top-level
        _id = oid_to_str(raw.get("_id"))
        driver = oid_to_str(raw.get("driverId"))
        date = raw.get("date")
        expire = raw.get("expireAt")

        # simple fields
        createdat = raw.get("createdAt")
        updateat = raw.get("updatedAt")

        # nested structures
        rawdata = raw.get("rawData")

        # rawData can be a dict (single event) or a list (multiple events)
        events = []
        if isinstance(rawdata, list):
            events = rawdata
        elif isinstance(rawdata, dict):
            events = [rawdata]
        else:
            # unknown type -> skip
            events = []

        for evt in events:
            # ensure evt is a dict
            if not isinstance(evt, dict):
                continue

            record = {
                "_id": _id,
                "driverid": driver,
                "date": date,
                "eventSequenceIdNumber": evt.get("eventSequenceIdNumber", 0),
                "eventRecordOrigin": evt.get("eventRecordOrigin", 0),
                "eventRecordStatus": evt.get("eventRecordStatus", 0),
                "eventType": evt.get("eventType", 0),
                "eventCode": evt.get("eventCode", 0),
                "eventTime": evt.get("eventTime", 0),
                "eventDate": evt.get("eventDate", 0),
                "trailerId": evt.get("trailerId", 0),
                "eventEndTime": evt.get("eventEndTime", 0),
                "accumulatedVehicleMiles": evt.get("accumulatedVehicleMiles", 0),
                "accumulatedEngineHours": evt.get("accumulatedEngineHours", 0),
                "eventLatitude": evt.get("eventLatitude", 0),
                "eventLongitude": evt.get("eventLongitude", 0),
                "correspondingCmvOrderNumber": evt.get("correspondingCmvOrderNumber", 0),
                "userOrderNumberForRecordOriginator": evt.get("userOrderNumberForRecordOriginator", 0),
                "address": evt.get("address", 0),
                "totalVehicleMilesDutyStatus": evt.get("totalVehicleMilesDutyStatus", 0),
                "totalEngineHoursDutyStatus": evt.get("totalEngineHoursDutyStatus", 0),
                "state": evt.get("state", 0),
                "notes": evt.get("notes", 0),
                "isApproved": evt.get("isApproved", 0),
                "shippingId": evt.get("shippingId", 0),
                "vinNumber": evt.get("vinNumber", 0),
                "vehicleId": evt.get("vehicleId", 0),
                "driver": evt.get("driver", 0),
                "vehicle": evt.get("vehicle", 0),
                "tenantId": evt.get("tenantId", 0),
                "mobileId": evt.get("mobileId", 0),
                "platform": evt.get("platform", 0),
                "expireAt": expire,
                "createdAt": createdat,
                "updatedAt": updateat,
            }

            records.append(record)

    # -------- DATAFRAME & CSV --------
    df = pd.DataFrame(records)
    df = df.fillna("")

    outname = "raw_driver_log.csv"
    df.to_csv(outname, index=False)
    print(f"Saved {len(df)} records to {outname}")
