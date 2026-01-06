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
DRIVER_ID = ObjectId("68496a5ff38dc2543d248051")
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


    # -------- ALL RECORDS FROM `recordtables` --------
    record_cursor = db.recordtables.find({"driverId": DRIVER_ID}).sort("createdAt", ASCENDING)

    records = []

    def oid_to_str(oid):
        try:
            return str(oid)
        except Exception:
            return None

    def sanitize(v):
        if isinstance(v, ObjectId):
            return str(v)
        if isinstance(v, (datetime, timedelta)):
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        if isinstance(v, dict):
            return {k: sanitize(val) for k, val in v.items()}
        if isinstance(v, list):
            return [sanitize(i) for i in v]
        return v

    for rec in record_cursor:
        s = sanitize(rec)

        tz = s.get("homeTerminalTimeZone", {})

        record = {
            "_id": oid_to_str(s.get("_id")),
            "driverId": oid_to_str(s.get("driverId")),
            "date": s.get("date", ""),
            "clock": json.dumps(s.get("clock", s.get("clockData", {})), default=str),
            "createdAt": s.get("createdAt"),
            "distance": s.get("distance", 0),
            "driverName": s.get("driverName", ""),
            "homeTerminal_id": tz.get("_id"),
            "homeTerminal_tzCode": tz.get("tzCode", ""),
            "homeTerminal_utc": tz.get("utc", ""),
            "homeTerminal_label": tz.get("label", ""),
            "homeTerminal_name": tz.get("name", ""),
            "homeTerminal_isActive": tz.get("isActive", False),
            "homeTerminal_isDeleted": tz.get("isDeleted", False),
            "homeTerminal_createdAt": tz.get("createdAt", ""),
            "homeTerminal_updatedAt": tz.get("updatedAt", ""),
            "hoursWorked": s.get("hoursWorked", 0),
            "isActive": s.get("isActive", True),
            "isDeleted": s.get("isDeleted", False),
            "isPti": s.get("isPti", ""),
            "lastKnownActivity": json.dumps(s.get("lastKnownActivity", {}), default=str),
            "shippingId": s.get("shippingId", ""),
            "signature": s.get("signature", ""),
            "status": json.dumps(s.get("status", {}), default=str),
            "tenantId": oid_to_str(s.get("tenantId")),
            "updatedAt": s.get("updatedAt"),
            "vehicleName": s.get("vehicleName", None),
            "violations": json.dumps(s.get("violations", []), default=str),
            
        }

        records.append(record)

    # -------- DATAFRAME & CSV --------
    df = pd.DataFrame(records)
    df = df.fillna("")

    outname = "record_tables.csv"
    df.to_csv(outname, index=False)
    print(f"Saved {len(df)} records to {outname}")
