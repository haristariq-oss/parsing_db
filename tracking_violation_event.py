from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
from datetime import datetime
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
DRIVER_ID = ObjectId("690c1051eb5ee1394d947f58")

# -------- HELPER FUNCTIONS --------
def sanitize(v):
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, (datetime)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: sanitize(val) for k, val in v.items()}
    if isinstance(v, list):
        return [sanitize(i) for i in v]
    return v

# -------- CONNECT VIA SSH TUNNEL --------
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

    # -------- FETCH ALL TRACKING VIOLATION EVENTS --------
    cursor = db.trackingviolationevents.find({"driverId": DRIVER_ID}).sort("createdAt", ASCENDING)

    records = []
    for rec in cursor:
        s = sanitize(rec)
        record = {
            "_id": s.get("_id"),
            "driverId": s.get("driverId"),
            "vehicleId": s.get("vehicleId"),
            "tenantId": s.get("tenantId"),
            "date": s.get("date"),
            "timeStamp": s.get("timeStamp"),
            "currentEventType": s.get("currentEventType"),
            "currentEventCode": s.get("current EventCode"),
            "wasMoving": s.get("wasMoving"),
            "violationType": s.get("violationType"),
            "latitude": s.get("latitude"),
            "longitude": s.get("longitude"),
            "address": s.get("address"),
            "speed": s.get("speed"),
            "isAlerted": s.get("isAlerted"),
            "isActive": s.get("isActive"),
            "isDeleted": s.get("isDeleted"),
            "createdAt": s.get("createdAt"),
            "updatedAt": s.get("updatedAt"),
        }
        records.append(record)

    # -------- DATAFRAME & CSV --------
    df = pd.DataFrame(records)
    df = df.fillna("")
    outname = "tracking_violation_events.csv"
    df.to_csv(outname, index=False)
    print(f"Saved {len(df)} records to {outname}")
