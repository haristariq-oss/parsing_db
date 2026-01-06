from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
from datetime import datetime
import os
from dotenv import load_dotenv

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

# -------- INPUT DRIVER ID (ObjectId) --------
DRIVER_ID = ObjectId("6900f6a1a1d53ab328b90be8")  # replace with your driverId

# -------- HELPER FUNCTION --------
def sanitize(v):
    """Convert ObjectId and datetime to strings for CSV export"""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
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

    # -------- FETCH ALL COMPLIANCE SCORE HISTORIES --------
    cursor = db["compliancescorehistories"].find({"driverId": DRIVER_ID}).sort("scoreDate", ASCENDING)

    records = []
    for rec in cursor:
        s = sanitize(rec)
        record = {
            "_id": s.get("_id"),
            "driverId": s.get("driverId"),
            "tenantId": s.get("tenantId"),
            "scoreDate": s.get("scoreDate"),
            "endOfDayScore": s.get("endOfDayScore"),
            "netChange": s.get("netChange"),
            "band": s.get("band"),
            "violationCount": s.get("violationCount"),
            "trend": s.get("trend"),
            "createdAt": s.get("createdAt"),
            "updatedAt": s.get("updatedAt")
        }
        records.append(record)

    # -------- DATAFRAME & CSV --------
    df = pd.DataFrame(records)
    df = df.fillna("")
    outname = f"compliance_score_histories.csv"
    df.to_csv(outname, index=False)
    print(f"Saved {len(df)} records to {outname}")
