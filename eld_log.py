from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING
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

# -------- INPUT DRIVER ID (STRING) --------
DRIVER_ID = "lovepreet08601"

# -------- HELPER FUNCTION --------
def sanitize(v):
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

    # -------- FETCH ALL ELD LOGS FOR DRIVER --------
    cursor = db["eld-logs"].find({"driverId": DRIVER_ID}).sort("createdAt", ASCENDING)

    records = []
    for rec in cursor:
        s = sanitize(rec)
        record = {
            "_id": str(s.get("_id")),
            "driverId": s.get("driverId"),
            "vehicleId": s.get("vehicleId"),
            "tenantId": str(s.get("tenantId")),
            "date": s.get("date"),
            "unixTime": s.get("unixTime"),
            "eventType": s.get("eventType"),
            "dop": s.get("dop"),
            "odometer": s.get("odometer"),
            "lat": s.get("lat"),
            "lng": s.get("lng"),
            "satellites": s.get("satellites"),
            "rpm": s.get("rpm"),
            "gpsDateTime": s.get("gpsDateTime"),
            "sequence": s.get("sequence"),
            "vin": s.get("vin"),
            "course": s.get("course"),
            "tripDistance": s.get("tripDistance"),
            "mslAlt": s.get("mslAlt"),
            "engineHours": s.get("engineHours"),
            "cachedRecType": s.get("cachedRecType"),
            "gpsSpeed": s.get("gpsSpeed"),
            "firmware": s.get("firmware"),
            "motionStartRecord": s.get("motionStartRecord"),
            "tripHours": s.get("tripHours"),
            "broadcastString": s.get("broadcastString"),
            "voltage": s.get("voltage"),
            "engineState": s.get("engineState"),
            "motionStopRecord": s.get("motionStopRecord"),
            "engineOnRecord": s.get("engineOnRecord"),
            "engineOffRecord": s.get("engineOffRecord"),
            "cmvVinNo": s.get("cmvVinNo"),
            "eldNumber": s.get("eldNumber"),
            "speed": s.get("speed"),
            "createdAt": s.get("createdAt"),
            "updatedAt": s.get("updatedAt")
            
        }
        records.append(record)

    # -------- DATAFRAME & CSV --------
    df = pd.DataFrame(records)
    df = df.fillna("")
    outname = f"eld_logs.csv"
    df.to_csv(outname, index=False)
    print(f"Saved {len(df)} records to {outname}")
