from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
from datetime import datetime
import os
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

# ================= SSH CONFIG =================
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")

# ================= MONGO CONFIG =================
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = urllib.parse.quote_plus(os.getenv("MONGO_PASSWORD"))
MONGO_DB = os.getenv("MONGO_DB")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT"))

# ================= INPUT VEHICLE ID =================
VEHICLE_ID = ObjectId("68cf155c25b5590dd4e8a479")

# ================= HELPER FUNCTION =================
def get_field(doc, key):
    """
    Return field value based on:
    - If field missing → 'not_avail'
    - If field exists but empty string → 0
    - If numeric string → convert to float
    - Otherwise → return value as is
    """
    if key not in doc:
        return "not_avail"
    
    value = doc[key]
    
    if value == "":
        return 0
    
    # Try to convert numeric strings to float
    try:
        return float(value)
    except (ValueError, TypeError):
        return value

# ================= SSH TUNNEL =================
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

    print("✅ Connected to MongoDB")

    # ================= FETCH VEHICLE TRACKING DATA =================
    cursor = db.driverlocations.find(
        {
            "vehicleId": VEHICLE_ID,
            "isDeleted": False
        }
    ).sort("createdAt", ASCENDING)

    records = []

    for doc in cursor:
        record = {
            "driverId": str(get_field(doc, "driverId")),
            "vehicleId": str(get_field(doc, "vehicleId")),
            "tenantId": str(get_field(doc, "tenantId")),

            "date": get_field(doc, "date"),
            "time": get_field(doc, "time"),
            "type": get_field(doc, "type"),
            "address": get_field(doc, "address"),
            "dateExtra": get_field(doc, "dateExtra"),

            "latitude": get_field(doc, "latitude"),
            "longitude": get_field(doc, "longitude"),
            "speed": get_field(doc, "speed"),
            "direction": get_field(doc, "direction"),

            "odometer": get_field(doc, "odometer"),
            "engineHours": get_field(doc, "engineHours"),
            "tripDistance": get_field(doc, "tripDistance"),
            "tripHours": get_field(doc, "tripHours"),

            "engineState": get_field(doc, "engineState"),
            "eventCode": get_field(doc, "eventCode"),
            "eventType": get_field(doc, "eventType"),
            "origin": get_field(doc, "origin"),
            "moving": get_field(doc, "moving"),

            "vin": get_field(doc, "vin"),
            "firmwareVersion": get_field(doc, "firmwareVersion"),
            "voltage": get_field(doc, "voltage"),
            "sequence": get_field(doc, "sequence"),

            "timeStamp": get_field(doc, "timeStamp"),
            "engineParamsTimestamp": get_field(doc, "engineParamsTimestamp"),

            "coolantLevel": get_field(doc, "coolantLevel"),
            "oilLevel": get_field(doc, "oilLevel"),
            "oilTemperature": get_field(doc, "oilTemprature"),
            "oilPressure": get_field(doc, "oilPressure"),

            "fuelTemp": get_field(doc, "fuelTemp"),
            "fuelPressure": get_field(doc, "fuelPressure"),
            "turboBoost": get_field(doc, "turboBoost"),
            "turboRpm": get_field(doc, "turboRpm"),

            "engineCoolantTemp": get_field(doc, "engineCoolantTemp"),
            "engineOilTemp": get_field(doc, "engineOilTemp"),
            "engineOilLevel": get_field(doc, "engineOilLevel"),
            "engineCoolantLevel": get_field(doc, "engineCoolantLevel"),

            "load_pct": get_field(doc, "load_pct"),

            "stateAccel": get_field(doc, "stateAccel"),
            "stateAnticipate": get_field(doc, "stateAnticipate"),
            "stateEco": get_field(doc, "stateEco"),
            "stateEnginePower": get_field(doc, "stateEnginePower"),
            "stateHighRPM": get_field(doc, "stateHighRPM"),
            "stateUnsteady": get_field(doc, "stateUnsteady"),
            "isActive": get_field(doc, "isActive"),
            "createdAt": get_field(doc, "createdAt"),
            "updatedAt": get_field(doc, "updatedAt"),
        }

        records.append(record)

    # ================= SAVE CSV =================
    df = pd.DataFrame(records)

    vehicle_id_str = str(VEHICLE_ID)
    output_file = f"vehicle_tracking_{vehicle_id_str}.csv"
    df.to_csv(output_file, index=False)

    print(f"✅ Saved {len(df)} records to {output_file}")
