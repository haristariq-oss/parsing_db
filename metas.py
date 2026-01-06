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
DRIVER_ID = ObjectId("68cf142425b5590dd4e8a44a")
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


    # -------- ALL METAS --------
    metas_cursor = db.metas.find(
        {"driver": DRIVER_ID},
        projection={
            "_id": 1,
            "driver": 1,
            "vehicle": 1,
            "unit": 1,
            "dateTime": 1,
            "date": 1,
            "clockData": 1,
            "voilations": 1,
            "ptiViolation": 1,
            "deviceCalculations": 1,
            "deviceModel": 1,
            "deviceVersion": 1,
            "editRequest": 1,
            "editRequests": 1,
            "eldType": 1,
            "engineStart": 1,
            "eventSequenceIdNumber": 1,
            "isRecap": 1,
            "pti": 1,
            "totalEngineHours": 1,
            "totalVehicleMiles": 1,
            "powerUp": 1,
            "powerUps": 1,
            "createdAt": 1,
            "updatedAt": 1,
            "lastActivity": 1,
            "currentTime": 1,
            "currentDate": 1,
            "latitude": 1,
            "longitude": 1,
            "address": 1,
            "speed": 1,
            "currentEventCode": 1,
            "currentEventType": 1,
            "odoMeterMillage": 1,
            "engineHours": 1
        }
    ).sort("createdAt", ASCENDING)

    records = []

    def oid_to_str(oid):
        try:
            return str(oid)
        except Exception:
            return None

    for meta in metas_cursor:
        # top-level
        _id = oid_to_str(meta.get("_id"))
        driver = oid_to_str(meta.get("driver"))
        vehicle = oid_to_str(meta.get("vehicle"))
        unit = oid_to_str(meta.get("unit"))

        # simple fields
        dateTime = meta.get("dateTime")
        date_str = meta.get("date")
        date_parsed = parse_event_date(date_str) if date_str else None

        # nested structures
        clock = meta.get("clockData") or {}
        device_calc = meta.get("deviceCalculations") or {}
        violations = meta.get("voilations") or []
        pti_violations = meta.get("ptiViolation") or []
        last_act = meta.get("lastActivity") or {}

        # Build flattened record following provided structure while keeping raw JSON for nested
        record = {
            "_id": _id,
            "driver": driver,
            "vehicle": vehicle,
            "unit": unit,
            "dateTime": dateTime,
            "date": date_str,
            "date_parsed": date_parsed,

            # clockData fields
            "clock_breakSeconds": clock.get("breakSeconds", 0),
            "clock_cycleSeconds": clock.get("cycleSeconds", 0),
            "clock_driveSeconds": clock.get("driveSeconds", 0),
            "clock_driveSecondsSplit": json.dumps(clock.get("driveSecondsSplit", [])),
            "clock_shiftDutySecond": clock.get("shiftDutySecond", 0),
            "clock_shiftDutySecondsSplit": json.dumps(clock.get("shiftDutySecondsSplit", [])),
            "clock_isSplitActive": clock.get("isSplitActive", False),
            "clock_recapeClock": clock.get("recapeClock", 0),
            "clock_recap_clock_raw": json.dumps(clock),

            # deviceCalculations fields (common ones)
            "dc_BREAK_30_MIN": device_calc.get("BREAK_30_MIN", 0),
            "dc_BREAK_ENABLE": device_calc.get("BREAK_ENABLE", False),
            "dc_BREAK_VERIFIED": device_calc.get("BREAK_VERIFIED", False),
            "dc_BREAK_VIOLATION": device_calc.get("BREAK_VIOLATION", False),
            "dc_CONSECUTIVE_DRIVING": device_calc.get("CONSECUTIVE_DRIVING", 0),
            "dc_CURRENT_STATUS": device_calc.get("CURRENT_STATUS", None),
            "dc_CYCLE_DATA": json.dumps(device_calc.get("CYCLE_DATA", [])),
            "dc_CYCLE_START": json.dumps(device_calc.get("CYCLE_START", {})),
            "dc_CYCLE_START_DATE": json.dumps(device_calc.get("CYCLE_START_DATE", {})),
            "dc_CYCLE_TIME_DUPLICATE": device_calc.get("CYCLE_TIME_DUPLICATE", 0),
            "dc_DRIVING": device_calc.get("DRIVING", 0),
            "dc_DRIVING_ADDED": device_calc.get("DRIVING_ADDED", 0),
            "dc_HOURS_WORKED": device_calc.get("HOURS_WORKED", 0),
            "dc_DRIVING_COUNTER": device_calc.get("DRIVING_COUNTER", 0),
            "dc_DRIVING_CYCLE": json.dumps(device_calc.get("DRIVING_CYCLE", [])) if isinstance(device_calc.get("DRIVING_CYCLE"), list) else device_calc.get("DRIVING_CYCLE", 0),
            "dc_powerUp": device_calc.get("powerUp", False),
            "dc_DRIVING_VIOLATION": device_calc.get("DRIVING_VIOLATION", False),
            "dc_LAST_SYNC_TIME": device_calc.get("LAST_SYNC_TIME", None),
            "dc_NUMBER_SHIFTS": device_calc.get("NUMBER_SHIFTS", 0),
            "dc_OFF_DUTY": device_calc.get("OFF_DUTY", 0),
            "dc_OFF_DUTY_CONSECUTIVE": device_calc.get("OFF_DUTY_CONSECUTIVE", 0),
            "dc_OFF_DUTY_COUNTER": device_calc.get("OFF_DUTY_COUNTER", 0),
            "dc_ON_DRIVING_CHUNK": device_calc.get("ON_DRIVING_CHUNK", 0),
            "dc_ON_DUTY_ADDED": device_calc.get("ON_DUTY_ADDED", 0),
            "dc_ON_DUTY_CURRENT_TIME": device_calc.get("ON_DUTY_CURRENT_TIME", 0),
            "dc_ON_DUTY_MAX_HOURS": device_calc.get("ON_DUTY_MAX_HOURS", False),
            "dc_SLEEPER_BERTH_34_HOURS": device_calc.get("SLEEPER_BERTH_34_HOURS", 0),
            "dc_SPLIT_ACTIVE": device_calc.get("SPLIT_ACTIVE", False),
            "dc_TOTAL_SHIFT_COUNTER": device_calc.get("TOTAL_SHIFT_COUNTER", 0),
            "dc_currentDateTime": device_calc.get("currentDateTime", ""),
            "dc_engineStart": device_calc.get("engineStart", False),
            "dc_lastLogTime": json.dumps(device_calc.get("lastLogTime", {})),
            "dc_violation": json.dumps(device_calc.get("violation", [])),
            "dc_timeDifferenceInSeconds": device_calc.get("timeDifferenceInSeconds", 0),
            "dc_isRecap": device_calc.get("isRecap", False),
            "device_calculations_raw": json.dumps(device_calc),

            # other top-level fields
            "deviceModel": meta.get("deviceModel", ""),
            "deviceVersion": meta.get("deviceVersion", ""),
            "editRequest": meta.get("editRequest", meta.get("editRequests", False)),
            "eldType": meta.get("eldType", ""),
            "engineStart": meta.get("engineStart", False),
            "eventSequenceIdNumber": meta.get("eventSequenceIdNumber", 0),
            "isRecap": meta.get("isRecap", False),
            "pti": meta.get("pti", ""),
            "totalEngineHours": meta.get("totalEngineHours", 0),
            "totalVehicleMiles": meta.get("totalVehicleMiles", ""),
            "powerUp": meta.get("powerUp", False),

            # location / status
            "lastActivity": json.dumps(last_act),
            "odoMeterMillage": meta.get("odoMeterMillage", ""),
            "engineHours": meta.get("engineHours", 0),
            "currentTime": meta.get("currentTime", ""),
            "currentDate": meta.get("currentDate", ""),
            "latitude": meta.get("latitude", 0),
            "longitude": meta.get("longitude", 0),
            "address": meta.get("address", ""),
            "speed": meta.get("speed", 0),
            "currentEventCode": meta.get("currentEventCode", ""),
            "currentEventType": meta.get("currentEventType", ""),

            # arrays
            "voilations": json.dumps(violations),
            "ptiViolation": json.dumps(pti_violations),

            # timestamps
            "createdAt": meta.get("createdAt"),
            "updatedAt": meta.get("updatedAt")
        }

        records.append(record)

    # -------- DATAFRAME & CSV --------
    df = pd.DataFrame(records)
    df = df.fillna("")

    outname = "metas.csv"
    df.to_csv(outname, index=False)
    print(f"Saved {len(df)} records to {outname}")
