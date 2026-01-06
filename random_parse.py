from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import urllib.parse
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

# ================= INPUT DRIVER _ID =================
DRIVER_OBJECT_ID = ObjectId("68cf142425b5590dd4e8a44a")  # Replace with your _id

# ================= HELPER FUNCTION =================
def get_field(doc, key):
    """
    Safely extract field from document.
    - If field is missing, None, or empty string "", return None
    """
    value = doc.get(key)
    if value is None or value == "":
        return None
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
    print("Connected to MongoDB")

    # -------- QUERY DRIVER DATA BY _id --------
    driver_doc = db.drivers.find_one({"_id": DRIVER_OBJECT_ID, "isDeleted": False})

    if driver_doc:
        print("Driver Data Found")

        # Extract driver fields (excluding assigned vehicles)
        driver_info = {k: get_field(driver_doc, k) for k in driver_doc if k != "assigned Vehicles"}

        # Get assigned vehicles
        assigned_vehicles = driver_doc.get("assigned Vehicles", [])

        if not assigned_vehicles:
            # No vehicles, just save driver info as single row
            df = pd.DataFrame([driver_info])
        else:
            # Repeat driver info for each vehicle
            vehicle_rows = []
            for vehicle in assigned_vehicles:
                combined_row = driver_info.copy()
                # Flatten vehicle info, convert empty strings to None
                for v_key, v_value in vehicle.items():
                    combined_row[f"vehicle_{v_key}"] = v_value if v_value not in [None, ""] else None
                vehicle_rows.append(combined_row)

            df = pd.DataFrame(vehicle_rows)

        # Save to single CSV, nulls appear as "NULL"
        output_file = f"drivers.csv"
        df.to_csv(output_file, index=False, na_rep="NULL")
        print(f"Data exported to CSV: {output_file}")

    else:
        print(f"No driver found with _id: {DRIVER_OBJECT_ID}")
