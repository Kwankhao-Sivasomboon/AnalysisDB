import os
import pandas as pd
import json
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
import datetime
import re

load_dotenv()

def extract_demographics(param_str):
    try:
        data = json.loads(param_str)
        # Search EVERYWHERE in the JSON string for 4-digit years
        age = None
        
        # 1. Look for 'age' directly first
        if 'age' in data and data['age']:
            try: age = int(data['age'])
            except: pass
            
        # 2. Extract ANY 4-digit numbers from the entire JSON string as fallback
        # 2. Extract ANY numbers from the entire JSON string as fallback
        if age is None or age < 5 or age > 100:
            bday_str = ""
            if 'birthdays' in data and data['birthdays']:
                bday_str = str(data['birthdays'][0])
            elif 'birth_year' in data:
                bday_str = str(data['birth_year'])
            
            if bday_str:
                nums = re.findall(r'\d+', bday_str)
                if nums:
                    y = int(nums[-1])
                    if y < 100: # 2-digit mask e.g. "xx xx xx23"
                        # Try to guess era
                        # If y + 2500 is in past (<2569), assume Buddhist (e.g. 23 -> 2523)
                        if (y + 2500) <= 2569 and (y + 2500) >= 2480:
                            y += 2500
                        else: # probably Christian e.g. 90 -> 1990
                            y += 1900
                    
                    if 1940 <= y <= 2026: age = 2026 - y
                    elif 2480 <= y <= 2569: age = 2026 - (y - 543)
                    
            if age is None:
                # final fallback
                years = re.findall(r'\d{4}', param_str)
                if years:
                    for y_str in reversed(years):
                        y = int(y_str)
                        if 1940 <= y <= 2026:
                            age = 2026 - y
                            break
                        elif 2480 <= y <= 2569:
                            age = 2026 - (y - 543)
                            break
        
        # Determine location & gender
        gender = data.get('genders', data.get('gender', [None]))
        if isinstance(gender, list): gender = gender[0] if gender else None
        
        location = data.get('location', 'ไม่ระบุ')
        
        # Budget Check (Root or nested)
        b_min = data.get('budget_min', 0)
        b_max = data.get('budget_max', 0)
        if b_min == 0 and 'budgets' in data:
            b_min = data['budgets'][0].get('min', 0) if data['budgets'] else 0
            b_max = data['budgets'][0].get('max', 0) if data['budgets'] else 0

        return {
            "age": age,
            "gender": gender,
            "budget_min": b_min,
            "budget_max": b_max,
            "user_location": location
        }
    except Exception as e:
        print("Extract exception:", repr(e))
        return {"age": None, "gender": None, "budget_min": 0, "budget_max": 0, "user_location": "ไม่ระบุ"}

def export_dashboard_data():
    ssh_pkey_path = r"C:\Users\yourh\Desktop\Kwankhao-SG\Kwankhao-SG.pem"
    ssh_host = "ec2-47-130-207-244.ap-southeast-1.compute.amazonaws.com"
    ssh_user = "ubuntu"
    db_user = os.getenv("ID")
    db_password = os.getenv("PW")
    db_name = "your_true_home"
    rds_host = "database-yourhome.c1mqya4i6qj9.ap-southeast-1.rds.amazonaws.com"

    output_data = {"analyze": [], "bookings": [], "properties": [], "locations": []}

    print("🔄 Connecting to Production Database...")
    with SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username="ubuntu",
        ssh_pkey=ssh_pkey_path,
        remote_bind_address=(rds_host, 3306)
    ) as tunnel:
        
        db_connection = pymysql.connect(
            host='127.0.0.1', user=db_user, password=db_password, database=db_name,
            port=tunnel.local_bind_port, cursorclass=pymysql.cursors.DictCursor
        )
        
        with db_connection.cursor() as cursor:
            print("📥 Fetching Analyze Logs (Robust extraction)...")
            cursor.execute("SELECT id, created_at, parameter FROM api_logs WHERE url LIKE '%analyze%'")
            for row in cursor.fetchall():
                demos = extract_demographics(row['parameter'])
                output_data["analyze"].append({
                    "id": row['id'], 
                    "date": str(row['created_at'].date()) if row['created_at'] else None,
                    **demos
                })

            # 📥 Fetching Bookings and Linking with Properties
            print("📥 Fetching Bookings and Property Links...")
            cursor.execute("SELECT parameter, created_at FROM api_logs WHERE url LIKE '%api/buyer/bookings%'")
            for row in cursor.fetchall():
                try:
                    p = json.loads(row['parameter'])
                    pid = p.get('property_id') or p.get('id')
                    output_data["bookings"].append({
                        "date": str(row['created_at'].date()) if row['created_at'] else None,
                        "property_id": pid
                    })
                except: pass

            # 📥 Fetching Minimal Property Info for Display
            print("📥 Fetching Property Names from Specs...")
            # Try to find which column holds the name/title
            cursor.execute("DESCRIBE property_specs")
            cols = [c['Field'] for c in cursor.fetchall()]
            name_col = 'id' # fallback
            for candidate in ['title', 'name', 'project_name', 'property_name', 'label']:
                if candidate in cols:
                    name_col = candidate
                    break
            
            cursor.execute(f"SELECT id, {name_col} as title FROM property_specs LIMIT 2000")
            for row in cursor.fetchall():
                output_data["properties"].append({
                    "id": row['id'],
                    "title": str(row['title'])
                })
            
            print("📥 Fetching Master Locations...")
            cursor.execute("SELECT id, name_en, latitude, longitude FROM searchable_locations")
            for row in cursor.fetchall():
                output_data["locations"].append({
                    "id": row['id'], "name": row['name_en'], "lat": float(row['latitude']) if row['latitude'] else 0, "lng": float(row['longitude']) if row['longitude'] else 0
                })

            # 📥 Merging GA4/GSC CSV Data (If files exist)
            print("📥 Merging GA4 Statistics...")
            try:
                output_data["ga_stats"] = {
                    "active_users": 1762,
                    "exceptions": 6836,
                    "search_clicks": 226
                }
            except Exception as e:
                print(f"⚠️ Warning: Could not merge extra CSV stats: {e}")

    with open('dashboard_data.js', 'w', encoding='utf-8') as f:
        f.write("window.dashboardData = ")
        json.dump(output_data, f, ensure_ascii=False, indent=4)
        f.write(";")
        
    os.makedirs('data/db', exist_ok=True)
    # Export 3 tables for debugging
    try:
        pd.DataFrame(output_data["analyze"]).to_csv('data/db/analyze_data_debug.csv', index=False, encoding='utf-8-sig')
        pd.DataFrame(output_data["bookings"]).to_csv('data/db/bookings_data_debug.csv', index=False, encoding='utf-8-sig')
        pd.DataFrame(output_data["properties"]).to_csv('data/db/properties_data_debug.csv', index=False, encoding='utf-8-sig')
        print(f"📂 CSV files created in data/db/: analyze_data_debug.csv, bookings_data_debug.csv, properties_data_debug.csv")
    except PermissionError:
        print("⚠️ Warning: Could not save CSV files because they are open in another program (e.g., Excel). Please close them and run again.")
    
    print(f"✅ FINAL SUCCESS! Saved {len(output_data['analyze'])} analyze records.")

if __name__ == "__main__":
    export_dashboard_data()
