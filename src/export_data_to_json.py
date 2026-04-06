import os
import pandas as pd
import json
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
import datetime

load_dotenv()

def extract_demographics(param_str):
    try:
        data = json.loads(param_str)
        birthdays = data.get('birthdays', [])
        genders = data.get('genders', [])
        budgets = data.get('budgets', [{}])
        budget = budgets[0] if budgets else {}
        
        age = None
        if birthdays:
            try:
                year = int(birthdays[0].split()[-1])
                age = datetime.datetime.now().year - year
            except: pass
            
        return {
            "age": age,
            "gender": genders[0] if genders else None,
            "budget_min": budget.get('min', 0),
            "budget_max": budget.get('max', 0)
        }
    except:
        return {"age": None, "gender": None, "budget_min": 0, "budget_max": 0}

def export_dashboard_data():
    ssh_pkey_path = r"C:\Users\yourh\Desktop\Kwankhao-SG\Kwankhao-SG.pem"
    ssh_host = "ec2-47-130-225-25.ap-southeast-1.compute.amazonaws.com"
    ssh_user = "ubuntu"
    db_user = os.getenv("ID")
    db_password = os.getenv("PW")
    db_name = "your_true_home"

    output_data = {"analyze": [], "bookings": [], "properties": [], "locations": []}

    print("🔄 Connecting to Database...")
    with SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_pkey=ssh_pkey_path,
        remote_bind_address=('127.0.0.1', 3306)
    ) as tunnel:
        
        db_connection = pymysql.connect(
            host='127.0.0.1', user=db_user, password=db_password, database=db_name,
            port=tunnel.local_bind_port, cursorclass=pymysql.cursors.DictCursor
        )
        
        with db_connection.cursor() as cursor:
            # 1. Analyze Logs
            print("📥 Fetching Analyze Logs...")
            cursor.execute("SELECT id, created_at, parameter, ip_address FROM api_logs WHERE url LIKE '%analyze%'")
            for row in cursor.fetchall():
                demos = extract_demographics(row['parameter'])
                output_data["analyze"].append({
                    "id": row['id'], "date": str(row['created_at'].date()) if row['created_at'] else None,
                    "ip": row['ip_address'], **demos
                })

            # 2. Bookings
            print("📥 Fetching Bookings...")
            cursor.execute("SELECT created_at, parameter FROM api_logs WHERE url LIKE '%api/buyer/bookings%'")
            for row in cursor.fetchall():
                output_data["bookings"].append({
                    "date": str(row['created_at'].date()) if row['created_at'] else None,
                    "parameter": row['parameter']
                })
            
            # 3. Property Specs (Safe Query)
            print("📥 Fetching Property Specs (Safe checking columns)...")
            cursor.execute("SELECT * FROM property_specs LIMIT 1")
            first_row = cursor.fetchone()
            if first_row:
                # พยายามหาคอลัมน์ราคาที่น่าจะใช่
                price_col = next((c for c in first_row.keys() if 'price' in c.lower()), 'price')
                bed_col = next((c for c in first_row.keys() if 'bed' in c.lower()), 'bedrooms')
                
                cursor.execute(f"SELECT property_id, {price_col} as price, {bed_col} as bedrooms FROM property_specs LIMIT 2000")
                for row in cursor.fetchall():
                    output_data["properties"].append({
                        "property_id": row['property_id'], "price": float(row['price']) if row['price'] else 0,
                        "bedrooms": row['bedrooms']
                    })

            # 4. Locations
            print("📥 Fetching Searchable Locations...")
            try:
                cursor.execute("SELECT id, name_en, latitude, longitude FROM searchable_locations WHERE latitude IS NOT NULL LIMIT 500")
                for row in cursor.fetchall():
                    output_data["locations"].append({
                        "id": row['id'], "name": row['name_en'], "lat": float(row['latitude']), "lng": float(row['longitude'])
                    })
            except: print("⚠️ Could not fetch locations table.")

    with open('dashboard_data.js', 'w', encoding='utf-8') as f:
        f.write("window.dashboardData = ")
        json.dump(output_data, f, ensure_ascii=False, indent=4)
        f.write(";")
        
    print("✅ Success! Data saved to dashboard_data.js")

if __name__ == "__main__":
    export_dashboard_data()
