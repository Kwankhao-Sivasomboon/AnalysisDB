import pandas as pd
import json
import os
import math

def process_data():
    data_dir = r"C:\Users\yourh\Desktop\PainpointToday\Yourhome_AnalysisDB\data"
    db_dir = os.path.join(data_dir, "db")

    # 1. DB Analysis
    analyze_df = pd.read_csv(os.path.join(db_dir, "analyze_data_debug.csv"))
    
    analyze_records = []
    test_users_count = 0
    valid_leads = 0

    for idx, row in analyze_df.iterrows():
        age = row['age']
        if pd.isna(age):
            age = None
        else:
            age = float(age)

        if age is None or pd.isna(age):
            age_group = "Error (No Age)"
        elif age <= 1:
            age_group = "Test User"
            test_users_count += 1
        else:
            if age < 15:
                age_group = "<15"
            elif age < 25:
                age_group = "15-24"
            elif age <= 35:
                age_group = "25-35"
            elif age <= 45:
                age_group = "36-45"
            elif age <= 55:
                age_group = "46-55"
            else:
                age_group = "55+"

        budget_max = row['budget_max'] if not pd.isna(row['budget_max']) else 0
        budget_min = row['budget_min'] if not pd.isna(row['budget_min']) else 0
        
        # Consider valid if non test and has a reasonable budget or gender
        if age_group not in ["Test User", "Error (No Age)"]:
            valid_leads += 1

        analyze_records.append({
            "id": row['id'],
            "date": row['date'],
            "age": age,
            "age_group": age_group,
            "gender": row['gender'] if not pd.isna(row['gender']) else None,
            "budget_min": budget_min,
            "budget_max": budget_max,
            "location": row['user_location'] if not pd.isna(row['user_location']) else "Unknown"
        })

    # 2. Bookings
    bookings_df = pd.read_csv(os.path.join(db_dir, "bookings_data_debug.csv"))
    valid_bookings = bookings_df.dropna(subset=['property_id'])
    
    booking_records = []
    for idx, row in valid_bookings.iterrows():
        try:
            pid = int(float(row['property_id']))
        except:
            pid = row['property_id']
        booking_records.append({
            "date": row['date'],
            "property_id": pid
        })
        
    prop_df = pd.read_csv(os.path.join(db_dir, "properties_data_debug.csv"))
    props = {row['id']: row['title'] for _, row in prop_df.iterrows()}
    def get_prop_name(pid):
        return props.get(pid, f"Property {pid}")

    booking_counts = {}
    for b in booking_records:
        pid = b['property_id']
        booking_counts[pid] = booking_counts.get(pid, 0) + 1
    
    top_bookings = [{"property_id": k, "name": get_prop_name(k), "count": v} for k, v in booking_counts.items()]
    top_bookings.sort(key=lambda x: x['count'], reverse=True)

    # 3. GA Data
    ga_pages = pd.read_csv(os.path.join(data_dir, "Pages_and_screens_Page_path_and_screen_class.csv"), skiprows=9)
    ga_events = pd.read_csv(os.path.join(data_dir, "Events_Event_name.csv"), skiprows=9)
    
    ga_stats = {
        "active_users": 0,
        "search_clicks": 0,
        "analyze_views": 0,
        "booking_clicks": 0,
        "exceptions": 0
    }
    
    # Active Users = users landing on /
    home_page = ga_pages[ga_pages["Page path and screen class"] == "/"]
    if not home_page.empty:
        ga_stats["active_users"] = int(home_page.iloc[0]["Active users"])
        
    search_page = ga_pages[ga_pages["Page path and screen class"] == "/search"]
    if not search_page.empty:
        ga_stats["search_clicks"] = int(search_page.iloc[0]["Active users"])
        
    analyze_page = ga_pages[ga_pages["Page path and screen class"] == "/result"]
    if not analyze_page.empty:
        ga_stats["analyze_views"] = int(analyze_page.iloc[0]["Active users"])

    booking_page = ga_pages[ga_pages["Page path and screen class"] == "/booking"]
    if not booking_page.empty:
        ga_stats["booking_clicks"] = int(booking_page.iloc[0]["Active users"])
        
    exceptions = ga_events[ga_events["Event name"] == "web_exception"]
    if not exceptions.empty:
        ga_stats["exceptions"] = int(exceptions.iloc[0]["Event count"])

    # GSC Data
    queries_path = os.path.join(data_dir, "yourhome.co.th-Performance-on-Search-2026-04-06", "Queries.csv")
    gsc_queries = pd.read_csv(queries_path)
    gsc_data = []
    for _, row in gsc_queries.iterrows():
        gsc_data.append({
            "query": row["Top queries"],
            "clicks": int(row["Clicks"]),
            "impressions": int(row["Impressions"]),
            "ctr": row["CTR"],
            "position": float(row["Position"])
        })
        
    # Write to final json
    output = {
        "analyze_records": analyze_records,
        "db_stats": {
            "total_runs": len(analyze_records),
            "test_users": test_users_count,
            "valid_leads": valid_leads,
            "total_bookings": len(booking_records)
        },
        "top_bookings": top_bookings[:15],
        "ga_stats": ga_stats,
        "gsc_data": gsc_data
    }

    with open(os.path.join(data_dir, "dashboard_data_new.js"), "w", encoding="utf-8") as f:
        f.write("window.dashboardDataNew = ")
        json.dump(output, f, ensure_ascii=False)
        f.write(";")
        
    print("Dashboard data successfully updated.")

if __name__ == "__main__":
    process_data()
