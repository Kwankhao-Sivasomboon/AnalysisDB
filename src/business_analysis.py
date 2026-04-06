import os
import pandas as pd
import json
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
from collections import Counter
import ast
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

load_dotenv()

def extract_demographics(param_str):
    try:
        # Some params might be strings of JSON or dicts
        data = json.loads(param_str)
        birthdays = data.get('birthdays', [])
        genders = data.get('genders', [])
        wealth = data.get('wealth_status', [])
        return birthdays[0] if birthdays else None, genders[0] if genders else None, wealth[0] if wealth else None
    except:
        return None, None, None

def analyze_db():
    print("Connecting to DB...")
    ssh_host = "ec2-47-130-225-25.ap-southeast-1.compute.amazonaws.com"
    ssh_user = "ubuntu"
    ssh_pkey_path = r"C:\Users\yourh\Desktop\Kwankhao-SG\Kwankhao-SG.pem" 
    
    db_user = os.getenv("ID")
    db_password = os.getenv("PW")
    db_name = "your_true_home"

    with SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_pkey=ssh_pkey_path,
        remote_bind_address=('127.0.0.1', 3306)
    ) as tunnel:
        
        db_connection = pymysql.connect(
            host='127.0.0.1',
            user=db_user,
            password=db_password,
            database=db_name,
            port=tunnel.local_bind_port
        )

        # 1. Analyze Demographics
        query_analyze = """
        SELECT parameter FROM api_logs 
        WHERE url = 'https://dev.yourhome.co.th/api/public/comprehensive-compatibility/analyze';
        """
        df_analyze = pd.read_sql(query_analyze, db_connection)
        
        ages = []
        genders = []
        wealths = []
        for p in df_analyze['parameter'].dropna():
            b, g, w = extract_demographics(p)
            if b: ages.append(b[-4:]) # extract year
            if g: genders.append(g)
            if w: wealths.append(w)
            
        print("Demographics from 'analyze' endpoint:")
        print("Total Analyzes:", len(df_analyze))
        print("Birth Years:", Counter(ages).most_common(5))
        print("Genders:", Counter(genders).most_common(5))
        print("Wealth Status:", Counter(wealths).most_common(5))
        
        # 2. Bookings Analysis
        query_bookings = """
        SELECT parameter FROM api_logs 
        WHERE url LIKE '%api/buyer/bookings%';
        """
        df_book = pd.read_sql(query_bookings, db_connection)
        print("\nTotal Booking API Calls:", len(df_book))
        
        db_connection.close()

if __name__ == "__main__":
    analyze_db()
