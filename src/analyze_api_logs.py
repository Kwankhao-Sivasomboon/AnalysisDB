import os
import pandas as pd
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
import getpass

# 1. โหลดค่าคอนฟิกจาก .env (ID, PW ของ MySQL)
load_dotenv()

def get_data_from_mysql():
    # --- ส่วนที่ออกแบบให้ AI ไม่เห็น (Input ตอนรัน) ---
    print("🔒 โปรดระบุข้อมูลเพื่อความปลอดภัย (ข้อมูลนี้จะไม่ถูกบันทึกใน Code)")
    ssh_pkey_path = input("ระบุ Full Path ของไฟล์ .pem (เช่น C:/Keys/key.pem): ").strip().replace('"', '')
    
    # ข้อมูล SSH (จากคำสั่งที่คุณให้มา)
    ssh_host = "ec2-47-130-225-25.ap-southeast-1.compute.amazonaws.com"
    ssh_user = "ubuntu"
    
    # ข้อมูล MySQL (จาก .env)
    db_user = os.getenv("ID")
    db_password = os.getenv("PW")
    db_name = "your_true_home"

    # 2. สร้าง SSH Tunnel
    with SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_pkey=ssh_pkey_path,
        remote_bind_address=('127.0.0.1', 3306)
    ) as tunnel:
        
        print(f"✅ SSH Tunnel เชื่อมต่อสำเร็จที่ Port: {tunnel.local_bind_port}")

        # 3. เชื่อมต่อ MySQL ผ่าน Tunnel
        db_connection = pymysql.connect(
            host='127.0.0.1',
            user=db_user,
            password=db_password,
            database=db_name,
            port=tunnel.local_bind_port
        )

        query = """
        SELECT * FROM api_logs 
        WHERE url = 'https://dev.yourhome.co.th/api/public/comprehensive-compatibility/analyze';
        """

        # 4. ดึงข้อมูลเข้า Pandas DataFrame
        print("📊 กำลังดึงข้อมูลและวิเคราะห์...")
        df = pd.read_sql(query, db_connection)
        
        db_connection.close()
        return df

if __name__ == "__main__":
    try:
        df_logs = get_data_from_mysql()
        
        # แสดงผลการวิเคราะห์เบื้องต้น
        print("\n--- ผลการดึงข้อมูล ---")
        print(df_logs.head())
        print(f"\nจำนวนลอคทั้งหมด: {len(df_logs)} แถว")
        
        # คุณสามารถเริ่มวิเคราะห์ต่อได้ที่นี่ เช่น
        # df_logs.describe()
        # df_logs.to_csv("analyzed_result.csv", index=False)
        
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")
