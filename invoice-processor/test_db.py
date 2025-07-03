import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

pg_config = {
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT', '5432'),
    'database': os.getenv('PG_DATABASE'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD')
}

try:
    with psycopg2.connect(**pg_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM capital_project_invoices_emailed")
            count = cursor.fetchone()[0]
            print(f"✅ Database connection successful! Current records: {count}")
except Exception as e:
    print(f"❌ Database connection failed: {e}")