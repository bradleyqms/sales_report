"""
Minimal test of pyodbc with Azure AD token - no SQLAlchemy
"""
import pyodbc
import struct
from azure.identity import DefaultAzureCredential

# SQL_COPT_SS_ACCESS_TOKEN constant
SQL_COPT_SS_ACCESS_TOKEN = 1256

print("Step 1: Getting Azure AD token...")
credential = DefaultAzureCredential()
token = credential.get_token("https://database.windows.net/.default")
token_bytes = token.token.encode("utf-16-le")
token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
print(f"✅ Token acquired: {len(token_struct)} bytes")

print("\nStep 2: Testing pyodbc connection...")

#Test 1: Connection string with minimal parameters
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=dnr-sql-server-qmsmedicosmetics.database.windows.net;"
    "DATABASE=dnr-mapping-db;"
    "Encrypt=yes;"
)

print(f"Connection string: {conn_str}")

try:
    print("Attempting connection with attrs_before...")
    conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    print("✅ Connection successful!")
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")
    print(f"SQL Server version: {cursor.fetchone()[0][:80]}...")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
    
    # Try alternative method - set attribute on connection object
    print("\nTrying alternative method...")
    try:
        conn = pyodbc.connect(conn_str, autocommit=True, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        print("✅ Connection successful with autocommit!")
        conn.close()
    except Exception as e2:
        print(f"❌ Also failed: {e2}")
