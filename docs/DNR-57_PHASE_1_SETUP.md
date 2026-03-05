# DNR-57 Phase 1 Setup Complete ✅

## What Was Created

### ✅ Azure Infrastructure  
- **SQL Database**: `dnr-mapping-db` (Standard S0, Germany West Central)
- **SQL Server**: `dnr-sql-server-qmsmedicosmetics.database.windows.net`
- **Managed Identity**: Enabled on App Service `qms-sales-report`  
  - Object ID: `6eb53519-89fc-4229-834d-52516e10e6e1`
- **Database Permissions**: Granted `db_datareader`, `db_datawriter`, `db_ddladmin` to MI

### ✅ Code Infrastructure Created
1. **src/database.py** - Database connection with Managed Identity support
2. **src/models.py** - 4 SQLAlchemy ORM models:
   - `EntityMapping` - Customer/employee mappings (replaces CSV)
   - `UnmappedLog` - Track unmapped entities from SAP extracts
   - `ReportRun` - Report execution history  
   - `AuditLog` - User actions audit trail
3. **src/seed_mappings.py** - Script to import existing CSV data
4. **alembic/** - Database migration framework configured
5. **.env.example** - Environment variable template
6. **requirements.txt** - Added SQLAlchemy, pyodbc, alembic

---

## 🔧 Next Steps (You Need to Do)

### **Step 1: Grant Your Azure AD Account Database Access**

Connect to your SQL Database using Azure Portal Query Editor:

1. **Open Query Editor:**
   - Go to Azure Portal → SQL Database `dnr-mapping-db`
   - Left menu → **Query editor (preview)**
   - Login with **Azure AD authentication** (bradley@qmsmedicosmetics.com)

2. **Run these SQL commands:**

```sql
-- Create user for your Azure AD account
CREATE USER [bradley@qmsmedicosmetics.com] FROM EXTERNAL PROVIDER;

-- Grant necessary permissions
ALTER ROLE db_datareader ADD MEMBER [bradley@qmsmedicosmetics.com];
ALTER ROLE db_datawriter ADD MEMBER [bradley@qmsmedicosmetics.com];
ALTER ROLE db_ddladmin ADD MEMBER [bradley@qmsmedicosmetics.com];

-- Verify it worked
SELECT name, type_desc FROM sys.database_principals 
WHERE name = 'bradley@qmsmedicosmetics.com';
```

Expected output: `bradley@qmsmedicosmetics.com | EXTERNAL_USER`

---

### **Step 2: Login to Azure CLI**

For local development, you need to authenticate with Azure CLI:

```powershell
# Login with your Azure account
az login
```

A browser window will open for authentication. After logging in, you should see your subscription listed.

---

### **Step 3: Install Python Dependencies**

```powershell
pip install -r requirements.txt
```

This installs: `SQLAlchemy`, `pyodbc`, `alembic`, `azure-identity`

---

### **Step 4: Test Database Connection**

```powershell
python -c "from src.database import test_connection; test_connection()"
```

Expected output:
```
[Database] Using DefaultAzureCredential (Azure CLI/VS Code)
[Database] Azure AD token acquired successfully
[Database] Connecting to dnr-sql-server-qmsmedicosmetics.database.windows.net/dnr-mapping-db with Azure AD authentication
[Database] Connection successful!
[Database] SQL Server version: Microsoft SQL Server 2022...
```

❌ If connection fails:
- **"Failed to get Azure AD token"**: Run `az login` to authenticate
- **"Login failed"**: Grant database permissions (Step 1 above)
- **"Cannot open database"**: Check DATABASE_NAME in .env
- **"TCP Provider: No connection"**: Your IP needs to be whitelisted in Azure SQL firewall
- **"Data source name not found"**: Install [ODBC Driver 18](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

### **Step 5: Create Database Schema**

Generate initial Alembic migration from models:

```powershell
python -m alembic revision --autogenerate -m "initial_schema"
```

Apply migration to create tables:

```powershell
python -m alembic upgrade head
```

Expected output:
```
INFO  [alembic.runtime.migration] Running upgrade  -> abc123, initial_schema
```

---

### **Step 6: Seed Initial Data**

Import existing entity_mappings.csv into database:

```powershell
python src/seed_mappings.py
```

Expected output:
```
✅ Loaded 245 rows from CSV
💾 Inserting 245 mappings into database...
✅ Successfully seeded 245 entity mappings!
```

---

## ✅ Verification Checklist

After completing the steps above, verify:

```powershell
# Check tables were created
python -c "from src.database import engine; from sqlalchemy import inspect; print(inspect(engine).get_table_names())"
```

Expected output: `['entity_mappings', 'unmapped_logs', 'report_runs', 'audit_log', 'alembic_version']`

```powershell
# Check data was seeded
python -c "from src.database import SessionLocal; from src.models import EntityMapping; db = SessionLocal(); print(f'Total mappings: {db.query(EntityMapping).count()}'); db.close()"
```

Expected output: `Total mappings: 245` (or your actual count)

---

## 📝 Authentication Details

### **🔐 No Passwords Required!**

This setup uses **Azure AD authentication everywhere**:

### **Production** (Azure App Service)
- Uses **Managed Identity** (automatic)
- Token automatically refreshed every hour
- No credentials in environment variables

### **Local Development**
- Uses **DefaultAzureCredential** which tries in order:
  1. Environment variables (if set)
  2. Managed Identity (if running on Azure)
  3. **Azure CLI** (`az login`) ← Most common for local dev
  4. Visual Studio Code
  5. Azure PowerShell

### **Connection String** (Same for both environments)
```**"Failed to get Azure AD token"**
- **Local**: Run `az login` to authenticate with Azure
- **Production**: Verify Managed Identity is enabled on App Service

### **"Login failed for user 'bradley@qmsmedicosmetics.com'"** (Local)
- You haven't granted your user database permissions
- Run the SQL commands from Step 1 in Azure Portal Query Editor

### **"Login failed for user 'qms-sales-report'"** (Production)
- Managed Identity doesn't have database permissions
- Run: `CREATE USER [qms-sales-report] FROM EXTERNAL PROVIDER;`
- Grant roles: `db_datareader`, `db_datawriter`, `db_ddladmin`

### **"Cannot open database requested by the login"**
- Check DATABASE_NAME matches in .env

### **"TCP Provider: No connection could be made"**
- Add your IP to SQL Server firewall rules (Azure Portal → SQL Server → Networking)

### **"Data source name not found"** (ODBC error)
- Install ODBC Driver 18: Download from [Microsoft](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

### **"Azure CLI not installed"**
- Install: `winget install Microsoft.AzureCLI` (Windows)
- Or download from: https://aka.ms/installazurecliwindows
- Re-run SQL grant commands in Query Editor

### "Cannot open database requested by the login"
- Check DATABASE_NAME matches in .env

### "TCP Provider: No connection could be made"
- Add your IP to SQL Server firewall rules (Azure Portal → SQL Server → Networking)

### "Data source name not found" (ODBC error)
- Install ODBC Driver 18: Download from [Microsoft](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

## 🎯 What's Next (Phase 2)

Once Phase 1 is verified working:
- **Phase 2**: Update `qry_data_mapping.py` to use database instead of CSV
- **Phase 3**: Persist report runs to `ReportRuns` table
- **Phase 4**: Build Admin UI for mapping management
- **Phase 5**: Add Entra ID authentication

---

## 📚 Files Modified/Created

```
sales_report_v2_independent/
├── .env.example                    # NEW - Environment template
├── alembic.ini                     # NEW - Alembic config
├── requirements.txt                # UPDATED - Added SQL dependencies
├── alembic/
│   ├── env.py                      # UPDATED - Import models
│   ├── versions/                   # Will contain migrations
│   └── ...
└── src/
    ├── database.py                 # NEW - DB connection
    ├── models.py                   # NEW - ORM models  
    └── seed_mappings.py            # NEW - CSV import script
```

---

**Status**: ✅ Phase 1 infrastructure complete. Ready for local testing!
