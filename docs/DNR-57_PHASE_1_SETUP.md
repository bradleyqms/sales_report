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

### **Step 1: Update Your .env File**

Copy `.env.example` to `.env` (if not already done) and add your SQL admin password:

```bash
DATABASE_PASSWORD=your_actual_sql_password_here
```

**Important:** The password you set when creating the SQL Server!

---

### **Step 2: Verify Python Dependencies Installed**

Confirm packages are installed globally (or create venv if needed):

```powershell
pip install -r requirements.txt
```

---

### **Step 3: Test Database Connection**

```powershell
python -c "from src.database import test_connection; test_connection()"
```

Expected output:
```
[Database] Using SQL authentication (local development)
[Database] Connection successful!
[Database] SQL Server version: Microsoft SQL Server 2022...
```

❌ If connection fails:
- Verify DATABASE_PASSWORD in `.env`
- Check your IP is whitelisted in Azure SQL firewall rules
- Verify ODBC Driver 18 is installed: `odbcinst -q -d` (should list "ODBC Driver 18 for SQL Server")

---

### **Step 4: Create Database Schema**

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

### **Step 5: Seed Initial Data**

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

## 📝 Connection Strings Reference

### **Production** (Azure App Service with Managed Identity)
```
mssql+pyodbc://@dnr-sql-server-qmsmedicosmetics.database.windows.net/dnr-mapping-db?driver=ODBC+Driver+18+for+SQL+Server&Authentication=ActiveDirectoryMsi&Encrypt=yes
```
- No password needed
- Automatically used when `AZURE_CLIENT_ID` env var present

### **Local Development** (SQL Authentication)
```
mssql+pyodbc://sqladmin:YOUR_PASSWORD@dnr-sql-server-qmsmedicosmetics.database.windows.net/dnr-mapping-db?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes
```
- Uses DATABASE_PASSWORD from .env
- Used when `AZURE_CLIENT_ID` is not present

---

## 🚨 Troubleshooting

### "Login failed for user 'qms-sales-report'" (Production)
- Verify Managed Identity is enabled
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
