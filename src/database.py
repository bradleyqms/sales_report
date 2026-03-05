"""
Database connection and session management for Sales Report application.
Uses Azure AD authentication everywhere - no passwords needed!

Production: Managed Identity
Local Development: DefaultAzureCredential (uses Azure CLI login)
"""

import os
import struct
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import Generator
import pyodbc

try:
    from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
except ImportError:
    raise ImportError(
        "azure-identity package is required. Install with: pip install azure-identity"
    )

# SQLAlchemy 2.0 declarative base
Base = declarative_base()


def get_azure_sql_token() -> bytes:
    """
    Get Azure AD access token for SQL Database authentication.
    
    Production (Azure App Service):
        Uses Managed Identity to get token
        
    Local Development:
        Uses DefaultAzureCredential which tries (in order):
        1. Environment variables
        2. Managed Identity (if available)
        3. Azure CLI (az login)
        4. Visual Studio Code
        5. Azure PowerShell
    
    Returns:
        bytes: Access token in the format required by pyodbc SQL_COPT_SS_ACCESS_TOKEN
    """
    try:
        # In Azure App Service: Use Managed Identity
        if os.getenv("AZURE_CLIENT_ID"):
            print("[Database] Using Managed Identity for authentication")
            credential = ManagedIdentityCredential()
        else:
            # Local development: Use DefaultAzureCredential (Azure CLI, VS Code, etc.)
            print("[Database] Using DefaultAzureCredential (Azure CLI/VS Code)")
            credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        
        # Get token for Azure SQL Database
        token = credential.get_token("https://database.windows.net/.default")
        
        # Convert token to bytes in the format pyodbc expects
        token_bytes = token.token.encode("utf-16-le")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        
        print("[Database] Azure AD token acquired successfully")
        return token_struct
        
    except Exception as e:
        print(f"[Database] ERROR: Failed to get Azure AD token: {e}")
        print("[Database] Local development: Ensure you're logged in with 'az login'")
        print("[Database] Production: Verify Managed Identity is enabled and has database permissions")
        raise


def get_connection_string() -> str:
    """
    Get database connection string for Azure AD authentication.
    No password needed - authentication is handled by Azure AD token.
    
    Returns:
        str: SQLAlchemy connection string for pyodbc (without credentials)
    """
    server = os.getenv("DATABASE_SERVER", "dnr-sql-server-qmsmedicosmetics.database.windows.net")
    database = os.getenv("DATABASE_NAME", "dnr-mapping-db")
    
    print(f"[Database] Connecting to {server}/{database} with Azure AD authentication")
    
    # Connection string without username/password - authentication via token
    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server"
        f"&Encrypt=yes"
    )
    
    return connection_string


def create_db_engine():
    """
    Create SQLAlchemy engine with Azure AD authentication.
    
    Pool settings:
        - pool_size=4: One connection per Gunicorn worker (typical setup)
        - max_overflow=8: Allow burst connections
        - pool_pre_ping=True: Verify connections before use (handles disconnects)
        - pool_recycle=3600: Recycle connections every hour
    
    Note: Azure AD tokens expire after 1 hour, so pool_recycle=3600 ensures
          connections are refreshed before token expiration.
    
    Returns:
        Engine: Configured SQLAlchemy engine with Azure AD authentication
    """
    connection_string = get_connection_string()
    
    # Get Azure AD token for authentication
    azure_token = get_azure_sql_token()
    
    engine = create_engine(
        connection_string,
        pool_size=4,
        max_overflow=8,
        pool_pre_ping=True,
        pool_recycle=3600,  # Critical: Recycle before token expires (1 hour)
        echo=os.getenv("SQL_ECHO", "false").lower() == "true",
        connect_args={
            "attrs_before": {
                # Pass Azure AD token to pyodbc for authentication
                pyodbc.SQL_COPT_SS_ACCESS_TOKEN: azure_token
            }
        }
    )
    
    # Set isolation level for SQL Server
    @event.listens_for(engine, "connect")
    def set_isolation_level(dbapi_conn, connection_record):
        """Set transaction isolation level to READ COMMITTED (default, but explicit)"""
        dbapi_conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    
    return engine


# Create global engine instance
engine = create_db_engine()

# Create SessionLocal factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db() -> Generator:
    """
    FastAPI dependency for database sessions.
    
    Usage:
        @app.get("/items")
        def get_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    
    Yields:
        Session: SQLAlchemy database session
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """
    Initialize database by creating all tables.
    Only used for initial setup - Alembic migrations should be used after that.
    """
    from src.models import EntityMapping, UnmappedLog, ReportRun, AuditLog
    Base.metadata.create_all(bind=engine)
    print("[Database] All tables created successfully")


def test_connection():
    """
    Test database connection and print server version.
    Useful for verifying setup.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@VERSION AS version"))
            version = result.scalar()
            print(f"[Database] Connection successful!")
            print(f"[Database] SQL Server version: {version[:80]}...")
            return True
    except Exception as e:
        print(f"[Database] Connection failed: {e}")
        return False
