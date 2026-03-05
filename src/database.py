"""
Database connection and session management for Sales Report application.
Uses SQLAlchemy 2.0 with support for both Managed Identity (production) 
and SQL authentication (local development).
"""

import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool
from typing import Generator
import urllib.parse

# SQLAlchemy 2.0 declarative base
Base = declarative_base()


def get_connection_string() -> str:
    """
    Get database connection string based on environment.
    
    Production (Azure App Service with Managed Identity):
        Uses ActiveDirectoryMsi authentication - no password needed
        
    Local Development:
        Uses SQL authentication with username/password from .env
    
    Returns:
        str: SQLAlchemy connection string for pyodbc
    """
    # Check if running on Azure App Service (has AZURE_CLIENT_ID when MI is enabled)
    if os.getenv("AZURE_CLIENT_ID"):
        # Production: Use Managed Identity (no password)
        server = os.getenv("DATABASE_SERVER", "dnr-sql-server-qmsmedicosmetics.database.windows.net")
        database = os.getenv("DATABASE_NAME", "dnr-mapping-db")
        
        connection_string = (
            f"mssql+pyodbc://@{server}/{database}"
            f"?driver=ODBC+Driver+18+for+SQL+Server"
            f"&Authentication=ActiveDirectoryMsi"
            f"&Encrypt=yes"
        )
        print(f"[Database] Using Managed Identity connection to {server}/{database}")
        return connection_string
    else:
        # Local development: Use SQL authentication
        database_url = os.getenv("DATABASE_URL_LOCAL")
        if not database_url:
            # Construct from individual components if DATABASE_URL_LOCAL not set
            server = os.getenv("DATABASE_SERVER", "dnr-sql-server-qmsmedicosmetics.database.windows.net")
            database = os.getenv("DATABASE_NAME", "dnr-mapping-db")
            username = os.getenv("DATABASE_USER", "sqladmin")
            password = os.getenv("DATABASE_PASSWORD", "")
            
            if not password:
                raise ValueError(
                    "DATABASE_PASSWORD is required for local development. "
                    "Set it in your .env file or use DATABASE_URL_LOCAL."
                )
            
            # URL-encode password to handle special characters
            password_encoded = urllib.parse.quote_plus(password)
            
            database_url = (
                f"mssql+pyodbc://{username}:{password_encoded}@{server}/{database}"
                f"?driver=ODBC+Driver+18+for+SQL+Server"
                f"&Encrypt=yes"
                f"&TrustServerCertificate=no"
            )
        
        print(f"[Database] Using SQL authentication (local development)")
        return database_url


def create_db_engine():
    """
    Create SQLAlchemy engine with appropriate pool settings for Gunicorn workers.
    
    Pool settings:
        - pool_size=4: One connection per Gunicorn worker (typical setup)
        - max_overflow=8: Allow burst connections
        - pool_pre_ping=True: Verify connections before use (handles disconnects)
        - pool_recycle=3600: Recycle connections every hour
    
    Returns:
        Engine: Configured SQLAlchemy engine
    """
    connection_string = get_connection_string()
    
    engine = create_engine(
        connection_string,
        pool_size=4,
        max_overflow=8,
        pool_pre_ping=True,
        pool_recycle=3600,  # Recycle connections after 1 hour
        echo=os.getenv("SQL_ECHO", "false").lower() == "true",  # Set SQL_ECHO=true for query logging
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
