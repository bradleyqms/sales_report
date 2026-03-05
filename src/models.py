"""
SQLAlchemy ORM models for Sales Report database.

Tables:
    - EntityMapping: Customer/employee mappings for sales data classification
    - UnmappedLog: Track unmapped entities from SAP extracts
    - ReportRun: Persist report execution history and status
    - AuditLog: Track user actions for compliance
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, ForeignKey, Index
from sqlalchemy.sql import func
from src.database import Base


class EntityMapping(Base):
    """
    Entity mappings for customer codes, customer names, and sales employees.
    Replaces the flat file entity_mappings.csv with persistent storage.
    """
    __tablename__ = "entity_mappings"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_code = Column(Integer, nullable=True, index=True)
    customer_name = Column(String(500), nullable=True, index=True)
    sales_employee = Column(String(200), nullable=True, index=True)
    
    # Classification fields
    entity = Column(String(100), nullable=False)
    market_group = Column(String(100), nullable=False)
    region = Column(String(100), nullable=False)
    sub_region = Column(String(100), nullable=True)
    channel_level = Column(String(100), nullable=False)
    company_group = Column(String(100), nullable=True)
    sales_employee_cleaned = Column(String(200), nullable=True)
    
    # Metadata
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    created_by = Column(String(200), nullable=False)
    
    # Composite indexes for common query patterns
    __table_args__ = (
        Index('idx_customer_code_active', 'customer_code', 'is_active'),
        Index('idx_customer_name_active', 'customer_name', 'is_active'),
        Index('idx_sales_employee_active', 'sales_employee', 'is_active'),
    )
    
    def __repr__(self):
        return f"<EntityMapping(id={self.id}, customer_code={self.customer_code}, customer_name={self.customer_name})>"


class UnmappedLog(Base):
    """
    Log entries for unmapped customers and sales employees found in SAP extracts.
    Used by the Admin UI for resolving mapping gaps.
    """
    __tablename__ = "unmapped_logs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String(20), nullable=False, index=True)  # 'customer' or 'employee'
    entity_name = Column(String(500), nullable=False, index=True)
    customer_code = Column(String(50), nullable=True)
    
    # Statistics
    count = Column(Integer, nullable=False, default=1)
    first_seen = Column(DateTime(timezone=True), nullable=True)
    last_seen = Column(DateTime(timezone=True), nullable=True)
    total_ar_value_keur = Column(Float, nullable=False, default=0.0)
    sap_extract_files = Column(Text, nullable=True)  # Comma-separated list of source files
    
    # Status tracking
    status = Column(String(20), nullable=False, default='pending', index=True)  # 'pending', 'resolved', 'ignored'
    resolved_by = Column(String(200), nullable=True)
    resolved_at = Column(DateTime(timezone=True), nullable=True)
    resolved_to_mapping_id = Column(Integer, ForeignKey('entity_mappings.id'), nullable=True)
    
    # Source tracking
    run_timestamp = Column(String(20), nullable=False, index=True)
    source_file = Column(String(200), nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    
    __table_args__ = (
        Index('idx_entity_status', 'entity_type', 'status'),
        Index('idx_entity_name_type', 'entity_name', 'entity_type'),
    )
    
    def __repr__(self):
        return f"<UnmappedLog(id={self.id}, entity_type={self.entity_type}, entity_name={self.entity_name}, status={self.status})>"


class ReportRun(Base):
    """
    Report execution history with status, outputs, and metrics.
    Replaces the in-memory report_status dict in fastapi_web_app/main.py.
    """
    __tablename__ = "report_runs"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(String(20), unique=True, nullable=True, index=True)  # Format: YYYY.MM.DD_HHMM
    status = Column(String(20), nullable=False, index=True)  # 'running', 'success', 'error', 'crashed', 'unmapped_check'
    triggered_by = Column(String(200), nullable=False, default='system')
    
    # Output tracking
    output_log = Column(Text, nullable=True)
    csv_url = Column(String(500), nullable=True)
    xlsx_url = Column(String(500), nullable=True)
    zip_url = Column(String(500), nullable=True)
    core_market_csv_url = Column(String(500), nullable=True)
    usa_spa_csv_url = Column(String(500), nullable=True)
    unmapped_url = Column(String(500), nullable=True)
    
    # Metrics (store as JSON string)
    metrics_json = Column(Text, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    __table_args__ = (
        Index('idx_status_created', 'status', 'created_at'),
    )
    
    def __repr__(self):
        return f"<ReportRun(id={self.id}, timestamp={self.timestamp}, status={self.status}, triggered_by={self.triggered_by})>"


class AuditLog(Base):
    """
    Audit log for tracking user actions on mappings and unmapped entity resolution.
    Used for compliance and troubleshooting.
    """
    __tablename__ = "audit_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_email = Column(String(200), nullable=False, index=True)
    action = Column(String(100), nullable=False, index=True)  # 'create', 'update', 'delete', 'resolve', 'ignore'
    target_type = Column(String(100), nullable=False)  # 'EntityMapping', 'UnmappedLog'
    target_id = Column(String(100), nullable=True)
    detail = Column(Text, nullable=True)  # JSON or free text description
    ip_address = Column(String(50), nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    
    __table_args__ = (
        Index('idx_user_action', 'user_email', 'action'),
        Index('idx_action_timestamp', 'action', 'timestamp'),
    )
    
    def __repr__(self):
        return f"<AuditLog(id={self.id}, user={self.user_email}, action={self.action}, target={self.target_type})>"


class TelemetryLog(Base):
    """
    Usage telemetry for the Reporting Hub.
    Best-effort — failures are logged to console only, never raised to users.
    Pruned automatically to 90 days on each App Service cold-start.
    """
    __tablename__ = "telemetry_logs"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    user_email   = Column(String(255), nullable=False)                       # lowercased
    event_type   = Column(String(20),  nullable=False)                       # page_view | export | admin_click
    page_id      = Column(String(50),  nullable=True)                        # /coremarkets, /admin/mappings, etc.
    load_time_ms = Column(Integer,     nullable=True)
    action       = Column(String(50),  nullable=True)
    entity_id    = Column(Integer,     nullable=True)
    file_format  = Column(String(10),  nullable=True)                        # csv | xlsx | pdf | zip
    report_type  = Column(String(50),  nullable=True)
    timestamp    = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index('idx_telemetry_event_ts',  'event_type', 'timestamp'),
        Index('idx_telemetry_user_ts',   'user_email',  'timestamp'),
    )

    def __repr__(self):
        return f"<TelemetryLog(id={self.id}, user={self.user_email}, event={self.event_type}, page={self.page_id})>"
