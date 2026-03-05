"""
Admin UI routes for managing entity mappings and resolving unmapped entities.
Part of DNR-57 Phase 4: Admin UI implementation.
"""
from fastapi import APIRouter, Request, Form, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from sqlalchemy import desc, func, or_
from typing import Optional
import sys

# Add parent directory to path to import from src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import engine
from src.models import EntityMapping, UnmappedLog, AuditLog
from datetime import datetime

router = APIRouter(prefix="/admin", tags=["admin"])

# Templates
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

# Database session maker
SessionLocal = sessionmaker(bind=engine)


# ================================
# MAIN VIEWS
# ================================

@router.get("/mappings", response_class=HTMLResponse)
async def admin_mappings_page(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=500),
    search: Optional[str] = None,
    filter_by: Optional[str] = None
):
    """
    Main admin page for viewing and managing entity mappings.
    Displays a searchable, filterable grid of all active mappings.
    """
    session = SessionLocal()
    try:
        # Build query
        query = session.query(EntityMapping).filter_by(is_active=True)
        
        # Apply search filter
        if search:
            search_term = f"%{search}%"
            query = query.filter(
                or_(
                    EntityMapping.customer_name.ilike(search_term),
                    EntityMapping.sales_employee.ilike(search_term),
                    EntityMapping.region.ilike(search_term),
                    EntityMapping.market_group.ilike(search_term)
                )
            )
        
        # Apply filter by type
        if filter_by == "customers":
            query = query.filter(EntityMapping.customer_name.isnot(None))
        elif filter_by == "employees":
            query = query.filter(EntityMapping.sales_employee.isnot(None))
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * per_page
        mappings = query.order_by(desc(EntityMapping.updated_at)).offset(offset).limit(per_page).all()
        
        # Calculate pagination
        total_pages = (total_count + per_page - 1) // per_page
        
        return templates.TemplateResponse(
            "admin_mappings.html",
            {
                "request": request,
                "mappings": mappings,
                "page": page,
                "per_page": per_page,
                "total_count": total_count,
                "total_pages": total_pages,
                "search": search or "",
                "filter_by": filter_by or "all"
            }
        )
    finally:
        session.close()


@router.get("/unmapped", response_class=HTMLResponse)
async def admin_unmapped_page(
    request: Request,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=500),
    status: Optional[str] = "pending",
    sort_by: Optional[str] = "ar_value",
    entity_type: Optional[str] = None
):
    """
    Admin page for reviewing and resolving unmapped entities.
    Shows entities that need mapping decisions.
    """
    session = SessionLocal()
    try:
        # Build query
        query = session.query(UnmappedLog)
        
        # Filter by status
        if status and status != "all":
            query = query.filter_by(resolution_status=status)
        
        # Filter by entity type
        if entity_type and entity_type != "all":
            query = query.filter_by(entity_type=entity_type)
        
        # Apply sorting
        if sort_by == "ar_value":
            query = query.order_by(desc(UnmappedLog.total_ar_value))
        elif sort_by == "count":
            query = query.order_by(desc(UnmappedLog.occurrence_count))
        elif sort_by == "recent":
            query = query.order_by(desc(UnmappedLog.last_seen))
        else:
            query = query.order_by(desc(UnmappedLog.total_ar_value))
        
        # Get total count
        total_count = query.count()
        
        # Apply pagination
        offset = (page - 1) * per_page
        unmapped_items = query.offset(offset).limit(per_page).all()
        
        # Get statistics
        stats = {
            "pending": session.query(UnmappedLog).filter_by(resolution_status='pending').count(),
            "resolved": session.query(UnmappedLog).filter_by(resolution_status='resolved').count(),
            "ignored": session.query(UnmappedLog).filter_by(resolution_status='ignored').count(),
        }
        stats["total"] = stats["pending"] + stats["resolved"] + stats["ignored"]
        
        # Calculate pagination
        total_pages = (total_count + per_page - 1) // per_page
        
        return templates.TemplateResponse(
            "admin_unmapped.html",
            {
                "request": request,
                "unmapped_items": unmapped_items,
                "page": page,
                "per_page": per_page,
                "total_count": total_count,
                "total_pages": total_pages,
                "status": status or "pending",
                "sort_by": sort_by or "ar_value",
                "entity_type": entity_type or "all",
                "stats": stats
            }
        )
    finally:
        session.close()


# ================================
# API ENDPOINTS - Mappings
# ================================

@router.get("/api/mappings/{mapping_id}")
async def get_mapping(mapping_id: int):
    """Get a single mapping by ID."""
    session = SessionLocal()
    try:
        mapping = session.query(EntityMapping).filter_by(id=mapping_id).first()
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        return {
            "id": mapping.id,
            "customer_code": mapping.customer_code,
            "customer_name": mapping.customer_name,
            "sales_employee": mapping.sales_employee,
            "entity": mapping.entity,
            "market_group": mapping.market_group,
            "region": mapping.region,
            "sub_region": mapping.sub_region,
            "channel_level": mapping.channel_level,
            "company_group": mapping.company_group,
            "sales_employee_cleaned": mapping.sales_employee_cleaned,
            "is_active": mapping.is_active,
            "created_at": mapping.created_at.isoformat(),
            "updated_at": mapping.updated_at.isoformat()
        }
    finally:
        session.close()


@router.post("/api/mappings")
async def create_mapping(
    customer_code: Optional[int] = Form(None),
    customer_name: Optional[str] = Form(None),
    sales_employee: Optional[str] = Form(None),
    entity: str = Form(...),
    market_group: str = Form(...),
    region: str = Form(...),
    sub_region: Optional[str] = Form(None),
    channel_level: str = Form(...),
    company_group: Optional[str] = Form(None),
    sales_employee_cleaned: Optional[str] = Form(None),
    user_email: str = Form("admin@qmsmedicosmetics.com")
):
    """Create a new entity mapping."""
    session = SessionLocal()
    try:
        # Create new mapping
        new_mapping = EntityMapping(
            customer_code=customer_code,
            customer_name=customer_name,
            sales_employee=sales_employee,
            entity=entity,
            market_group=market_group,
            region=region,
            sub_region=sub_region,
            channel_level=channel_level,
            company_group=company_group,
            sales_employee_cleaned=sales_employee_cleaned,
            created_by=user_email
        )
        
        session.add(new_mapping)
        session.commit()
        session.refresh(new_mapping)
        
        # Create audit log
        audit = AuditLog(
            user_email=user_email,
            action="create_mapping",
            entity_type="entity_mapping",
            entity_id=new_mapping.id,
            changes=f"Created mapping: {customer_name or sales_employee} → {region}/{market_group}"
        )
        session.add(audit)
        session.commit()
        
        return {"success": True, "mapping_id": new_mapping.id}
        
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.put("/api/mappings/{mapping_id}")
async def update_mapping(
    mapping_id: int,
    customer_code: Optional[int] = Form(None),
    customer_name: Optional[str] = Form(None),
    sales_employee: Optional[str] = Form(None),
    entity: str = Form(...),
    market_group: str = Form(...),
    region: str = Form(...),
    sub_region: Optional[str] = Form(None),
    channel_level: str = Form(...),
    company_group: Optional[str] = Form(None),
    sales_employee_cleaned: Optional[str] = Form(None),
    user_email: str = Form("admin@qmsmedicosmetics.com")
):
    """Update an existing entity mapping."""
    session = SessionLocal()
    try:
        mapping = session.query(EntityMapping).filter_by(id=mapping_id).first()
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        # Track changes
        changes = []
        if mapping.entity != entity:
            changes.append(f"entity: {mapping.entity} → {entity}")
        if mapping.market_group != market_group:
            changes.append(f"market_group: {mapping.market_group} → {market_group}")
        if mapping.region != region:
            changes.append(f"region: {mapping.region} → {region}")
        
        # Update fields
        mapping.customer_code = customer_code
        mapping.customer_name = customer_name
        mapping.sales_employee = sales_employee
        mapping.entity = entity
        mapping.market_group = market_group
        mapping.region = region
        mapping.sub_region = sub_region
        mapping.channel_level = channel_level
        mapping.company_group = company_group
        mapping.sales_employee_cleaned = sales_employee_cleaned
        
        session.commit()
        
        # Create audit log
        if changes:
            audit = AuditLog(
                user_email=user_email,
                action="update_mapping",
                entity_type="entity_mapping",
                entity_id=mapping_id,
                changes="; ".join(changes)
            )
            session.add(audit)
            session.commit()
        
        return {"success": True}
        
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.delete("/api/mappings/{mapping_id}")
async def delete_mapping(
    mapping_id: int,
    user_email: str = Form("admin@qmsmedicosmetics.com")
):
    """Soft delete a mapping (set is_active=False)."""
    session = SessionLocal()
    try:
        mapping = session.query(EntityMapping).filter_by(id=mapping_id).first()
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        mapping.is_active = False
        session.commit()
        
        # Create audit log
        audit = AuditLog(
            user_email=user_email,
            action="delete_mapping",
            entity_type="entity_mapping",
            entity_id=mapping_id,
            changes=f"Deactivated mapping: {mapping.customer_name or mapping.sales_employee}"
        )
        session.add(audit)
        session.commit()
        
        return {"success": True}
        
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


# ================================
# API ENDPOINTS - Unmapped Entities
# ================================

@router.post("/api/unmapped/{unmapped_id}/resolve")
async def resolve_unmapped(
    unmapped_id: int,
    mapping_id: int = Form(...),
    user_email: str = Form("admin@qmsmedicosmetics.com")
):
    """
    Resolve an unmapped entity by linking it to an existing mapping.
    """
    session = SessionLocal()
    try:
        unmapped = session.query(UnmappedLog).filter_by(id=unmapped_id).first()
        if not unmapped:
            raise HTTPException(status_code=404, detail="Unmapped entity not found")
        
        mapping = session.query(EntityMapping).filter_by(id=mapping_id).first()
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        # Update unmapped status
        unmapped.status = "resolved"
        unmapped.resolved_by = user_email
        unmapped.resolved_at = datetime.utcnow()
        unmapped.resolved_to_mapping_id = mapping_id
        
        session.commit()
        
        # Create audit log
        audit = AuditLog(
            user_email=user_email,
            action="resolve_unmapped",
            entity_type="unmapped_log",
            entity_id=unmapped_id,
            changes=f"Resolved {unmapped.entity_type} '{unmapped.entity_name}' to mapping ID {mapping_id}"
        )
        session.add(audit)
        session.commit()
        
        return {"success": True}
        
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.post("/api/unmapped/{unmapped_id}/ignore")
async def ignore_unmapped(
    unmapped_id: int,
    user_email: str = Form("admin@qmsmedicosmetics.com")
):
    """Mark an unmapped entity as ignored."""
    session = SessionLocal()
    try:
        unmapped = session.query(UnmappedLog).filter_by(id=unmapped_id).first()
        if not unmapped:
            raise HTTPException(status_code=404, detail="Unmapped entity not found")
        
        unmapped.status = "ignored"
        unmapped.resolved_by = user_email
        unmapped.resolved_at = datetime.utcnow()
        
        session.commit()
        
        # Create audit log
        audit = AuditLog(
            user_email=user_email,
            action="ignore_unmapped",
            entity_type="unmapped_log",
            entity_id=unmapped_id,
            changes=f"Ignored {unmapped.entity_type} '{unmapped.entity_name}'"
        )
        session.add(audit)
        session.commit()
        
        return {"success": True}
        
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


@router.post("/api/unmapped/{unmapped_id}/create-and-resolve")
async def create_mapping_and_resolve(
    unmapped_id: int,
    entity: str = Form(...),
    market_group: str = Form(...),
    region: str = Form(...),
    sub_region: Optional[str] = Form(None),
    channel_level: str = Form(...),
    company_group: Optional[str] = Form(None),
    user_email: str = Form("admin@qmsmedicosmetics.com")
):
    """
    Create a new mapping for an unmapped entity and resolve it in one step.
    """
    session = SessionLocal()
    try:
        unmapped = session.query(UnmappedLog).filter_by(id=unmapped_id).first()
        if not unmapped:
            raise HTTPException(status_code=404, detail="Unmapped entity not found")
        
        # Create new mapping based on unmapped entity
        new_mapping = EntityMapping(
            customer_code=int(unmapped.customer_code) if unmapped.customer_code and unmapped.customer_code.isdigit() else None,
            customer_name=unmapped.entity_name if unmapped.entity_type == 'customer' else None,
            sales_employee=unmapped.entity_name if unmapped.entity_type == 'employee' else None,
            entity=entity,
            market_group=market_group,
            region=region,
            sub_region=sub_region,
            channel_level=channel_level,
            company_group=company_group,
            created_by=user_email
        )
        
        session.add(new_mapping)
        session.flush()  # Get the ID without committing
        
        # Resolve unmapped entity
        unmapped.status = "resolved"
        unmapped.resolved_by = user_email
        unmapped.resolved_at = datetime.utcnow()
        unmapped.resolved_to_mapping_id = new_mapping.id
        
        session.commit()
        session.refresh(new_mapping)
        
        # Create audit log
        audit = AuditLog(
            user_email=user_email,
            action="create_and_resolve",
            entity_type="unmapped_log",
            entity_id=unmapped_id,
            changes=f"Created mapping (ID {new_mapping.id}) and resolved {unmapped.entity_type} '{unmapped.entity_name}'"
        )
        session.add(audit)
        session.commit()
        
        return {"success": True, "mapping_id": new_mapping.id}
        
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        session.close()


# ================================
# API ENDPOINTS - Lookups/Reference
# ================================

@router.get("/api/mappings/search")
async def search_mappings(q: str = Query(..., min_length=2)):
    """
    Search for mappings by entity name (for dropdown autocomplete).
    Returns simplified results for quick selection.
    """
    session = SessionLocal()
    try:
        search_term = f"%{q}%"
        mappings = session.query(EntityMapping).filter(
            EntityMapping.is_active == True,
            or_(
                EntityMapping.customer_name.ilike(search_term),
                EntityMapping.sales_employee.ilike(search_term)
            )
        ).limit(20).all()
        
        results = []
        for m in mappings:
            entity_name = m.customer_name or m.sales_employee
            results.append({
                "id": m.id,
                "entity_name": entity_name,
                "region": m.region,
                "market_group": m.market_group,
                "label": f"{entity_name} → {m.region}/{m.market_group}"
            })
        
        return {"results": results}
        
    finally:
        session.close()


@router.get("/api/reference/regions")
async def get_regions():
    """Get list of unique regions for dropdown."""
    session = SessionLocal()
    try:
        regions = session.query(EntityMapping.region).filter(
            EntityMapping.is_active == True,
            EntityMapping.region.isnot(None)
        ).distinct().order_by(EntityMapping.region).all()
        
        return {"regions": [r[0] for r in regions if r[0]]}
    finally:
        session.close()


@router.get("/api/reference/market-groups")
async def get_market_groups():
    """Get list of unique market groups for dropdown."""
    session = SessionLocal()
    try:
        market_groups = session.query(EntityMapping.market_group).filter(
            EntityMapping.is_active == True,
            EntityMapping.market_group.isnot(None)
        ).distinct().order_by(EntityMapping.market_group).all()
        
        return {"market_groups": [m[0] for m in market_groups if m[0]]}
    finally:
        session.close()


@router.get("/api/reference/channel-levels")
async def get_channel_levels():
    """Get list of unique channel levels for dropdown."""
    session = SessionLocal()
    try:
        channels = session.query(EntityMapping.channel_level).filter(
            EntityMapping.is_active == True,
            EntityMapping.channel_level.isnot(None)
        ).distinct().order_by(EntityMapping.channel_level).all()
        
        return {"channel_levels": [c[0] for c in channels if c[0]]}
    finally:
        session.close()
