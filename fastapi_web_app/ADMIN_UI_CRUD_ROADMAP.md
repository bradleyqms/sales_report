# Admin UI - Full CRUD Functionality Roadmap

**Reference Document for DNR-58 and Beyond**  
Comprehensive plan for interactive CRUD implementation across multiple tickets.

---

## DNR-57: Foundation (✅ COMPLETED - 2026-03-05)

### Backend API (admin_routes.py)
- ✅ GET /admin/mappings - Paginated grid view with search/filters
- ✅ GET /admin/unmapped - Unmapped entities queue with stats
- ✅ GET /api/mappings/{id} - Single mapping retrieval
- ✅ POST /api/mappings - Create new mapping
- ✅ PUT /api/mappings/{id} - Update existing mapping
- ✅ DELETE /api/mappings/{id} - Soft delete mapping
- ✅ POST /api/unmapped/{id}/resolve - Link unmapped to existing
- ✅ POST /api/unmapped/{id}/ignore - Mark as ignored
- ✅ POST /api/unmapped/{id}/create-and-resolve - Create + resolve
- ✅ GET /api/mappings/search - Autocomplete search
- ✅ GET /api/reference/* - Dropdown data (regions, markets, channels)

### Frontend Templates (Read-Only)
- ✅ admin_mappings.html - Grid with search/filters/pagination
- ✅ admin_unmapped.html - Queue with stats and placeholders
- ✅ Azure SQL Database with 246 entity mappings
- ✅ Azure AD authentication working
- ✅ Audit logging infrastructure in place

**See**: [DNR-57-COMPLETE.md](./.ai/features/DNR-57-COMPLETE.md) for full details

---

## DNR-58: Interactive CRUD (🎯 NEXT - Sprint 1-2)

**Status**: Ready to start  
**Effort**: 18-23 hours over 2 weeks  
**Priority**: HIGH  

**Tasks Included**:
- ✏️ Task 1: Create Mapping Modal (3-4h)
- ✏️ Task 2: Edit Mapping (4-5h)
- ✏️ Task 3: Delete Mapping (2-3h)
- ✏️ Task 4: Link Existing (Unmapped) (5-6h)
- ✏️ Task 5: Create from Unmapped (4-5h)

**See**: [DNR-58.md](./.ai/features/DNR-58.md) for acceptance criteria and implementation plan

---

## 🎯 Roadmap: Full CRUD Implementation

### **Task 1: Create New Mapping Modal** (Priority: HIGH)
**Goal**: Allow users to add new entity mappings from the UI

**Frontend Changes**:
1. Add floating "+ New Mapping" button (already exists, needs wiring)
2. Create modal dialog with form fields:
   - Customer Code (optional, number input)
   - Customer Name (optional, text input)
   - Sales Employee (optional, text input with autocomplete)
   - Entity (required, dropdown: "Customer" | "Employee")
   - Region (required, searchable dropdown from /api/reference/regions)
   - Sub-Region (optional, text input)
   - Market Group (required, searchable dropdown from /api/reference/market-groups)
   - Channel Level (required, searchable dropdown from /api/reference/channel-levels)
   - Company Group (optional, text input)
3. Form validation:
   - At least one of: customer_code, customer_name, or sales_employee required
   - All required fields must be filled
   - Duplicate detection (check if mapping already exists)
4. Submit to POST /api/mappings
5. Success: Show toast notification + close modal + refresh grid
6. Error: Display inline validation errors

**Files to Create/Modify**:
- `templates/admin_mappings.html` - Add modal HTML + JavaScript
- `static/css/admin.css` - Modal styling
- `static/js/admin_mappings.js` - Form handling logic

**Estimated Time**: 3-4 hours

---

### **Task 2: Edit Existing Mapping** (Priority: HIGH)
**Goal**: Allow users to modify existing entity mappings inline or via modal

**Frontend Changes**:
1. Wire up existing "Edit" button on each row
2. Two options (choose one):
   - **Option A: Modal Edit** (recommended for complexity)
     - Click "Edit" → Open modal pre-populated with current values
     - Same form as Create, but with mapping ID hidden
     - Submit to PUT /api/mappings/{id}
   - **Option B: Inline Edit** (more advanced UX)
     - Click "Edit" → Row becomes editable with dropdowns
     - Save/Cancel buttons appear
     - Submit to PUT /api/mappings/{id}
3. Audit trail: Show "Last updated by X at Y" tooltip
4. Optimistic UI update: Update row immediately, rollback on error

**Before/After Comparison**:
- Add side-by-side comparison panel showing old vs new values before save
- Highlight changed fields in yellow/orange
- "Are you sure?" confirmation for critical fields (Region, Market, Channel)

**Files to Create/Modify**:
- `templates/admin_mappings.html` - Edit modal or inline editing
- `static/js/admin_mappings.js` - Edit form handling

**Estimated Time**: 4-5 hours

---

### **Task 3: Delete (Soft Delete) Mapping** (Priority: MEDIUM)
**Goal**: Allow users to deactivate mappings (set is_active=False)

**Frontend Changes**:
1. Add "Delete" button to each row (icon: 🗑️ or "×")
2. Confirmation dialog:
   - "Are you sure you want to delete this mapping?"
   - Show mapping details for review
   - Warning: "This will mark the mapping as inactive. The entity will become unmapped in future reports."
3. Submit to DELETE /api/mappings/{id}
4. Success: Fade out row with slide-left animation + remove from DOM
5. Add "Show Inactive Mappings" toggle filter at top
6. Deleted mappings shown in gray with "Restore" button (optional)

**Files to Create/Modify**:
- `templates/admin_mappings.html` - Add delete button + confirmation dialog
- `static/js/admin_mappings.js` - Delete handler with confirmation
- `admin_routes.py` - Add filter for is_active status

**Estimated Time**: 2-3 hours

---

### **Task 4: Unmapped Entity Resolution - Link Existing** (Priority: HIGH)
**Goal**: Allow users to link unmapped entities to existing mappings

**Frontend Changes**:
1. Wire up "Link" button in admin_unmapped.html
2. Open slide-out drawer (right side) with:
   - Unmapped entity details at top (name, code, occurrences, AR value)
   - Search box: "Search existing mappings..."
   - Autocomplete results from /api/mappings/search
   - Click result → Show full mapping details
   - "Confirm Link" button at bottom
3. Submit to POST /api/unmapped/{id}/resolve with mapping_id
4. Success: 
   - Slide-out animation removing row
   - Green checkmark flash
   - Update statistics bar (pending -1, resolved +1)
   - Toast: "Successfully linked to [Mapping Name]"

**Fuzzy Matching Enhancement**:
- Add "Suggested Matches" section below search
- Use fuzzy string matching (Levenshtein distance) to suggest similar names
- Show confidence score badges (95%, 80%, etc.)
- Click suggestion to auto-populate

**Files to Create/Modify**:
- `templates/admin_unmapped.html` - Add slide-out drawer HTML
- `static/js/admin_unmapped.js` - Link workflow logic
- `admin_routes.py` - Add fuzzy matching endpoint (optional)

**Estimated Time**: 5-6 hours

---

### **Task 5: Unmapped Entity Resolution - Create New Mapping** (Priority: HIGH)
**Goal**: Allow users to create new mappings directly from unmapped entity

**Frontend Changes**:
1. Wire up "Create" button in admin_unmapped.html
2. Open modal with:
   - Unmapped entity details pre-filled (customer_name, customer_code, entity_type)
   - Editable fields for: Region, Sub-Region, Market, Channel, Company
   - All fields from Create Mapping form, pre-populated where possible
   - "Create & Resolve" submit button
3. Submit to POST /api/unmapped/{id}/create-and-resolve
4. Success:
   - Slide-out animation removing row
   - Green checkmark + confetti animation (optional)
   - Update stats
   - Toast: "New mapping created successfully"
   - Option to "View New Mapping" (navigate to /admin/mappings filtered)

**Smart Defaults**:
- Infer Region from system patterns (e.g., customer_code ranges)
- Suggest Market Group based on historical data
- Pre-select most common Channel Level

**Files to Create/Modify**:
- `templates/admin_unmapped.html` - Create-and-resolve modal
- `static/js/admin_unmapped.js` - Create workflow
- `admin_routes.py` - Enhance endpoint with smart defaults

**Estimated Time**: 4-5 hours

---

## Future Enhancements (DNR-59 through DNR-63)

The following tasks from the original roadmap have been split into separate tickets for future sprints:

### **Task 6: Advanced Search and Filtering** → DNR-59
**Priority**: MEDIUM | **Effort**: 3-4 hours
**Goal**: Enhanced search capabilities for power users

**Frontend Changes**:
1. **Mappings Grid**:
   - Advanced filter panel (collapsible):
     - Multi-select: Region (checkboxes)
     - Multi-select: Market Group (checkboxes)
     - Multi-select: Channel Level (checkboxes)
     - Date range: Updated Between [date picker]
     - Toggle: Include Inactive Mappings
   - "Clear Filters" button
   - Filter count badge: "3 filters active"

2. **Unmapped Queue**:
   - Filter by entity_type: Customer | Employee | All
   - Filter by AR value range: [min] to [max] kEUR
   - Filter by occurrence count: >= [N]
   - Sort by: AR Value | Count | Most Recent | Oldest

**Backend Enhancement**:
- Update GET /admin/mappings to accept multiple filter params
- Add query parameter validation
- Return filter metadata (available values for each field)

**Files to Create/Modify**:
- `templates/admin_mappings.html` - Advanced filter panel
- `templates/admin_unmapped.html` - Enhanced filters
- `admin_routes.py` - Multi-filter query logic
- `static/css/admin.css` - Filter panel styling

**Estimated Time**: 3-4 hours

---

### **Task 7: Bulk Operations** → DNR-60
**Priority**: LOW | **Effort**: 4-5 hours
**Goal**: Allow batch actions on multiple entities

**Frontend Changes**:
1. Add checkboxes to first column of grids
2. "Select All" checkbox in header
3. Bulk action bar appears when items selected:
   - "Delete Selected" (mappings)
   - "Ignore Selected" (unmapped)
   - "Export Selected" (CSV download)
4. Show selection count: "5 items selected"
5. Confirmation dialog for destructive actions

**Backend Changes**:
- Add POST /api/mappings/bulk-delete
- Add POST /api/unmapped/bulk-ignore
- Add GET /api/mappings/export?ids=1,2,3,4

**Files to Create/Modify**:
- `templates/admin_mappings.html` - Checkboxes + bulk bar
- `templates/admin_unmapped.html` - Checkboxes + bulk bar
- `admin_routes.py` - Bulk operation endpoints
- `static/js/admin_common.js` - Shared bulk logic

**Estimated Time**: 4-5 hours

---

### **Task 8: Audit Trail and History Panel** → DNR-61
**Priority**: MEDIUM | **Effort**: 3-4 hours
**Goal**: Show change history for compliance and debugging

**Frontend Changes**:
1. Add "History" button next to "Edit" on mapping rows
2. Slide-out panel showing:
   - Timeline of changes (newest first)
   - Each entry shows: User, Action, Timestamp, Old Value → New Value
   - Filter: Show only [field changes | status changes | all]
3. Diff view for complex changes
4. "Undo" button for recent changes (last 24 hours only)

**Backend Changes**:
- Add GET /api/mappings/{id}/history
- Query AuditLog table filtered by target_id
- Parse JSON detail field for before/after comparison

**Files to Create/Modify**:
- `templates/components/history_panel.html` - Reusable component
- `admin_routes.py` - History endpoint
- `static/js/admin_history.js` - History panel logic

**Estimated Time**: 3-4 hours

---

### **Task 9: Impact Preview** → DNR-62
**Priority**: LOW | **Effort**: 5-6 hours
**Goal**: Show how changes will affect reports before committing

**Frontend Changes**:
1. Before saving mapping changes, show:
   - "This change will affect N records in the database"
   - "Estimated impact on next report: €X,XXX AR value"
   - List of affected SAP extracts/customers
2. Preview mode: Show what report would look like with new mapping
3. "Commit Changes" vs "Cancel" buttons

**Backend Changes**:
- Add POST /api/mappings/preview-impact (dry-run)
- Query historical data to estimate affected records
- Return statistics without persisting

**Files to Create/Modify**:
- `templates/components/impact_preview.html`
- `admin_routes.py` - Impact preview endpoint
- `static/js/admin_mappings.js` - Preview logic

**Estimated Time**: 5-6 hours

---

### **Task 10: Keyboard Shortcuts and Accessibility** → DNR-63
**Priority**: LOW | **Effort**: 4-5 hours
**Goal**: Power user enhancements and WCAG 2.1 compliance

**Frontend Changes**:
1. Keyboard shortcuts:
   - `Ctrl+N`: New mapping
   - `Ctrl+F`: Focus search box
   - `Ctrl+S`: Save current edit
   - `Esc`: Close modal/cancel edit
   - Arrow keys: Navigate grid
   - Enter: Edit selected row
2. Accessibility improvements:
   - ARIA labels on all interactive elements
   - Tab order optimization
   - Screen reader announcements for status changes
   - High contrast mode support
   - Focus visible indicators

**Files to Create/Modify**:
- `static/js/admin_keyboard.js` - Keyboard handler
- All templates - Add ARIA attributes
- `static/css/admin.css` - Focus styles

**Estimated Time**: 4-5 hours

---

## 📊 Implementation Priority Matrix

**DNR-58 Tasks (Sprint 1-2)**: See [DNR-58.md](./.ai/features/DNR-58.md)
- ✏️ Task 1: Create Mapping Modal - **HIGH** Priority - 3-4h
- ✏️ Task 2: Edit Mapping - **HIGH** Priority - 4-5h
- ✏️ Task 3: Delete Mapping - MEDIUM Priority - 2-3h
- ✏️ Task 4: Link Existing (Unmapped) - **HIGH** Priority - 5-6h
- ✏️ Task 5: Create from Unmapped - **HIGH** Priority - 4-5h

**DNR-58 Total**: 18-23 hours

---

**Future Tickets Matrix**:

| Task | Ticket | Priority | Complexity | User Value | Time Est. |
|------|--------|----------|------------|------------|-----------|
| Task 6: Advanced Filters | DNR-59 | MEDIUM | Medium | High | 3-4h |
| Task 8: Audit Trail | DNR-61 | MEDIUM | Medium | Medium | 3-4h |
| Task 7: Bulk Operations | DNR-60 | LOW | Medium | Medium | 4-5h |
| Task 9: Impact Preview | DNR-62 | LOW | High | Medium | 5-6h |
| Task 10: Keyboard Shortcuts | DNR-63 | LOW | Medium | Low | 4-5h |

**Future Tickets Total**: 20-28 hours

---

## 🚀 Implementation Timeline

### **DNR-58: Interactive CRUD (Current Priority)**

**Sprint 1 (Week 1): Core CRUD** - ~10 hours
1. Task 1: Create Mapping Modal (3-4h)
2. Task 3: Delete Mapping (2-3h)
3. Task 2: Edit Mapping (4-5h)

**Deliverable**: Full CRUD for entity mappings

---

**Sprint 2 (Week 2): Unmapped Resolution** - ~10 hours
1. Task 4: Link Existing workflow (5-6h)
2. Task 5: Create from Unmapped (4-5h)

**Deliverable**: Complete unmapped entity resolution UI

---

### **Future Enhancements (DNR-59+)**

**Sprint 3 (Optional): Power User Features** - ~7 hours
1. DNR-59: Advanced Search/Filters (3-4h)
2. DNR-61: Audit Trail panel (3-4h)

**Deliverable**: Enhanced usability for frequent users

---

**Sprint 4 (Optional): Advanced Features** - ~15 hours
1. DNR-60: Bulk Operations (4-5h)
2. DNR-62: Impact Preview (5-6h)
3. DNR-63: Keyboard Shortcuts (4-5h)

**Deliverable**: Production-ready enterprise UI

---

## 🛠️ Technical Stack for Frontend

### Required Libraries
```html
<!-- Already included -->
<script src="https://unpkg.com/htmx.org@1.9.10"></script>

<!-- Recommended additions -->
<script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script> <!-- Modals/alerts -->
<script src="https://cdn.jsdelivr.net/npm/fuse.js@6.6.2"></script> <!-- Fuzzy search -->
<script src="https://unpkg.com/axios/dist/axios.min.js"></script> <!-- AJAX -->
<script src="https://cdn.jsdelivr.net/npm/choices.js/public/assets/scripts/choices.min.js"></script> <!-- Searchable dropdowns -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/choices.js/public/assets/styles/choices.min.css">
```

### File Structure (Recommended)
```
fastapi_web_app/
├── templates/
│   ├── admin_mappings.html            ✅ Exists (needs enhancement)
│   ├── admin_unmapped.html            ✅ Exists (needs enhancement)
│   └── components/
│       ├── mapping_modal.html         🆕 Create mapping form
│       ├── edit_modal.html            🆕 Edit mapping form
│       ├── link_drawer.html           🆕 Link workflow UI
│       ├── history_panel.html         🆕 Audit trail viewer
│       └── delete_confirm.html        🆕 Delete confirmation
├── static/
│   ├── css/
│   │   ├── admin.css                  🆕 Admin-specific styles
│   │   └── modals.css                 🆕 Modal/drawer styles
│   └── js/
│       ├── admin_common.js            🆕 Shared utilities
│       ├── admin_mappings.js          🆕 Mappings CRUD logic
│       ├── admin_unmapped.js          🆕 Unmapped resolution logic
│       ├── admin_history.js           🆕 History panel
│       └── admin_keyboard.js          🆕 Keyboard shortcuts
└── admin_routes.py                    ✅ Backend complete
```

---

## 🧪 Testing Checklist

### Unit Tests (Backend)
- [ ] Create mapping with valid data
- [ ] Create mapping with invalid data (validation errors)
- [ ] Update mapping (all fields, partial fields)
- [ ] Delete mapping (soft delete, restore)
- [ ] Link unmapped entity to existing mapping
- [ ] Create mapping from unmapped entity
- [ ] Ignore unmapped entity
- [ ] Search mappings (autocomplete)
- [ ] Filter mappings (multi-criteria)
- [ ] Pagination edge cases

### Integration Tests
- [ ] End-to-end create workflow
- [ ] End-to-end edit workflow
- [ ] End-to-end delete workflow
- [ ] Unmapped resolution workflows (all 3 paths)
- [ ] Audit log creation on all actions
- [ ] Azure AD token refresh during long sessions

### UI/UX Tests
- [ ] All modals open/close correctly
- [ ] Form validation shows appropriate errors
- [ ] Success toasts appear and auto-dismiss
- [ ] Loading spinners during API calls
- [ ] Optimistic UI updates work correctly
- [ ] Keyboard navigation works across all components
- [ ] Mobile responsive (tablet/phone)
- [ ] Browser compatibility (Chrome, Edge, Firefox, Safari)

---

## 📋 Current Status & Next Steps

**DNR-57 Status**: ✅ CLOSED (2026-03-05)
- Database infrastructure complete
- Backend API complete (15 endpoints)
- Read-only Admin UI templates complete
- See [DNR-57-COMPLETE.md](./.ai/features/DNR-57-COMPLETE.md)

**DNR-58 Status**: 🎯 READY TO START
- All prerequisites met
- Detailed acceptance criteria defined
- File structure planned
- See [DNR-58.md](./.ai/features/DNR-58.md)

**Immediate Actions** (for DNR-58 Sprint 1 kickoff):
1. Create `static/css/admin.css` stylesheet
2. Create `static/js/admin_mappings.js` with modal framework
3. Add SweetAlert2 CDN or build custom modal
4. Wire up "+ New Mapping" button to open create modal
5. Implement form submission handler for POST /api/mappings
6. Test create workflow end-to-end

**Questions Resolved**:
- ✅ Modal library: SweetAlert2 recommended (or custom HTML/CSS)
- ✅ Edit approach: Modal (not inline) for consistency
- ✅ Delete type: Soft delete only (is_active=False)
- ❌ Authentication: Deferred to DNR-64 (later sprint)
- ❌ Approval workflow: Not needed initially

---

## 🔗 Related Documentation & Tickets

**Completed**:
- [DNR-57-COMPLETE.md](./.ai/features/DNR-57-COMPLETE.md) - Phase 1-4 summary

**Current Work**:
- [DNR-58.md](./.ai/features/DNR-58.md) - Interactive CRUD implementation

**Future Work**:
- DNR-59: Advanced Search & Filtering
- DNR-60: Bulk Operations
- DNR-61: Audit Trail History Panel
- DNR-62: Impact Preview
- DNR-63: Keyboard Shortcuts & Accessibility
- DNR-64: Azure AD Authentication for Admin UI

**Code References**:
- Backend: [admin_routes.py](./admin_routes.py) - Lines 1-587
- Models: [src/models.py](../src/models.py) - EntityMapping, UnmappedLog, AuditLog
- Database: [src/database.py](../src/database.py) - Azure AD connection

---

**Document Updated**: 2026-03-05  
**Author**: GitHub Copilot  
**Status**: DNR-57 Complete | DNR-58 Ready | Roadmap Active
