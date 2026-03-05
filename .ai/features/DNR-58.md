# DNR-58: Admin UI Interactive CRUD Implementation

**Epic**: Entity Mapping Management System  
**Parent Ticket**: DNR-57 (Foundation & Read-Only Views)  
**Status**: Ready for Development  
**Priority**: High  
**Estimated Effort**: 18-23 hours (Sprint 1 + Sprint 2)

---

## Overview

Implement full interactive CRUD (Create, Read, Update, Delete) functionality for the Admin UI entity mapping management system. This builds on DNR-57's foundation which delivered the database infrastructure, read-only views, and complete backend API.

---

## Prerequisites (✅ Completed in DNR-57)

- ✅ Azure SQL Database with entity_mappings and unmapped_logs tables
- ✅ Backend API with 15 REST endpoints (admin_routes.py)
- ✅ Read-only HTML templates (admin_mappings.html, admin_unmapped.html)
- ✅ Azure AD authentication for database
- ✅ 246 entity mappings seeded from CSV
- ✅ Audit logging infrastructure (AuditLog table)

---

## Scope: Sprint 1 - Core CRUD Operations

### **Task 1: Create New Mapping Modal** 
**Priority**: HIGH | **Effort**: 3-4 hours

**Acceptance Criteria**:
- [ ] Clicking "+ New Mapping" button opens a modal dialog
- [ ] Modal contains form with all EntityMapping fields:
  - Customer Code (optional, number)
  - Customer Name (optional, text)
  - Sales Employee (optional, text)
  - Entity (required, dropdown: Customer/Employee)
  - Region (required, searchable dropdown - loads from /api/reference/regions)
  - Sub-Region (optional, text)
  - Market Group (required, searchable dropdown - loads from /api/reference/market-groups)
  - Channel Level (required, searchable dropdown - loads from /api/reference/channel-levels)
  - Company Group (optional, text)
- [ ] Form validation prevents submission if:
  - All of customer_code, customer_name, and sales_employee are empty
  - Required fields (entity, region, market_group, channel_level) are missing
- [ ] Successful submission to POST /api/mappings shows:
  - Success toast notification with green checkmark
  - Modal closes automatically
  - Grid refreshes to show new mapping at top
- [ ] Failed submission shows inline validation errors
- [ ] Modal can be closed with X button or ESC key

**Technical Implementation**:
- Use native modal or SweetAlert2 library
- Searchable dropdowns using Choices.js or native datalist
- AJAX submission with fetch() or axios
- Optimistic UI update (add row immediately, rollback on error)

**Files to Create/Modify**:
- `static/css/admin.css` - Modal container, form styling
- `static/js/admin_mappings.js` - Modal open/close, form submission logic
- `templates/admin_mappings.html` - Wire "+ New Mapping" button, add modal HTML

---

### **Task 2: Edit Existing Mapping**
**Priority**: HIGH | **Effort**: 4-5 hours

**Acceptance Criteria**:
- [ ] Clicking "Edit" button on any row opens edit modal
- [ ] Modal pre-populated with current mapping values
- [ ] Modal fetches fresh data via GET /api/mappings/{id}
- [ ] All fields editable (same form as Create modal)
- [ ] Before/after comparison shown for changed fields:
  - Changed fields highlighted in yellow/orange
  - Side-by-side display: "Old Value → New Value"
- [ ] Confirmation required for critical field changes (Region, Market, Channel):
  - "Are you sure? This will affect report categorization."
- [ ] Successful submission to PUT /api/mappings/{id}:
  - Row updates in-place with fade animation
  - Success toast: "Mapping updated successfully"
  - Shows "Last updated: just now" timestamp
- [ ] Failed submission shows inline validation errors
- [ ] Cancel button reverts all changes

**Technical Implementation**:
- Reuse create modal HTML with "Edit Mode" flag
- Store original values for comparison
- Diff algorithm to highlight changes
- Optimistic update with rollback on error

**Files to Create/Modify**:
- `static/js/admin_mappings.js` - Edit modal logic, diff comparison
- `templates/admin_mappings.html` - Wire "Edit" buttons, add comparison UI
- `static/css/admin.css` - Changed field highlighting, comparison panel

---

### **Task 3: Soft Delete Mapping**
**Priority**: MEDIUM | **Effort**: 2-3 hours

**Acceptance Criteria**:
- [ ] "Delete" button (trash icon) added to each mapping row
- [ ] Clicking delete shows confirmation dialog:
  - "Delete mapping for [Customer Name]?"
  - Shows mapping details (Region, Market, Channel)
  - Warning: "Entity will become unmapped in future reports"
  - "Delete" (red) and "Cancel" (gray) buttons
- [ ] Successful deletion to DELETE /api/mappings/{id}:
  - Row fades out with slide-left animation
  - Row removed from DOM after animation
  - Success toast: "Mapping deleted"
- [ ] Failed deletion shows error toast
- [ ] "Show Inactive Mappings" toggle filter added to toolbar:
  - When enabled, deleted mappings show in gray
  - "Restore" button appears on inactive mappings (optional)

**Technical Implementation**:
- Native confirm() or SweetAlert2 for confirmation
- CSS animation for row removal
- Update GET /admin/mappings query to filter by is_active
- Add is_active toggle to toolbar

**Files to Create/Modify**:
- `static/js/admin_mappings.js` - Delete handler with confirmation
- `templates/admin_mappings.html` - Add delete button, inactive toggle
- `admin_routes.py` - Add is_active filter to GET /admin/mappings
- `static/css/admin.css` - Delete animation, inactive row styling

---

## Scope: Sprint 2 - Unmapped Entity Resolution

### **Task 4: Link Unmapped Entity to Existing Mapping**
**Priority**: HIGH | **Effort**: 5-6 hours

**Acceptance Criteria**:
- [ ] Clicking "Link" button opens slide-out drawer from right side
- [ ] Drawer contains:
  - **Top**: Unmapped entity details (name, code, occurrences, AR value)
  - **Search box**: "Search existing mappings..." with live autocomplete
  - **Results**: List of matching mappings from /api/mappings/search
  - **Suggested Matches** section (fuzzy matching):
    - Shows top 3 similar mappings with confidence scores (95%, 80%, etc.)
    - Badge colors: Green (>90%), Yellow (70-90%), Orange (<70%)
  - **Selected Mapping**: Full details of clicked result
  - **Confirm Link** button at bottom
- [ ] Successful link to POST /api/unmapped/{id}/resolve:
  - Row slides out to right with animation
  - Green checkmark appears
  - Statistics update: Pending -1, Resolved +1
  - Success toast: "Linked to [Mapping Name]"
- [ ] Drawer closes on Cancel, ESC, or clicking outside
- [ ] Failed link shows error message in drawer

**Fuzzy Matching Logic**:
- Use Levenshtein distance to find similar entity names
- Consider customer_code exact matches first
- Suggest mappings with same region/market if available
- Option to add backend endpoint /api/mappings/suggest?query=X

**Technical Implementation**:
- Slide-out drawer with CSS transform animation
- Fuse.js for client-side fuzzy search
- Debounced autocomplete (300ms delay)
- Highlight matching characters in search results

**Files to Create/Modify**:
- `templates/admin_unmapped.html` - Wire "Link" buttons, add drawer HTML
- `static/js/admin_unmapped.js` - Link workflow, autocomplete, fuzzy search
- `static/css/admin.css` - Drawer sliding animation, search styling
- `admin_routes.py` - (Optional) Add GET /api/mappings/suggest for fuzzy backend

---

### **Task 5: Create New Mapping from Unmapped Entity**
**Priority**: HIGH | **Effort**: 4-5 hours

**Acceptance Criteria**:
- [ ] Clicking "Create" button opens "Create & Resolve" modal
- [ ] Modal pre-filled with unmapped entity data:
  - Customer Name/Code (if customer) - read-only, highlighted
  - Sales Employee (if employee) - read-only, highlighted
  - Entity type - auto-selected based on unmapped type
- [ ] Editable fields for mapping:
  - Region (required, dropdown)
  - Sub-Region (optional)
  - Market Group (required, dropdown)
  - Channel Level (required, dropdown)
  - Company Group (optional)
- [ ] Smart defaults applied:
  - Pre-select most common Region for similar customers
  - Suggest Market based on customer code patterns
  - Default Channel to most frequent value
- [ ] Successful submission to POST /api/unmapped/{id}/create-and-resolve:
  - Row slides out with celebration animation (green checkmark + subtle confetti)
  - Statistics update immediately
  - Success toast: "New mapping created & resolved"
  - Option to "View Mapping" (navigates to /admin/mappings with filter)
- [ ] Failed submission shows inline validation errors
- [ ] Preview shows: "This will resolve X occurrences totaling €Y,YYY"

**Smart Defaults Logic**:
- Query similar customer_codes for region patterns
- Use most common market_group in database
- Default channel_level to "Retail" if customer, "Direct" if employee

**Technical Implementation**:
- Reuse create modal with pre-filled read-only fields
- Backend endpoint returns smart default suggestions
- Celebration animation with CSS keyframes
- Two-step confirmation: Create → Resolve

**Files to Create/Modify**:
- `templates/admin_unmapped.html` - Wire "Create" buttons, add create modal
- `static/js/admin_unmapped.js` - Create workflow, smart defaults
- `static/css/admin.css` - Celebration animation, read-only field styling
- `admin_routes.py` - Enhance POST /api/unmapped/{id}/create-and-resolve with defaults

---

## Technical Requirements

### Frontend Libraries to Add
```html
<!-- Modal/Alert Library -->
<script src="https://cdn.jsdelivr.net/npm/sweetalert2@11"></script>

<!-- Fuzzy Search -->
<script src="https://cdn.jsdelivr.net/npm/fuse.js@6.6.2"></script>

<!-- AJAX Requests -->
<script src="https://unpkg.com/axios/dist/axios.min.js"></script>

<!-- Searchable Dropdowns -->
<script src="https://cdn.jsdelivr.net/npm/choices.js/public/assets/scripts/choices.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/choices.js/public/assets/styles/choices.min.css">
```

### File Structure
```
fastapi_web_app/
├── static/
│   ├── css/
│   │   └── admin.css          🆕 Admin-specific styles
│   └── js/
│       ├── admin_mappings.js  🆕 Mappings CRUD logic
│       └── admin_unmapped.js  🆕 Unmapped resolution logic
└── templates/
    ├── admin_mappings.html    ✏️ Enhance with modals
    └── admin_unmapped.html    ✏️ Enhance with drawer
```

---

## Testing Checklist

### Sprint 1 Tests
- [ ] **Create Mapping Modal**
  - [ ] Opens on "+ New Mapping" click
  - [ ] Validates required fields
  - [ ] Prevents submission with no entity identifiers
  - [ ] Successfully creates mapping via API
  - [ ] Shows success toast and closes modal
  - [ ] Grid refreshes with new mapping
  - [ ] Handles API errors gracefully

- [ ] **Edit Mapping**
  - [ ] Opens with correct pre-filled data
  - [ ] Highlights changed fields
  - [ ] Shows before/after comparison
  - [ ] Confirms critical field changes
  - [ ] Successfully updates via API
  - [ ] Row updates in-place
  - [ ] Handles API errors gracefully

- [ ] **Delete Mapping**
  - [ ] Shows confirmation dialog
  - [ ] Displays mapping details in confirmation
  - [ ] Successfully soft deletes via API
  - [ ] Row animates out and disappears
  - [ ] "Show Inactive" toggle works
  - [ ] Inactive mappings appear grayed out

### Sprint 2 Tests
- [ ] **Link Existing**
  - [ ] Drawer slides in from right
  - [ ] Shows unmapped entity details
  - [ ] Autocomplete search works
  - [ ] Fuzzy matches show with confidence scores
  - [ ] Successfully links to existing mapping
  - [ ] Statistics update correctly
  - [ ] Row disappears with animation

- [ ] **Create from Unmapped**
  - [ ] Modal pre-fills unmapped data
  - [ ] Smart defaults populate dropdowns
  - [ ] Successfully creates and resolves
  - [ ] Celebration animation plays
  - [ ] Statistics update correctly
  - [ ] "View Mapping" link works

### Cross-Browser Testing
- [ ] Chrome (latest)
- [ ] Edge (latest)
- [ ] Firefox (latest)
- [ ] Safari (if available)

### Accessibility Testing
- [ ] Keyboard navigation works (Tab, Enter, ESC)
- [ ] Screen reader compatible (test with NVDA/JAWS)
- [ ] Focus indicators visible
- [ ] Color contrast meets WCAG 2.1 AA

---

## Success Metrics

**Functional Completeness**:
- All 5 tasks pass acceptance criteria
- Zero critical bugs in testing
- API error handling covers all edge cases

**User Experience**:
- Modal/drawer animations smooth (60fps)
- Forms submit in < 500ms (excluding network)
- Success/error feedback always visible
- No broken UI states (loading, error, success)

**Code Quality**:
- JavaScript follows consistent patterns
- CSS organized and documented
- No console errors or warnings
- Audit logs captured for all actions

---

## Out of Scope (Future Tickets)

These tasks from the original roadmap will be addressed in later tickets:

- ❌ Task 6: Advanced Search & Filtering (DNR-59)
- ❌ Task 7: Bulk Operations (DNR-60)
- ❌ Task 8: Audit Trail History Panel (DNR-61)
- ❌ Task 9: Impact Preview (DNR-62)
- ❌ Task 10: Keyboard Shortcuts & Accessibility (DNR-63)

---

## Dependencies

**Requires DNR-57 Completion**:
- Database tables and models
- Backend API endpoints (admin_routes.py)
- Base HTML templates
- Azure AD authentication

**No Blocking Dependencies**: Can start immediately

---

## Implementation Timeline

### Sprint 1 (Week 1): Core CRUD - ~10 hours
**Days 1-2** (6 hours):
- Task 1: Create Mapping Modal (3-4h)
- Task 3: Delete Mapping (2-3h)

**Days 3-5** (4-5 hours):
- Task 2: Edit Mapping (4-5h)

### Sprint 2 (Week 2): Unmapped Resolution - ~10 hours
**Days 1-3** (5-6 hours):
- Task 4: Link Existing workflow (5-6h)

**Days 4-5** (4-5 hours):
- Task 5: Create from Unmapped (4-5h)

**Total**: 18-23 hours over 2 weeks

---

## Definition of Done

- [ ] All 5 tasks completed with acceptance criteria met
- [ ] Code committed to feature/DNR-58-admin-crud branch
- [ ] Manual testing completed (checklist above)
- [ ] Demo video recorded showing all workflows
- [ ] Documentation updated (ADMIN_UI_CRUD_ROADMAP.md)
- [ ] Pull request created and reviewed
- [ ] Merged to main after approval
- [ ] Deployed to test environment
- [ ] UAT sign-off from product owner

---

**Created**: 2026-03-05  
**Author**: Bradley (GitHub Copilot assisted)  
**Ticket Type**: Feature Development  
**Epic**: Entity Mapping Management System
