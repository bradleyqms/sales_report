/**
 * Admin Mappings CRUD - JavaScript
 * Part of DNR-57/DNR-58: Entity Mapping Management System
 */

// ================================
// STATE & CONFIGURATION
// ================================

const API_BASE = '/admin/api';
let referenceData = {
    regions: [],
    marketGroups: [],
    channelLevels: []
};

// ================================
// INITIALIZATION
// ================================

document.addEventListener('DOMContentLoaded', async () => {
    console.log('Admin Mappings UI initialized');
    
    // Load reference data for dropdowns
    await loadReferenceData();
    
    // Setup event listeners
    setupEventListeners();
});

// ================================
// REFERENCE DATA LOADING
// ================================

async function loadReferenceData() {
    try {
        const [regionsRes, marketsRes, channelsRes] = await Promise.all([
            fetch(`${API_BASE}/reference/regions`),
            fetch(`${API_BASE}/reference/market-groups`),
            fetch(`${API_BASE}/reference/channel-levels`)
        ]);
        
        const [regionsData, marketsData, channelsData] = await Promise.all([
            regionsRes.json(),
            marketsRes.json(),
            channelsRes.json()
        ]);
        
        referenceData.regions = regionsData.regions || [];
        referenceData.marketGroups = marketsData.market_groups || [];
        referenceData.channelLevels = channelsData.channel_levels || [];
        
        console.log('Reference data loaded:', referenceData);
    } catch (error) {
        console.error('Failed to load reference data:', error);
        showToast('error', 'Failed to load dropdown options');
    }
}

// ================================
// EVENT LISTENERS
// ================================

function setupEventListeners() {
    // Modal close handlers
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('modal-overlay')) {
            closeModal();
        }
    });
    
    // ESC key to close modal
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeModal();
        }
    });
    
    // Entity type change handler
    const entitySelect = document.getElementById('entity');
    if (entitySelect) {
        entitySelect.addEventListener('change', handleEntityTypeChange);
    }
}

// ================================
// TASK 1: CREATE NEW MAPPING MODAL
// ================================

function openCreateModal() {
    const modal = document.getElementById('createModal');
    if (!modal) {
        console.error('Create modal not found in DOM');
        return;
    }
    
    // Reset form
    const form = document.getElementById('createMappingForm');
    if (form) {
        form.reset();
        clearFormErrors(form);
    }
    
    // Populate dropdowns
    populateDropdown('region', referenceData.regions);
    populateDropdown('market_group', referenceData.marketGroups);
    populateDropdown('channel_level', referenceData.channelLevels);
    
    // Show modal
    modal.classList.add('active');
}

function closeModal() {
    const modals = document.querySelectorAll('.modal-overlay');
    modals.forEach(modal => {
        modal.classList.remove('active');
    });
}

function populateDropdown(fieldId, options) {
    const select = document.getElementById(fieldId);
    if (!select) return;
    
    // Clear existing options (except first placeholder)
    while (select.options.length > 1) {
        select.remove(1);
    }
    
    // Add new options
    options.forEach(option => {
        const opt = document.createElement('option');
        opt.value = option;
        opt.textContent = option;
        select.appendChild(opt);
    });
}

function handleEntityTypeChange(e) {
    const entityType = e.target.value;
    const customerFields = document.getElementById('customerFields');
    const employeeFields = document.getElementById('employeeFields');
    
    if (entityType === 'Customer') {
        customerFields.style.display = 'block';
        employeeFields.style.display = 'none';
        
        // Make customer fields optional (at least one required)
        document.getElementById('customer_name').removeAttribute('required');
        document.getElementById('customer_code').removeAttribute('required');
        document.getElementById('sales_employee').removeAttribute('required');
    } else if (entityType === 'Employee') {
        customerFields.style.display = 'none';
        employeeFields.style.display = 'block';
        
        // Make employee field required
        document.getElementById('sales_employee').setAttribute('required', 'required');
        document.getElementById('customer_name').removeAttribute('required');
        document.getElementById('customer_code').removeAttribute('required');
    }
}

async function submitCreateMapping(event) {
    event.preventDefault();
    
    const form = event.target;
    const submitBtn = form.querySelector('button[type="submit"]');
    
    // Validate form
    if (!validateCreateForm(form)) {
        return;
    }
    
    // Show loading state
    submitBtn.disabled = true;
    submitBtn.classList.add('btn-loading');
    const originalText = submitBtn.textContent;
    submitBtn.textContent = '';
    
    try {
        const formData = new FormData(form);
        
        // Convert customer_code to integer if present
        if (formData.get('customer_code')) {
            const code = parseInt(formData.get('customer_code'));
            if (!isNaN(code)) {
                formData.set('customer_code', code);
            }
        }
        
        const response = await fetch(`${API_BASE}/mappings`, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (response.ok && result.success) {
            // Success!
            showToast('success', 'Mapping created successfully', 'New entity mapping has been added');
            closeModal();
            
            // Refresh the table
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            throw new Error(result.detail || 'Failed to create mapping');
        }
    } catch (error) {
        console.error('Create mapping failed:', error);
        showToast('error', 'Failed to create mapping', error.message);
    } finally {
        // Reset button state
        submitBtn.disabled = false;
        submitBtn.classList.remove('btn-loading');
        submitBtn.textContent = originalText;
    }
}

function validateCreateForm(form) {
    clearFormErrors(form);
    
    let isValid = true;
    
    // Get form values
    const entity = form.entity.value;
    const customerCode = form.customer_code.value;
    const customerName = form.customer_name.value;
    const salesEmployee = form.sales_employee.value;
    const region = form.region.value;
    const marketGroup = form.market_group.value;
    const channelLevel = form.channel_level.value;
    
    // Validate required fields
    if (!entity) {
        showFieldError(form, 'entity', 'Entity type is required');
        isValid = false;
    }
    
    if (!region) {
        showFieldError(form, 'region', 'Region is required');
        isValid = false;
    }
    
    if (!marketGroup) {
        showFieldError(form, 'market_group', 'Market group is required');
        isValid = false;
    }
    
    if (!channelLevel) {
        showFieldError(form, 'channel_level', 'Channel level is required');
        isValid = false;
    }
    
    // Validate entity identifiers: at least one must be provided
    if (!customerCode && !customerName && !salesEmployee) {
        showFieldError(form, 'customer_name', 'At least one identifier is required');
        showFieldError(form, 'sales_employee', 'At least one identifier is required');
        showToast('warning', 'Missing entity identifier', 'Please provide at least one of: Customer Code, Customer Name, or Sales Employee');
        isValid = false;
    }
    
    // Validate customer code is numeric if provided
    if (customerCode && isNaN(parseInt(customerCode))) {
        showFieldError(form, 'customer_code', 'Customer code must be a number');
        isValid = false;
    }
    
    return isValid;
}

function showFieldError(form, fieldName, message) {
    const field = form.elements[fieldName];
    if (!field) return;
    
    const formGroup = field.closest('.form-group');
    if (!formGroup) return;
    
    formGroup.classList.add('has-error');
    field.classList.add('error');
    
    const errorText = formGroup.querySelector('.error-text');
    if (errorText) {
        errorText.textContent = message;
    }
}

function clearFormErrors(form) {
    form.querySelectorAll('.form-group').forEach(group => {
        group.classList.remove('has-error');
    });
    form.querySelectorAll('.error').forEach(field => {
        field.classList.remove('error');
    });
}

// ================================
// TASK 2: EDIT EXISTING MAPPING
// ================================

let originalMappingData = {};

async function openEditModal(mappingId) {
    const modal = document.getElementById('editModal');
    if (!modal) {
        console.error('Edit modal not found in DOM');
        return;
    }
    
    const form = document.getElementById('editMappingForm');
    if (!form) {
        console.error('Edit form not found in DOM');
        return;
    }
    
    // Show modal with loading indicator
    modal.classList.add('active');
    const submitBtn = form.querySelector('button[type="submit"]');
    submitBtn.disabled = true;
    submitBtn.classList.add('btn-loading');
    
    try {
        // Fetch mapping data
        const response = await fetch(`${API_BASE}/mappings/${mappingId}`);
        if (!response.ok) {
            throw new Error('Failed to load mapping data');
        }
        
        const data = await response.json();
        originalMappingData = { ...data };
        
        // Populate dropdowns first
        populateDropdown('edit_region', referenceData.regions);
        populateDropdown('edit_market_group', referenceData.marketGroups);
        populateDropdown('edit_channel_level', referenceData.channelLevels);
        
        // Populate form with data
        populateEditForm(data);
        
        // Enable submit button
        submitBtn.disabled = false;
        submitBtn.classList.remove('btn-loading');
        
    } catch (error) {
        console.error('Failed to load mapping:', error);
        showToast('error', 'Failed to load mapping', error.message);
        submitBtn.disabled = false;
        submitBtn.classList.remove('btn-loading');
        closeModal();
    }
}

function populateEditForm(data) {
    const form = document.getElementById('editMappingForm');
    if (!form) return;
    
    // Set mapping ID
    form.dataset.mappingId = data.id;
    
    // Populate fields
    setFieldValue(form, 'edit_customer_code', data.customer_code);
    setFieldValue(form, 'edit_customer_name', data.customer_name);
    setFieldValue(form, 'edit_sales_employee', data.sales_employee);
    setFieldValue(form, 'edit_entity', data.entity);
    setFieldValue(form, 'edit_region', data.region);
    setFieldValue(form, 'edit_sub_region', data.sub_region);
    setFieldValue(form, 'edit_market_group', data.market_group);
    setFieldValue(form, 'edit_channel_level', data.channel_level);
    setFieldValue(form, 'edit_company_group', data.company_group);
    
    // Clear changes summary
    const changesSummary = document.getElementById('changesSummary');
    if (changesSummary) {
        changesSummary.style.display = 'none';
    }
    
    // Add change listeners
    form.querySelectorAll('input, select').forEach(field => {
        field.addEventListener('change', () => detectChanges(form));
    });
}

function setFieldValue(form, fieldName, value) {
    const field = form.elements[fieldName];
    if (!field) return;
    
    field.value = value || '';
}

function detectChanges(form) {
    const changes = [];
    const criticalFields = ['region', 'market_group', 'channel_level'];
    let hasCriticalChange = false;
    
    form.querySelectorAll('input, select').forEach(field => {
        const fieldName = field.name.replace('edit_', '');
        const currentValue = field.value;
        const originalValue = originalMappingData[fieldName] || '';
        
        if (currentValue !== originalValue) {
            const formGroup = field.closest('.form-group');
            if (formGroup) {
                formGroup.classList.add('field-changed');
            }
            
            changes.push({
                field: fieldName,
                oldValue: originalValue,
                newValue: currentValue,
                isCritical: criticalFields.includes(fieldName)
            });
            
            if (criticalFields.includes(fieldName)) {
                hasCriticalChange = true;
            }
        } else {
            const formGroup = field.closest('.form-group');
            if (formGroup) {
                formGroup.classList.remove('field-changed');
            }
        }
    });
    
    // Update changes summary
    const changesSummary = document.getElementById('changesSummary');
    const changesList = document.getElementById('changesList');
    
    if (changes.length > 0) {
        changesSummary.style.display = 'block';
        changesList.innerHTML = changes.map(change => `
            <div class="change-item">
                <strong>${formatFieldName(change.field)}:</strong>
                <span class="old-value">${change.oldValue || '(empty)'}</span>
                →
                <span class="new-value">${change.newValue || '(empty)'}</span>
                ${change.isCritical ? '<span style="color: #ff9800;">⚠️</span>' : ''}
            </div>
        `).join('');
    } else {
        changesSummary.style.display = 'none';
    }
    
    return { changes, hasCriticalChange };
}

function formatFieldName(fieldName) {
    return fieldName
        .split('_')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}

async function submitEditMapping(event) {
    event.preventDefault();
    
    const form = event.target;
    const mappingId = form.dataset.mappingId;
    const submitBtn = form.querySelector('button[type="submit"]');
    
    // Detect changes
    const { changes, hasCriticalChange } = detectChanges(form);
    
    if (changes.length === 0) {
        showToast('info', 'No changes to save');
        return;
    }
    
    // Confirm critical changes
    if (hasCriticalChange) {
        const confirmed = await confirmCriticalChanges(changes);
        if (!confirmed) {
            return;
        }
    }
    
    // Show loading state
    submitBtn.disabled = true;
    submitBtn.classList.add('btn-loading');
    const originalText = submitBtn.textContent;
    submitBtn.textContent = '';
    
    try {
        const formData = new FormData(form);
        
        // Remove 'edit_' prefix from field names
        const cleanFormData = new FormData();
        for (const [key, value] of formData.entries()) {
            const cleanKey = key.replace('edit_', '');
            cleanFormData.append(cleanKey, value);
        }
        
        // Disabled fields are excluded from FormData - add them manually
        const entityField = document.getElementById('edit_entity');
        if (entityField && !cleanFormData.get('entity')) {
            cleanFormData.append('entity', entityField.value);
        }
        
        const response = await fetch(`${API_BASE}/mappings/${mappingId}`, {
            method: 'PUT',
            body: cleanFormData
        });
        
        const result = await response.json();
        
        if (response.ok && result.success) {
            showToast('success', 'Mapping updated successfully');
            closeModal();
            
            // Refresh the table
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            // Extract readable error message from FastAPI validation errors or plain strings
            let errorMsg = 'Failed to update mapping';
            if (result.detail) {
                if (Array.isArray(result.detail)) {
                    errorMsg = result.detail.map(e => `${e.loc?.slice(-1)[0] || ''}: ${e.msg}`).join(', ');
                } else {
                    errorMsg = result.detail;
                }
            }
            throw new Error(errorMsg);
        }
    } catch (error) {
        console.error('Update mapping failed:', error);
        showToast('error', 'Failed to update mapping', error.message);
    } finally {
        submitBtn.disabled = false;
        submitBtn.classList.remove('btn-loading');
        submitBtn.textContent = originalText;
    }
}

async function confirmCriticalChanges(changes) {
    const criticalChanges = changes.filter(c => c.isCritical);
    const message = `You are changing critical fields that will affect report categorization:\n\n${
        criticalChanges.map(c => `• ${formatFieldName(c.field)}: ${c.oldValue} → ${c.newValue}`).join('\n')
    }\n\nAre you sure you want to continue?`;
    
    return confirm(message);
}

// ================================
// TASK 3: DELETE MAPPING
// ================================

async function confirmDeleteMapping(mappingId, entityName) {
    // Use native confirm for now (can be enhanced with custom modal later)
    const confirmed = confirm(
        `Delete mapping for "${entityName}"?\n\n` +
        `⚠️ Warning: This entity will become unmapped in future reports.\n\n` +
        `This action can be undone by re-activating the mapping.`
    );
    
    if (!confirmed) {
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/mappings/${mappingId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok && result.success) {
            showToast('success', 'Mapping deleted successfully');
            
            // Animate row removal
            const row = document.querySelector(`tr[data-mapping-id="${mappingId}"]`);
            if (row) {
                row.classList.add('row-slide-out');
                setTimeout(() => {
                    row.remove();
                }, 400);
            }
            
            // Refresh page after animation
            setTimeout(() => {
                window.location.reload();
            }, 1000);
        } else {
            throw new Error(result.detail || 'Failed to delete mapping');
        }
    } catch (error) {
        console.error('Delete mapping failed:', error);
        showToast('error', 'Failed to delete mapping', error.message);
    }
}

// ================================
// TOAST NOTIFICATIONS
// ================================

function showToast(type, title, message = '') {
    const container = getOrCreateToastContainer();
    
    const icons = {
        success: '✓',
        error: '✕',
        warning: '⚠',
        info: 'ℹ'
    };
    
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `
        <div class="toast-icon">${icons[type] || 'ℹ'}</div>
        <div class="toast-content">
            <div class="toast-title">${title}</div>
            ${message ? `<div class="toast-message">${message}</div>` : ''}
        </div>
        <button class="toast-close" onclick="this.parentElement.remove()">×</button>
    `;
    
    container.appendChild(toast);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        toast.style.animation = 'slideInRight 0.3s ease reverse';
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

function getOrCreateToastContainer() {
    let container = document.querySelector('.toast-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'toast-container';
        document.body.appendChild(container);
    }
    return container;
}

// ================================
// INACTIVE MAPPINGS TOGGLE
// ================================

function toggleInactiveMappings() {
    const showInactive = document.getElementById('showInactive').checked;
    const url = new URL(window.location);
    
    if (showInactive) {
        url.searchParams.set('show_inactive', 'true');
    } else {
        url.searchParams.delete('show_inactive');
    }
    
    window.location = url.toString();
}

// ================================
// GLOBAL EXPORTS
// ================================

window.adminMappings = {
    openCreateModal,
    openEditModal,
    closeModal,
    submitCreateMapping,
    submitEditMapping,
    confirmDeleteMapping,
    toggleInactiveMappings,
    showToast
};
