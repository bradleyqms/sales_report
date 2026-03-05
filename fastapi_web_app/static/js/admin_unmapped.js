/**
 * Admin Unmapped Entities - JavaScript
 * Part of DNR-57/DNR-58: Unmapped Entity Resolution
 */

// ================================
// STATE & CONFIGURATION
// ================================

const API_BASE = '/admin/api';
let currentUnmappedItem = null;
let referenceData = {
    regions: [],
    marketGroups: [],
    channelLevels: []
};

// ================================
// INITIALIZATION
// ================================

document.addEventListener('DOMContentLoaded', async () => {
    console.log('Admin Unmapped UI initialized');
    
    // Load reference data
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
    // Drawer close handlers
    document.addEventListener('click', (e) => {
        if (e.target.classList.contains('drawer-overlay')) {
            closeDrawer();
        }
    });
    
    // ESC key to close drawer
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeDrawer();
            closeModal();
        }
    });
    

}

// ================================
// CREATE NEW MAPPING FROM UNMAPPED
// ================================

async function openCreateFromUnmappedModal(unmappedId) {
    const modal = document.getElementById('createFromUnmappedModal');
    if (!modal) {
        console.error('Create from unmapped modal not found');
        return;
    }
    
    const form = document.getElementById('createFromUnmappedForm');
    if (!form) {
        console.error('Create from unmapped form not found');
        return;
    }
    
    // Show modal and disable submit button during load
    modal.classList.add('active');
    const submitBtn = form.querySelector('button[type="submit"]');
    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.classList.add('btn-loading');
    }
    
    try {
        // Fetch unmapped entity details
        const response = await fetch(`${API_BASE}/unmapped/${unmappedId}`);
        if (!response.ok) {
            throw new Error('Failed to load unmapped entity');
        }
        
        currentUnmappedItem = await response.json();
        
        // Populate dropdowns first
        populateDropdown('cfu_region', referenceData.regions);
        populateDropdown('cfu_market_group', referenceData.marketGroups);
        populateDropdown('cfu_channel_level', referenceData.channelLevels);
        
        // Populate form with unmapped data
        populateCreateFromUnmappedForm(currentUnmappedItem);
        
        // Apply smart defaults
        await applySmartDefaults(currentUnmappedItem);
        
        // Enable submit button
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.classList.remove('btn-loading');
        }
        
    } catch (error) {
        console.error('Failed to load unmapped entity:', error);
        showToast('error', 'Failed to load entity data', error.message);
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.classList.remove('btn-loading');
        }
        closeModal();
    }
}

function populateCreateFromUnmappedForm(item) {
    const form = document.getElementById('createFromUnmappedForm');
    if (!form) return;
    
    // Set unmapped ID
    form.dataset.unmappedId = item.id;
    
    // Determine entity type
    const entityType = item.customer_name ? 'Customer' : 'Employee';
    
    // Populate read-only fields
    document.getElementById('cfu_entity_type_display').textContent = entityType;
    document.getElementById('cfu_entity').value = entityType;
    
    if (item.customer_name) {
        document.getElementById('cfu_customer_name').value = item.customer_name;
        document.getElementById('cfu_customer_code').value = item.customer_code || '';
        document.getElementById('cfuCustomerFields').style.display = 'block';
        document.getElementById('cfuEmployeeFields').style.display = 'none';
    } else if (item.sales_employee) {
        document.getElementById('cfu_sales_employee').value = item.sales_employee;
        document.getElementById('cfuCustomerFields').style.display = 'none';
        document.getElementById('cfuEmployeeFields').style.display = 'block';
    }
    
    // Show preview
    const previewText = document.getElementById('resolutionPreview');
    if (previewText) {
        const arValue = formatNumber(item.total_ar_value_keur || 0);
        previewText.textContent = `This will resolve ${item.count || 0} occurrences totaling €${arValue}`;
    }
}

async function applySmartDefaults(item) {
    // Apply smart defaults based on similar entities
    try {
        // For now, apply most common values
        // TODO: Could be enhanced with backend endpoint for smart suggestions
        
        if (referenceData.regions.length > 0) {
            // Default to most common region (could be enhanced)
            const defaultRegion = referenceData.regions[0];
            document.getElementById('cfu_region').value = defaultRegion;
        }
        
        if (referenceData.marketGroups.length > 0) {
            // Default market group
            const defaultMarket = referenceData.marketGroups[0];
            document.getElementById('cfu_market_group').value = defaultMarket;
        }
        
        if (referenceData.channelLevels.length > 0) {
            // Default channel level - prefer Retail for customers, Direct for employees
            const entityType = item.customer_name ? 'Customer' : 'Employee';
            let defaultChannel = referenceData.channelLevels[0];
            
            if (entityType === 'Customer' && referenceData.channelLevels.includes('Retail')) {
                defaultChannel = 'Retail';
            } else if (entityType === 'Employee' && referenceData.channelLevels.includes('Direct')) {
                defaultChannel = 'Direct';
            }
            
            document.getElementById('cfu_channel_level').value = defaultChannel;
        }
        
        console.log('Smart defaults applied');
    } catch (error) {
        console.error('Failed to apply smart defaults:', error);
    }
}

async function submitCreateFromUnmapped(event) {
    event.preventDefault();
    
    const form = event.target;
    const unmappedId = form.dataset.unmappedId;
    const submitBtn = form.querySelector('button[type="submit"]');
    
    if (!unmappedId) {
        showToast('error', 'Invalid unmapped entity');
        return;
    }
    
    // Show loading
    submitBtn.disabled = true;
    submitBtn.classList.add('btn-loading');
    const originalText = submitBtn.textContent;
    submitBtn.textContent = '';
    
    try {
        const formData = new FormData(form);
        
        // Remove 'cfu_' prefix from field names
        const cleanFormData = new FormData();
        for (const [key, value] of formData.entries()) {
            const cleanKey = key.replace('cfu_', '');
            cleanFormData.append(cleanKey, value);
        }
        
        const response = await fetch(`${API_BASE}/unmapped/${unmappedId}/create-and-resolve`, {
            method: 'POST',
            body: cleanFormData
        });
        
        const result = await response.json();
        
        if (response.ok && result.success) {
            // Success with celebration!
            showToast('success', '🎉 Mapping created & resolved!', 'New entity mapping has been created');
            
            // Animate row with celebration
            const row = document.querySelector(`tr[data-unmapped-id="${unmappedId}"]`);
            if (row) {
                row.classList.add('row-celebration');
                setTimeout(() => {
                    row.classList.add('row-slide-out');
                    setTimeout(() => row.remove(), 400);
                }, 600);
            }
            
            closeModal();
            
            // Refresh page
            setTimeout(() => {
                window.location.reload();
            }, 2000);
        } else {
            throw new Error(result.detail || 'Failed to create mapping');
        }
    } catch (error) {
        console.error('Create and resolve failed:', error);
        showToast('error', 'Failed to create mapping', error.message);
    } finally {
        submitBtn.disabled = false;
        submitBtn.classList.remove('btn-loading');
        submitBtn.textContent = originalText;
    }
}

// ================================
// UTILITY FUNCTIONS
// ================================

function closeDrawer() {
    const drawers = document.querySelectorAll('.drawer-overlay');
    drawers.forEach(drawer => {
        drawer.classList.remove('active');
    });
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

function formatNumber(num) {
    return new Intl.NumberFormat('de-DE', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(num);
}

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
// GLOBAL EXPORTS
// ================================

window.adminUnmapped = {
    openCreateFromUnmappedModal,
    closeDrawer,
    closeModal,
    submitCreateFromUnmapped,
    showToast
};
