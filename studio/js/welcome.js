let welcomeDontShow = localStorage.getItem('welcomeDontShow') === 'true';

// Open welcome modal (called from topbar Tips button)
function openWelcomeModal() {
    const modal = document.getElementById('modal-welcome');
    if (modal) {
        modal.style.display = 'flex';
        // Reset to first tab
        document.querySelectorAll('.welcome-tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.welcome-pane').forEach(p => p.style.display = 'none');
        document.querySelector('.welcome-tab').classList.add('active');
        document.getElementById('welcome-features').style.display = 'block';
    }
}

// Close welcome modal
function closeWelcomeModal() {
    document.getElementById('modal-welcome').style.display = 'none';
}

// Switch welcome tabs
function switchWelcomeTab(btn, paneId) {
    // Update tab buttons
    document.querySelectorAll('.welcome-tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');

    // Update panes
    document.querySelectorAll('.welcome-pane').forEach(p => p.style.display = 'none');
    document.getElementById(paneId).style.display = 'block';
}

// Set "don't show again" preference
function setWelcomeDontShow(checked) {
    welcomeDontShow = checked;
    localStorage.setItem('welcomeDontShow', checked);
}

// Show welcome modal on first visit (call from init)
function showWelcomeOnFirstVisit() {
    if (!welcomeDontShow) {
        // Check if first visit (no connection made yet)
        const hasVisited = localStorage.getItem('hasVisitedStudio');
        if (!hasVisited) {
            setTimeout(() => {
                openWelcomeModal();
                localStorage.setItem('hasVisitedStudio', 'true');
            }, 500); // Small delay to let UI render
        }
    }
}

// Add to window for global access
window.openWelcomeModal = openWelcomeModal;
window.closeWelcomeModal = closeWelcomeModal;
window.switchWelcomeTab = switchWelcomeTab;
window.setWelcomeDontShow = setWelcomeDontShow;
window.showWelcomeOnFirstVisit = showWelcomeOnFirstVisit;