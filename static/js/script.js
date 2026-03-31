// ============================================================
//  PENSIONGOV – DUAL PORTAL JAVASCRIPT
//  Admin Portal + Pensioner Portal
// ============================================================

// ── DATE DISPLAY ─────────────────────────────────────────────
(function () {
  const el = document.getElementById('current-date');
  if (el) {
    el.textContent = new Date().toLocaleDateString('en-IN', {
      weekday: 'short', year: 'numeric', month: 'short', day: 'numeric'
    });
  }
})();

// ── SIDEBAR TOGGLE ────────────────────────────────────────────
function toggleSidebar() {
  const sb = document.getElementById('sidebar');
  if (sb) sb.classList.toggle('open');
}
document.addEventListener('click', function (e) {
  const sb  = document.getElementById('sidebar');
  const btn = document.querySelector('.sidebar-toggle');
  if (sb && sb.classList.contains('open') && btn &&
      !sb.contains(e.target) && !btn.contains(e.target)) {
    sb.classList.remove('open');
  }
});

// ── NOTIFICATION BELL COUNT ───────────────────────────────────
function fetchUnreadCount() {
  fetch('/api/unread_count')
    .then(r => r.json())
    .then(data => {
      const count        = data.count || 0;
      const badge        = document.getElementById('notif-count');
      const sidebarBadge = document.getElementById('notif-badge-sidebar');
      if (badge) {
        badge.textContent   = count > 99 ? '99+' : count;
        badge.style.display = count > 0 ? 'flex' : 'none';
      }
      if (sidebarBadge) {
        sidebarBadge.textContent   = count;
        sidebarBadge.style.display = count > 0 ? 'inline-block' : 'none';
      }
    })
    .catch(() => {});
}
fetchUnreadCount();
setInterval(fetchUnreadCount, 30000);

// ── FLASH TOAST AUTO-DISMISS ──────────────────────────────────
document.addEventListener('DOMContentLoaded', function () {
  document.querySelectorAll('.flash-toast').forEach(function (toast) {
    setTimeout(function () {
      toast.style.transition = 'opacity .5s, transform .5s';
      toast.style.opacity    = '0';
      toast.style.transform  = 'translateX(40px)';
      setTimeout(() => toast.remove(), 500);
    }, 4500);
  });
});

// ── PAYMENT FORM: AUTO-FILL AMOUNT FROM DROPDOWN ──────────────
document.addEventListener('DOMContentLoaded', function () {
  const sel = document.querySelector('select[name="pensioner_id"]');
  const amt = document.querySelector('input[name="amount"]');
  if (sel && amt) {
    sel.addEventListener('change', function () {
      const text  = this.options[this.selectedIndex].textContent;
      const match = text.match(/₹([\d,]+)\/mo/);
      if (match) amt.value = match[1].replace(/,/g, '');
    });
  }

  // Set today's date / current month / year in payment form
  const monthSel = document.querySelector('select[name="payment_month"]');
  const yearInp  = document.querySelector('input[name="payment_year"]');
  const dateInp  = document.querySelector('input[name="payment_date"]');
  const months   = ['January','February','March','April','May','June',
                    'July','August','September','October','November','December'];
  const now      = new Date();
  if (monthSel) monthSel.value = months[now.getMonth()];
  if (yearInp  && !yearInp.value)  yearInp.value  = now.getFullYear();
  if (dateInp  && !dateInp.value)  dateInp.value  = now.toISOString().split('T')[0];
});

// ── REGISTRATION: AGE AUTO-CALC ───────────────────────────────
function calcAge() {
  const dobEl = document.getElementById('dob');
  const ageEl = document.getElementById('age_field');
  if (!dobEl || !ageEl) return;
  const dob = new Date(dobEl.value);
  const today = new Date();
  let age = today.getFullYear() - dob.getFullYear();
  const m = today.getMonth() - dob.getMonth();
  if (m < 0 || (m === 0 && today.getDate() < dob.getDate())) age--;
  ageEl.value = isNaN(age) ? '' : age;
}

// ── PENSION TYPE CARD SELECTION ───────────────────────────────
document.addEventListener('DOMContentLoaded', function () {
  const cards = document.querySelectorAll('.pension-type-card input[type="radio"]');
  cards.forEach(function (radio) {
    function updateSelected() {
      document.querySelectorAll('.pension-type-card').forEach(c => c.classList.remove('selected'));
      const checked = document.querySelector('.pension-type-card input:checked');
      if (checked) checked.closest('.pension-type-card').classList.add('selected');
    }
    radio.addEventListener('change', updateSelected);
    if (radio.checked) radio.closest('.pension-type-card').classList.add('selected');
  });
});

// ── PASSWORD TOGGLE ───────────────────────────────────────────
function togglePw(inputId, eyeId) {
  const input = document.getElementById(inputId);
  const eye   = document.getElementById(eyeId);
  if (!input) return;
  input.type = input.type === 'password' ? 'text' : 'password';
  if (eye) eye.className = input.type === 'password' ? 'bi bi-eye' : 'bi bi-eye-slash';
}

// ── QUICK STATUS UPDATE (pensioner view) ─────────────────────
function updateStatus(pid, status) {
  if (!confirm(`Change status to "${status}"?`)) return;
  fetch(`/api/status_update/${pid}`, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify({ status })
  })
    .then(r => r.json())
    .then(d => {
      if (d.success) location.reload();
      else alert('Error updating status');
    })
    .catch(() => alert('Network error'));
}

// ── FRAUD RERUN BUTTON ANIMATION ─────────────────────────────
document.addEventListener('DOMContentLoaded', function () {
  const form = document.querySelector('form[action*="rerun"]');
  if (form) {
    form.addEventListener('submit', function () {
      const btn = form.querySelector('button');
      if (btn) {
        btn.innerHTML = '<i class="bi bi-arrow-repeat spin-icon"></i> Running…';
        btn.disabled  = true;
      }
    });
  }
});

// ── PRINT ─────────────────────────────────────────────────────
function printReport() { window.print(); }

// ── SPIN KEYFRAME (injected once) ─────────────────────────────
(function () {
  const s = document.createElement('style');
  s.textContent = `
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
    .spin-icon { display: inline-block; animation: spin .8s linear infinite; }
    .sidebar.open { transform: translateX(0) !important; }
  `;
  document.head.appendChild(s);
})();

// ── CONFIRM HELPERS ───────────────────────────────────────────
function confirmDelete(name) {
  return confirm(`Delete "${name}"?\nThis action cannot be undone.`);
}
function confirmApprove(name) {
  return confirm(`Approve pension application for ${name}?`);
}

// ── GRANT ACCESS MODAL INLINE LOGIC ──────────────────────────
function showGrantModal(pid) {
  const pw = prompt('Set temporary password for pensioner portal (min 6 chars):', 'pensioner123');
  if (pw && pw.length >= 6) {
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = `/pensioners/grant_access/${pid}`;
    const inp = document.createElement('input');
    inp.type  = 'hidden';
    inp.name  = 'temp_password';
    inp.value = pw;
    form.appendChild(inp);
    document.body.appendChild(form);
    form.submit();
  } else if (pw !== null) {
    alert('Password must be at least 6 characters.');
  }
}