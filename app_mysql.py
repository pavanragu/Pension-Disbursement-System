from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
from flask_mail import Mail, Message
from werkzeug.security import generate_password_hash, check_password_hash
import mysql.connector, os, json, random
from datetime import datetime, date, timedelta
from functools import wraps
import threading

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'pension_gov_tn_secret_xK9mP')

# ══════════════════════════════════════════════════════════════
#  MYSQL CONFIG — reads from environment variables on server
#  or falls back to local values for development
# ══════════════════════════════════════════════════════════════
DB_CONFIG = {
    'host':     os.environ.get('DB_HOST',     'localhost'),
    'user':     os.environ.get('DB_USER',     'pension_admin'),
    'password': os.environ.get('DB_PASSWORD', 'Admin@2026'),
    'database': os.environ.get('DB_NAME',     'pension_system'),
    'charset':  'utf8mb4'
}

# ══════════════════════════════════════════════════════════════
#  EMAIL CONFIG — Gmail SMTP
# ══════════════════════════════════════════════════════════════
MAIL_SENDER_EMAIL = os.environ.get('MAIL_EMAIL', 'rvpavan06@gmail.com')

app.config['MAIL_SERVER']         = 'smtp.gmail.com'
app.config['MAIL_PORT']           = 587
app.config['MAIL_USE_TLS']        = True
app.config['MAIL_USERNAME']       = MAIL_SENDER_EMAIL
app.config['MAIL_PASSWORD']       = os.environ.get('MAIL_PASSWORD', 'trgjsdffoimzwaih')
app.config['MAIL_DEFAULT_SENDER'] = ('PensionGov Tamil Nadu', MAIL_SENDER_EMAIL)
mail = Mail(app)

@app.context_processor
def inject_now():
    return {'now': datetime.now(), 'now_year': datetime.now().year}

# ══════════════════════════════════════════════════════════════
#  DATABASE
# ══════════════════════════════════════════════════════════════
def get_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    return conn

def fix_dates(row):
    """Convert MySQL datetime/date objects to strings so templates work correctly."""
    if row is None:
        return None
    d = dict(row)
    for key, val in d.items():
        if hasattr(val, 'strftime'):
            if hasattr(val, 'hour'):
                d[key] = val.strftime('%Y-%m-%d %H:%M:%S')
            else:
                d[key] = val.strftime('%Y-%m-%d')
    return d

def fix_dates_list(rows):
    """Apply fix_dates to a list of rows."""
    return [fix_dates(r) for r in (rows or [])]

def qone(cur):
    """fetchone() with date conversion."""
    return fix_dates(cur.fetchone())

def qall(cur):
    """fetchall() with date conversion."""
    return fix_dates_list(cur.fetchall())

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        username   VARCHAR(100) UNIQUE NOT NULL,
        password   VARCHAR(255) NOT NULL,
        role       VARCHAR(20) DEFAULT 'admin',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    c.execute('''CREATE TABLE IF NOT EXISTS pensioners (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        pension_id     VARCHAR(30) UNIQUE,
        name           VARCHAR(150) NOT NULL,
        dob            DATE,
        age            INT,
        gender         VARCHAR(10),
        phone          VARCHAR(15),
        email          VARCHAR(150) UNIQUE,
        address        TEXT,
        aadhaar        VARCHAR(12),
        pension_type   VARCHAR(30) DEFAULT 'Old Age',
        status         VARCHAR(30) DEFAULT 'Pending Verification',
        monthly_amount DECIMAL(10,2) DEFAULT 0,
        created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    c.execute('''CREATE TABLE IF NOT EXISTS bank_details (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        pensioner_id   INT UNIQUE NOT NULL,
        bank_name      VARCHAR(100),
        account_number VARCHAR(30),
        ifsc_code      VARCHAR(15),
        branch         VARCHAR(100),
        account_type   VARCHAR(20) DEFAULT 'Savings',
        created_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (pensioner_id) REFERENCES pensioners(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    c.execute('''CREATE TABLE IF NOT EXISTS pensioner_accounts (
        id           INT AUTO_INCREMENT PRIMARY KEY,
        pensioner_id INT UNIQUE,
        email        VARCHAR(150) UNIQUE NOT NULL,
        password     VARCHAR(255) NOT NULL,
        is_active    TINYINT(1) DEFAULT 1,
        created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (pensioner_id) REFERENCES pensioners(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    c.execute('''CREATE TABLE IF NOT EXISTS payments (
        id            INT AUTO_INCREMENT PRIMARY KEY,
        pensioner_id  INT NOT NULL,
        amount        DECIMAL(10,2) NOT NULL,
        payment_date  DATE,
        payment_month VARCHAR(15),
        payment_year  INT,
        method        VARCHAR(30) DEFAULT 'Bank Transfer',
        reference     VARCHAR(50),
        notes         TEXT,
        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (pensioner_id) REFERENCES pensioners(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    c.execute('''CREATE TABLE IF NOT EXISTS notifications (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        recipient_type VARCHAR(20) NOT NULL,
        pensioner_id   INT,
        title          VARCHAR(200) NOT NULL,
        message        TEXT NOT NULL,
        is_read        TINYINT(1) DEFAULT 0,
        created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    c.execute('''CREATE TABLE IF NOT EXISTS fraud_alerts (
        id           INT AUTO_INCREMENT PRIMARY KEY,
        pensioner_id INT,
        alert_type   VARCHAR(50) NOT NULL,
        description  TEXT NOT NULL,
        is_resolved  TINYINT(1) DEFAULT 0,
        created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (pensioner_id) REFERENCES pensioners(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    c.execute('''CREATE TABLE IF NOT EXISTS profile_update_requests (
        id           INT AUTO_INCREMENT PRIMARY KEY,
        pensioner_id INT NOT NULL,
        field_name   VARCHAR(50) NOT NULL,
        old_value    TEXT,
        new_value    TEXT NOT NULL,
        status       VARCHAR(20) DEFAULT 'Pending Review',
        created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (pensioner_id) REFERENCES pensioners(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')

    conn.commit()

    # Seed admin
    c.execute("SELECT id FROM users WHERE username='admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users (username,password,role) VALUES (%s,%s,%s)",
                  ('admin', generate_password_hash('admin123'), 'admin'))

    # Seed demo data
    c.execute("SELECT COUNT(*) as cnt FROM pensioners")
    if c.fetchone()[0] == 0:
        yr  = datetime.now().year
        cy  = datetime.now().year
        cm  = datetime.now().month
        months_en = ['January','February','March','April','May','June',
                     'July','August','September','October','November','December']

        def prev_month(offset):
            import calendar
            m = cm - offset
            y = cy
            while m < 1:
                m += 12
                y -= 1
            day = min(10, calendar.monthrange(y, m)[1])
            return (f"{y}-{m:02d}-{day:02d}", months_en[m-1], y)

        def calc_age(dob_str):
            try:
                bd    = datetime.strptime(dob_str, '%Y-%m-%d').date()
                today = date.today()
                return today.year - bd.year - ((today.month, today.day) < (bd.month, bd.day))
            except:
                return 0

        demo_pensioners = [
            (f'PNS-{yr}-001','Rajaram Iyer','1956-03-12',calc_age('1956-03-12'),'Male','9841234567',
             'rajaram@email.com','12, Anna Nagar, Chennai','123456789012','Old Age','Approved',8500),
            (f'PNS-{yr}-002','Meenakshi Sundaram','1952-07-25',calc_age('1952-07-25'),'Female','9842345678',
             'meenakshi@email.com','45, T Nagar, Chennai','234567890123','Old Age','Disbursed',9000),
            (f'PNS-{yr}-003','Velmurugan K','1959-11-08',calc_age('1959-11-08'),'Male','9843456789',
             'velmu@email.com','78, Guindy, Chennai','345678901234','Disability','Pending Verification',7500),
            (f'PNS-{yr}-004','Lakshmi Devi','1954-05-20',calc_age('1954-05-20'),'Female','9844567890',
             'lakshmi@email.com','23, Mylapore, Chennai','456789012345','Widow','Under Verification',6000),
            (f'PNS-{yr}-005','Subramaniam P','1949-01-15',calc_age('1949-01-15'),'Male','9845678901',
             'subbu@email.com','56, Adyar, Chennai','567890123456','Old Age','Approved',8000),
            (f'PNS-{yr}-006','Kamalam R','1893-09-30',calc_age('1893-09-30'),'Female','9846789012',
             'kamalam@email.com','89, Vadapalani, Chennai','678901234567','Old Age','Pending Verification',7000),
        ]
        for d in demo_pensioners:
            c.execute('''INSERT INTO pensioners
                (pension_id,name,dob,age,gender,phone,email,address,aadhaar,pension_type,status,monthly_amount)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', d)

        demo_banks = [
            (1,'State Bank of India','40012345678','SBIN0001234','Anna Nagar Branch','Savings'),
            (2,'Indian Overseas Bank','50023456789','IOBA0002345','T Nagar Branch','Savings'),
            (3,'Canara Bank','60034567890','CNRB0003456','Guindy Branch','Savings'),
            (4,'Bank of Baroda','70045678901','BARB0004567','Mylapore Branch','Savings'),
            (5,'State Bank of India','40012345678','SBIN0001234','Adyar Branch','Savings'),
            (6,'Indian Overseas Bank','80056789012','IOBA0005678','Vadapalani Branch','Savings'),
        ]
        for b in demo_banks:
            c.execute('''INSERT INTO bank_details
                (pensioner_id,bank_name,account_number,ifsc_code,branch,account_type)
                VALUES (%s,%s,%s,%s,%s,%s)''', b)

        demo_payments = [
            (1, 8500, *prev_month(2), 'Bank Transfer', 'REF001'),
            (1, 8500, *prev_month(1), 'Bank Transfer', 'REF002'),
            (1, 8500, *prev_month(0), 'Bank Transfer', 'REF003'),
            (2, 9000, *prev_month(2), 'NEFT',          'REF004'),
            (2, 9000, *prev_month(1), 'NEFT',          'REF005'),
            (2, 9000, *prev_month(0), 'NEFT',          'REF006'),
            (5, 8000, *prev_month(1), 'Bank Transfer', 'REF007'),
            (5, 8000, *prev_month(0), 'Bank Transfer', 'REF008'),
        ]
        for p in demo_payments:
            c.execute('''INSERT INTO payments
                (pensioner_id,amount,payment_date,payment_month,payment_year,method,reference)
                VALUES (%s,%s,%s,%s,%s,%s,%s)''', p)

        for pid, email in [(1,'rajaram@email.com'),(2,'meenakshi@email.com'),(5,'subbu@email.com')]:
            c.execute('''INSERT IGNORE INTO pensioner_accounts (pensioner_id,email,password)
                VALUES (%s,%s,%s)''', (pid, email, generate_password_hash('pensioner123')))

    conn.commit()
    conn.close()
    run_fraud_detection()

# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════
def gen_pension_id():
    return f"PNS-{datetime.now().year}-{random.randint(100,999):03d}"

# ── Send email in background thread ──────────────────────────
def send_email_async(to_email, subject, body):
    """
    Sends email in a background thread so the page
    does not freeze while waiting for Gmail to respond.
    """
    def _send():
        try:
            with app.app_context():
                msg = Message(
                    subject    = subject,
                    recipients = [to_email],
                    body       = body
                )
                mail.send(msg)
                print(f"[EMAIL ✓] Sent to {to_email} — {subject}")
        except Exception as e:
            print(f"[EMAIL ✗] Failed to {to_email}: {e}")

    t = threading.Thread(target=_send)
    t.daemon = True
    t.start()

# ── Get pensioner's email from DB ────────────────────────────
def get_pensioner_email(pensioner_id):
    """Returns the email address of a pensioner by their ID."""
    if not pensioner_id:
        return None
    try:
        conn = get_db()
        cur  = conn.cursor(dictionary=True)
        cur.execute("SELECT email, name FROM pensioners WHERE id=%s", (pensioner_id,))
        row  = cur.fetchone()
        conn.close()
        if row:
            return row.get('email'), row.get('name')
    except:
        pass
    return None, None

# ── Main notification function ────────────────────────────────
def add_notification(recipient_type, title, message, pensioner_id=None):
    """
    1. Saves notification to the DB (portal notification — always)
    2. Sends a real-time email to the pensioner or admin

    recipient_type : 'admin' or 'pensioner'
    title          : short title e.g. 'Payment Credited'
    message        : full message text
    pensioner_id   : DB id of pensioner (None for admin-only notifications)
    """

    # ── Step 1: Save to database (portal notification) ────────
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        '''INSERT INTO notifications (recipient_type, pensioner_id, title, message)
           VALUES (%s, %s, %s, %s)''',
        (recipient_type, pensioner_id, title, message)
    )
    conn.commit()
    conn.close()

    # ── Step 2: Send email to PENSIONER ───────────────────────
    if recipient_type == 'pensioner' and pensioner_id:
        email, name = get_pensioner_email(pensioner_id)
        if email:
            body = f"""Dear {name or 'Pensioner'},

{message}

─────────────────────────────────────────
PensionGov Portal — Government of Tamil Nadu
Department of Social Welfare
─────────────────────────────────────────
This is an automated email notification.
Please do not reply to this email.
To view your full notification history, login at:
http://127.0.0.1:5000/pensioner/login
"""
            send_email_async(
                to_email = email,
                subject  = f"PensionGov: {title}",
                body     = body
            )

    # ── Step 3: Send email to ADMIN ───────────────────────────
    if recipient_type == 'admin':
        admin_email = app.config.get('MAIL_USERNAME')
        if admin_email:
            body = f"""Admin Notification — PensionGov System

{title}

{message}

─────────────────────────────────────────
PensionGov Portal — Government of Tamil Nadu
Login at: http://127.0.0.1:5000/admin/login
"""
            send_email_async(
                to_email = admin_email,
                subject  = f"[PensionGov Admin] {title}",
                body     = body
            )

STATUS_MSG = {
    'Pending Verification': 'Your application has been received and is pending verification.',
    'Under Verification':   'Your pension application is currently under verification by our team.',
    'Approved':             'Congratulations! Your pension application has been approved.',
    'Disbursed':            'Your pension payment has been successfully disbursed to your account.',
    'Rejected':             'Your pension application has been rejected. Please contact the office.',
}

def run_fraud_detection():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("DELETE FROM fraud_alerts")
    cur.execute("SELECT * FROM pensioners")
    pensioners = qall(cur)
    bank_map, name_map, aadhaar_map = {}, {}, {}

    for p in pensioners:
        if p['age'] and p['age'] > 120:
            cur.execute("INSERT INTO fraud_alerts (pensioner_id,alert_type,description) VALUES (%s,%s,%s)",
                (p['id'],'Suspicious Age',
                 f"Pensioner {p['name']} (ID:{p['pension_id'] or p['id']}) has age {p['age']} > 120."))
        cur.execute("SELECT * FROM bank_details WHERE pensioner_id=%s", (p['id'],))
        bd = qone(cur)
        if bd and bd['account_number']:
            acc = bd['account_number']
            if acc in bank_map:
                cur.execute("INSERT INTO fraud_alerts (pensioner_id,alert_type,description) VALUES (%s,%s,%s)",
                    (p['id'],'Duplicate Bank Account',
                     f"Account {acc} shared by '{bank_map[acc]}' and '{p['name']}'."))
            else:
                bank_map[acc] = p['name']
        nm = (p['name'] or '').lower().strip()
        if nm in name_map:
            cur.execute("INSERT INTO fraud_alerts (pensioner_id,alert_type,description) VALUES (%s,%s,%s)",
                (p['id'],'Duplicate Name',
                 f"Multiple records for '{p['name']}' (IDs: {name_map[nm]} & {p['pension_id'] or p['id']})."))
        else:
            name_map[nm] = p['pension_id'] or str(p['id'])
        if p['aadhaar']:
            if p['aadhaar'] in aadhaar_map:
                cur.execute("INSERT INTO fraud_alerts (pensioner_id,alert_type,description) VALUES (%s,%s,%s)",
                    (p['id'],'Duplicate Aadhaar',
                     f"Aadhaar {p['aadhaar']} used by '{aadhaar_map[p['aadhaar']]}' and '{p['name']}'."))
            else:
                aadhaar_map[p['aadhaar']] = p['name']
    conn.commit()
    conn.close()

# ══════════════════════════════════════════════════════════════
#  AUTH DECORATORS
# ══════════════════════════════════════════════════════════════
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('role') != 'admin':
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated

def pensioner_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('role') != 'pensioner':
            flash('Please login to your pensioner account.', 'warning')
            return redirect(url_for('pensioner_login'))
        return f(*args, **kwargs)
    return decorated

# ══════════════════════════════════════════════════════════════
#  ROOT
# ══════════════════════════════════════════════════════════════
@app.route('/')
def index():
    if session.get('role') == 'admin':
        return redirect(url_for('dashboard'))
    if session.get('role') == 'pensioner':
        return redirect(url_for('pensioner_dashboard'))
    return render_template('index.html')

# ══════════════════════════════════════════════════════════════
#  ADMIN AUTH
# ══════════════════════════════════════════════════════════════
@app.route('/admin/login', methods=['GET','POST'])
def admin_login():
    if session.get('role') == 'admin':
        return redirect(url_for('dashboard'))
    error = None
    if request.method == 'POST':
        uname = request.form.get('username','').strip()
        pwd   = request.form.get('password','')
        conn  = get_db()
        cur   = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM users WHERE username=%s AND role='admin'", (uname,))
        user  = qone(cur)
        conn.close()
        if user and check_password_hash(user['password'], pwd):
            session.clear()
            session['user_id']  = user['id']
            session['username'] = user['username']
            session['role']     = 'admin'
            return redirect(url_for('dashboard'))
        error = 'Invalid credentials.'
    return render_template('admin_login.html', error=error)

@app.route('/admin/logout')
def admin_logout():
    session.clear()
    return redirect(url_for('admin_login'))

@app.route('/login', methods=['GET','POST'])
def login():
    return redirect(url_for('admin_login'))

@app.route('/logout')
def logout():
    role = session.get('role')
    session.clear()
    return redirect(url_for('pensioner_login') if role == 'pensioner' else url_for('admin_login'))

# ══════════════════════════════════════════════════════════════
#  ADMIN DASHBOARD
# ══════════════════════════════════════════════════════════════
@app.route('/dashboard')
@admin_required
def dashboard():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT COUNT(*) as cnt FROM pensioners");                                                         total_pensioners         = qone(cur)['cnt']
    cur.execute("SELECT COUNT(*) as cnt FROM pensioners WHERE status IN ('Pending Verification','Under Verification')"); pending_approvals   = qone(cur)['cnt']
    cur.execute("SELECT COUNT(*) as cnt FROM fraud_alerts WHERE is_resolved=0");                                   fraud_count              = qone(cur)['cnt']
    cur.execute("SELECT COALESCE(SUM(monthly_amount),0) as t FROM pensioners WHERE status IN ('Approved','Disbursed')"); total_monthly      = qone(cur)['t']
    cur.execute("SELECT COUNT(*) as cnt FROM notifications WHERE is_read=0 AND recipient_type='admin'");           unread_notifs            = qone(cur)['cnt']
    cur.execute("SELECT COUNT(*) as cnt FROM pensioners WHERE status='Pending Verification'");                     new_registrations        = qone(cur)['cnt']
    cur.execute("SELECT COUNT(*) as cnt FROM profile_update_requests WHERE status='Pending Review'");             pending_profile_requests  = qone(cur)['cnt']
    cur.execute('''SELECT fa.*, p.name, p.pension_id FROM fraud_alerts fa
        LEFT JOIN pensioners p ON fa.pensioner_id=p.id WHERE fa.is_resolved=0
        ORDER BY fa.created_at DESC LIMIT 5''');                                                                   recent_alerts   = fix_dates_list(qall(cur))
    cur.execute('''SELECT pay.*, p.name FROM payments pay
        JOIN pensioners p ON pay.pensioner_id=p.id ORDER BY pay.created_at DESC LIMIT 5''');                      recent_payments = fix_dates_list(qall(cur))
    cur.execute('''SELECT payment_month, payment_year, SUM(amount) as total FROM payments
        GROUP BY payment_year, payment_month ORDER BY payment_year DESC, payment_month DESC LIMIT 6''');           monthly_data    = fix_dates_list(qall(cur))
    cur.execute("SELECT status, COUNT(*) as cnt FROM pensioners GROUP BY status");                                 status_data     = fix_dates_list(qall(cur))
    cur.execute('''SELECT p.*, bd.bank_name FROM pensioners p
        LEFT JOIN bank_details bd ON bd.pensioner_id=p.id WHERE p.status='Pending Verification'
        ORDER BY p.created_at DESC LIMIT 5''');                                                                    recent_regs     = fix_dates_list(qall(cur))
    conn.close()

    # Safe chart data — handle empty payments table
    chart_labels  = json.dumps([f"{r['payment_month']} {r['payment_year']}" for r in reversed(list(monthly_data))])
    chart_values  = json.dumps([float(r['total']) for r in reversed(list(monthly_data))])
    status_labels = json.dumps([r['status'] for r in status_data])
    status_counts = json.dumps([r['cnt'] for r in status_data])

    return render_template('admin_dashboard.html',
        total_pensioners=total_pensioners, pending_approvals=pending_approvals,
        fraud_count=fraud_count, total_monthly=total_monthly,
        unread_notifs=unread_notifs, new_registrations=new_registrations,
        pending_profile_requests=pending_profile_requests,
        recent_alerts=recent_alerts, recent_payments=recent_payments, recent_regs=recent_regs,
        chart_labels=chart_labels, chart_values=chart_values,
        status_labels=status_labels, status_counts=status_counts)

# ══════════════════════════════════════════════════════════════
#  ADMIN – PENSIONER MANAGEMENT
# ══════════════════════════════════════════════════════════════
@app.route('/pensioners')
@admin_required
def pensioners():
    search = request.args.get('search','')
    sf     = request.args.get('status','')
    conn   = get_db()
    cur    = conn.cursor(dictionary=True)
    q      = "SELECT p.*, bd.bank_name FROM pensioners p LEFT JOIN bank_details bd ON bd.pensioner_id=p.id WHERE 1=1"
    params = []
    if search:
        q += " AND (p.name LIKE %s OR p.pension_id LIKE %s OR p.phone LIKE %s OR p.email LIKE %s OR p.aadhaar LIKE %s)"
        params += [f'%{search}%']*5
    if sf:
        q += " AND p.status=%s"
        params.append(sf)
    q += " ORDER BY p.created_at DESC"
    cur.execute(q, params)
    rows = qall(cur)
    conn.close()
    return render_template('pensioners.html', pensioners=rows, search=search, status_filter=sf)

@app.route('/pensioners/add', methods=['GET','POST'])
@admin_required
def add_pensioner():
    if request.method == 'POST':
        f    = request.form
        import re
        name  = f.get('name','').strip()
        email = f.get('email','').strip().lower()

        # Name validation
        if re.search(r'\d', name):
            flash('Name must contain alphabets only. Numbers are not allowed.', 'danger')
            return redirect(url_for('add_pensioner'))
        if re.search(r'[^A-Za-z\s]', name):
            flash('Name must contain alphabets and spaces only.', 'danger')
            return redirect(url_for('add_pensioner'))

        pid  = gen_pension_id()
        conn = get_db()
        cur  = conn.cursor(dictionary=True)
        cur.execute("SELECT id FROM pensioners WHERE pension_id=%s", (pid,))
        while cur.fetchone():
            pid = gen_pension_id()
            cur.execute("SELECT id FROM pensioners WHERE pension_id=%s", (pid,))
        try:
            cur.execute('''INSERT INTO pensioners
                (pension_id,name,dob,age,gender,phone,email,address,aadhaar,pension_type,status,monthly_amount)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (
                pid, name, f.get('dob') or None, int(f.get('age',0) or 0),
                f.get('gender',''), f['phone'], email, f.get('address',''),
                f.get('aadhaar',''), f['pension_type'], f['status'],
                float(f.get('monthly_amount',0) or 0)))
            new_id = cur.lastrowid
            cur.execute('''INSERT INTO bank_details
                (pensioner_id,bank_name,account_number,ifsc_code,branch,account_type)
                VALUES (%s,%s,%s,%s,%s,%s)''', (
                new_id, f.get('bank_name',''), f.get('account_number',''),
                f.get('ifsc_code',''), f.get('branch',''), f.get('account_type','Savings')))
            conn.commit()
        except Exception as e:
            conn.rollback(); conn.close()
            flash(f'Error: {e}', 'danger')
            return redirect(url_for('add_pensioner'))
        conn.close()
        add_notification('admin','New Pensioner Added', f"{f['name']} ({pid}) added.")
        run_fraud_detection()
        flash(f'Pensioner added! Pension ID: {pid}', 'success')
        return redirect(url_for('pensioners'))
    return render_template('pensioner_form.html', pensioner=None, bank=None, action='Add')

@app.route('/pensioners/edit/<int:pid>', methods=['GET','POST'])
@admin_required
def edit_pensioner(pid):
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,));            p    = qone(cur)
    cur.execute("SELECT * FROM bank_details WHERE pensioner_id=%s", (pid,));bank = qone(cur)
    if not p:
        conn.close(); flash('Not found.','danger')
        return redirect(url_for('pensioners'))
    if request.method == 'POST':
        f = request.form
        old_s, new_s = p['status'], f['status']
        try:
            cur.execute('''UPDATE pensioners SET name=%s,dob=%s,age=%s,gender=%s,phone=%s,email=%s,
                address=%s,aadhaar=%s,pension_type=%s,status=%s,monthly_amount=%s WHERE id=%s''', (
                f['name'], f.get('dob') or None, int(f.get('age',0) or 0), f.get('gender',''),
                f['phone'], f.get('email',''), f.get('address',''), f.get('aadhaar',''),
                f['pension_type'], new_s, float(f.get('monthly_amount',0) or 0), pid))
            if bank:
                cur.execute('''UPDATE bank_details SET bank_name=%s,account_number=%s,ifsc_code=%s,
                    branch=%s,account_type=%s WHERE pensioner_id=%s''', (
                    f.get('bank_name',''), f.get('account_number',''), f.get('ifsc_code',''),
                    f.get('branch',''), f.get('account_type','Savings'), pid))
            else:
                cur.execute('''INSERT INTO bank_details
                    (pensioner_id,bank_name,account_number,ifsc_code,branch,account_type)
                    VALUES (%s,%s,%s,%s,%s,%s)''', (
                    pid, f.get('bank_name',''), f.get('account_number',''),
                    f.get('ifsc_code',''), f.get('branch',''), f.get('account_type','Savings')))
            conn.commit()
        except Exception as e:
            conn.rollback(); conn.close()
            flash(f'Error: {e}', 'danger')
            return redirect(url_for('edit_pensioner', pid=pid))
        conn.close()
        if old_s != new_s:
            add_notification('pensioner', f'Status: {new_s}', STATUS_MSG.get(new_s, f'Status: {new_s}'), pid)
            add_notification('admin','Status Changed', f"{f['name']}: {old_s} to {new_s}.")
        run_fraud_detection()
        flash('Updated!', 'success')
        return redirect(url_for('pensioners'))
    conn.close()
    return render_template('pensioner_form.html', pensioner=p, bank=bank, action='Edit')

@app.route('/pensioners/delete/<int:pid>', methods=['POST'])
@admin_required
def delete_pensioner(pid):
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,))
    p = qone(cur)
    if p:
        cur.execute("DELETE FROM pensioners WHERE id=%s", (pid,))
        conn.commit()
        add_notification('admin','Pensioner Deleted', f"{p['name']} ({p['pension_id']}) removed.")
    conn.close()
    run_fraud_detection()
    flash('Deleted.', 'warning')
    return redirect(url_for('pensioners'))

@app.route('/pensioners/view/<int:pid>')
@admin_required
def view_pensioner(pid):
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,));                                               p       = qone(cur)
    cur.execute("SELECT * FROM bank_details WHERE pensioner_id=%s", (pid,));                                   bank    = qone(cur)
    cur.execute("SELECT * FROM payments WHERE pensioner_id=%s ORDER BY payment_date DESC", (pid,));            pmts    = qall(cur)
    cur.execute("SELECT * FROM notifications WHERE pensioner_id=%s ORDER BY created_at DESC LIMIT 10",(pid,)); notifs  = qall(cur)
    cur.execute("SELECT * FROM fraud_alerts WHERE pensioner_id=%s", (pid,));                                   alerts  = qall(cur)
    cur.execute("SELECT id FROM pensioner_accounts WHERE pensioner_id=%s", (pid,));                            has_acc = qone(cur)
    conn.close()
    return render_template('admin_pensioner_view.html',
        pensioner=p, bank=bank, payments=pmts, notifs=notifs, alerts=alerts, has_account=has_acc)

@app.route('/pensioners/approve/<int:pid>', methods=['POST'])
@admin_required
def approve_pensioner(pid):
    amount = float(request.form.get('monthly_amount', 0) or 0)
    conn   = get_db()
    cur    = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,))
    p = qone(cur)
    if p:
        new_pid = p['pension_id'] or gen_pension_id()
        cur.execute("UPDATE pensioners SET status='Approved', monthly_amount=%s, pension_id=%s WHERE id=%s",
            (amount or p['monthly_amount'], new_pid, pid))
        conn.commit()
        add_notification('pensioner','Application Approved', STATUS_MSG['Approved'], pid)
        add_notification('admin','Pensioner Approved', f"{p['name']} approved. Rs.{amount:,.0f}/month.")
        flash(f"{p['name']} approved!", 'success')
    conn.close()
    return redirect(url_for('dashboard'))

@app.route('/pensioners/grant_access/<int:pid>', methods=['POST'])
@admin_required
def grant_portal_access(pid):
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,))
    p = qone(cur)
    if p and p['email']:
        pwd = request.form.get('temp_password','pensioner123')
        cur.execute("SELECT id FROM pensioner_accounts WHERE pensioner_id=%s", (pid,))
        if cur.fetchone():
            cur.execute("UPDATE pensioner_accounts SET password=%s,is_active=1 WHERE pensioner_id=%s",
                (generate_password_hash(pwd), pid))
        else:
            cur.execute("INSERT INTO pensioner_accounts (pensioner_id,email,password) VALUES (%s,%s,%s)",
                (pid, p['email'], generate_password_hash(pwd)))
        conn.commit()
        add_notification('pensioner','Portal Access Granted',
            f"Dear {p['name']}, your PensionGov portal has been activated.", pid)
        flash(f"Portal access granted to {p['name']}. Temp password: {pwd}", 'success')
    else:
        flash('Email missing.', 'danger')
    conn.close()
    return redirect(url_for('view_pensioner', pid=pid))

# ══════════════════════════════════════════════════════════════
#  ADMIN – PAYMENTS
# ══════════════════════════════════════════════════════════════
@app.route('/payments')
@admin_required
def payments():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute('''SELECT pay.*, p.name, p.pension_id FROM payments pay
        JOIN pensioners p ON pay.pensioner_id=p.id ORDER BY pay.payment_date DESC''')
    rows  = qall(cur)
    cur.execute("SELECT id,name,pension_id,monthly_amount FROM pensioners WHERE status IN ('Approved','Disbursed')")
    plist = qall(cur)
    cur.execute("SELECT COALESCE(SUM(amount),0) as t FROM payments")
    total = qone(cur)['t']
    conn.close()
    return render_template('payments.html', payments=rows, pensioners=plist, total=total)

@app.route('/payments/add', methods=['POST'])
@admin_required
def add_payment():
    f     = request.form
    pid   = int(f['pensioner_id'])
    amount= float(f['amount'])
    pdate = f.get('payment_date', str(date.today()))
    month = f.get('payment_month', datetime.now().strftime('%B'))
    year  = int(f.get('payment_year', datetime.now().year))
    meth  = f.get('method','Bank Transfer')
    ref   = f.get('reference','')
    notes = f.get('notes','')
    conn  = get_db()
    cur   = conn.cursor(dictionary=True)
    cur.execute('''INSERT INTO payments
        (pensioner_id,amount,payment_date,payment_month,payment_year,method,reference,notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''', (pid,amount,pdate,month,year,meth,ref,notes))
    cur.execute("UPDATE pensioners SET status='Disbursed' WHERE id=%s", (pid,))
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,))
    p = qone(cur)
    conn.commit(); conn.close()
    add_notification('admin','Payment Recorded', f"Rs.{amount:,.0f} for {p['name']} ({p['pension_id']}).")
    add_notification('pensioner','Payment Credited',
        f"Dear {p['name']}, Rs.{amount:,.0f} credited for {month} {year}. Ref: {ref or 'N/A'}.", pid)
    flash('Payment recorded!', 'success')
    return redirect(url_for('payments'))

# ══════════════════════════════════════════════════════════════
#  ADMIN – NOTIFICATIONS / FRAUD / REPORTS
# ══════════════════════════════════════════════════════════════
@app.route('/notifications')
@admin_required
def notifications():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute('''SELECT n.*, p.name as pname, p.pension_id as ppid
        FROM notifications n LEFT JOIN pensioners p ON n.pensioner_id=p.id
        ORDER BY n.created_at DESC''')
    rows = qall(cur)
    cur.execute("UPDATE notifications SET is_read=1 WHERE recipient_type='admin'")
    conn.commit(); conn.close()
    return render_template('notifications.html', notifications=rows)

@app.route('/notifications/clear', methods=['POST'])
@admin_required
def clear_notifications():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("UPDATE notifications SET is_read=1")
    conn.commit(); conn.close()
    flash('Cleared.', 'success')
    return redirect(url_for('notifications'))

@app.route('/fraud')
@admin_required
def fraud():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute('''SELECT fa.*, p.name, p.pension_id FROM fraud_alerts fa
        LEFT JOIN pensioners p ON fa.pensioner_id=p.id ORDER BY fa.created_at DESC''')
    alerts = qall(cur)
    conn.close()
    return render_template('fraud.html', alerts=alerts)

@app.route('/fraud/resolve/<int:aid>', methods=['POST'])
@admin_required
def resolve_fraud(aid):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("UPDATE fraud_alerts SET is_resolved=1 WHERE id=%s", (aid,))
    conn.commit(); conn.close()
    flash('Resolved.', 'success')
    return redirect(url_for('fraud'))

@app.route('/fraud/rerun', methods=['POST'])
@admin_required
def rerun_fraud():
    run_fraud_detection()
    flash('Fraud detection complete.', 'info')
    return redirect(url_for('fraud'))

@app.route('/reports')
@admin_required
def reports():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute('''SELECT payment_month,payment_year,COUNT(*) cnt,SUM(amount) total
        FROM payments GROUP BY payment_year,payment_month ORDER BY payment_year DESC, payment_month DESC''')
    monthly = qall(cur)
    cur.execute("SELECT pension_type,COUNT(*) cnt,SUM(monthly_amount) total FROM pensioners GROUP BY pension_type")
    type_data = qall(cur)
    cur.execute("SELECT status,COUNT(*) cnt FROM pensioners GROUP BY status")
    status_data = qall(cur)
    cur.execute("SELECT alert_type,COUNT(*) cnt FROM fraud_alerts GROUP BY alert_type")
    fraud_summ = qall(cur)
    cur.execute("SELECT COALESCE(SUM(amount),0) as t FROM payments")
    grand_total = qone(cur)['t']
    conn.close()
    return render_template('reports.html',
        monthly=monthly, type_data=type_data,
        status_data=status_data, fraud_summary=fraud_summ, grand_total=grand_total)

# ══════════════════════════════════════════════════════════════
#  PENSIONER PORTAL – REGISTRATION
# ══════════════════════════════════════════════════════════════
@app.route('/pensioner/register', methods=['GET','POST'])
def pensioner_register():
    if session.get('role') == 'pensioner':
        return redirect(url_for('pensioner_dashboard'))
    error = None
    if request.method == 'POST':
        f       = request.form
        name    = f.get('name','').strip()
        email   = f.get('email','').strip().lower()  # force lowercase
        aadhaar = f.get('aadhaar','').strip()
        pwd     = f.get('password','')
        pwd2    = f.get('confirm_password','')

        # ── Name validation — alphabets and spaces only ───────
        import re
        if not name:
            error = 'Full name is required.'
        elif re.search(r'\d', name):
            error = 'Name must contain alphabets only. Numbers are not allowed.'
        elif re.search(r'[^A-Za-z\s]', name):
            error = 'Name must contain alphabets and spaces only. Special characters are not allowed.'
        elif pwd != pwd2:
            error = 'Passwords do not match.'
        elif len(pwd) < 6:
            error = 'Password must be at least 6 characters.'
        else:
            conn = get_db()
            cur  = conn.cursor(dictionary=True)
            cur.execute("SELECT id FROM pensioners WHERE email=%s", (email,))
            if cur.fetchone():
                error = 'Email already registered.'
            else:
                cur.execute("SELECT id FROM pensioners WHERE aadhaar=%s", (aadhaar,))
                if aadhaar and cur.fetchone():
                    error = 'Aadhaar number already registered.'
                else:
                    dob = f.get('dob','')
                    age = int(f.get('age', 0) or 0)
                    if dob and not age:
                        try:
                            bd  = datetime.strptime(dob, '%Y-%m-%d')
                            age = (date.today() - bd.date()).days // 365
                        except:
                            pass
                    try:
                        cur.execute('''INSERT INTO pensioners
                            (name,dob,age,gender,phone,email,address,aadhaar,pension_type,status)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', (
                            f['name'], dob or None, age, f.get('gender',''),
                            f['phone'], email, f.get('address',''),
                            aadhaar, f['pension_type'], 'Pending Verification'))
                        new_pid = cur.lastrowid
                        cur.execute('''INSERT INTO bank_details
                            (pensioner_id,bank_name,account_number,ifsc_code,branch,account_type)
                            VALUES (%s,%s,%s,%s,%s,%s)''', (
                            new_pid, f.get('bank_name',''), f.get('account_number',''),
                            f.get('ifsc_code',''), f.get('branch',''), f.get('account_type','Savings')))
                        cur.execute('''INSERT INTO pensioner_accounts (pensioner_id,email,password)
                            VALUES (%s,%s,%s)''', (new_pid, email, generate_password_hash(pwd)))
                        conn.commit()
                        add_notification('admin','New Registration',
                            f"New registration from {f['name']} ({email}). Pending verification.")
                        add_notification('pensioner','Registration Successful',
                            f"Dear {f['name']}, your application is submitted (Pending Verification).", new_pid)
                        run_fraud_detection()
                        flash('Registration successful! Pending verification.', 'success')
                        conn.close()
                        return redirect(url_for('pensioner_login'))
                    except Exception as e:
                        conn.rollback()
                        error = f'Registration error: {str(e)}'
                    conn.close()
    return render_template('pensioner_register.html', error=error)

# ══════════════════════════════════════════════════════════════
#  PENSIONER PORTAL – LOGIN
# ══════════════════════════════════════════════════════════════
@app.route('/pensioner/login', methods=['GET','POST'])
def pensioner_login():
    if session.get('role') == 'pensioner':
        return redirect(url_for('pensioner_dashboard'))
    error = None
    if request.method == 'POST':
        identifier = request.form.get('identifier','').strip()
        pwd        = request.form.get('password','')
        conn       = get_db()
        cur        = conn.cursor(dictionary=True)
        cur.execute('''SELECT pa.*, p.pension_id, p.name, p.status, p.id as pid
            FROM pensioner_accounts pa JOIN pensioners p ON pa.pensioner_id=p.id
            WHERE pa.email=%s AND pa.is_active=1''', (identifier,))
        acc = qone(cur)
        if not acc:
            cur.execute("SELECT id FROM pensioners WHERE pension_id=%s", (identifier,))
            row = qone(cur)
            if row:
                cur.execute('''SELECT pa.*, p.pension_id, p.name, p.status, p.id as pid
                    FROM pensioner_accounts pa JOIN pensioners p ON pa.pensioner_id=p.id
                    WHERE pa.pensioner_id=%s AND pa.is_active=1''', (row['id'],))
                acc = qone(cur)
        conn.close()
        if acc and check_password_hash(acc['password'], pwd):
            session.clear()
            session['role']           = 'pensioner'
            session['account_id']     = acc['id']
            session['pensioner_id']   = acc['pid']
            session['pensioner_name'] = acc['name']
            session['pension_id']     = acc['pension_id']
            return redirect(url_for('pensioner_dashboard'))
        error = 'Invalid email/Pension ID or password.'
    return render_template('pensioner_login.html', error=error)

@app.route('/pensioner/logout')
def pensioner_logout():
    session.clear()
    return redirect(url_for('pensioner_login'))

# ══════════════════════════════════════════════════════════════
#  PENSIONER PORTAL – PAGES
# ══════════════════════════════════════════════════════════════
@app.route('/pensioner/dashboard')
@pensioner_required
def pensioner_dashboard():
    pid  = session['pensioner_id']
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,));                                                p          = qone(cur)
    cur.execute("SELECT * FROM bank_details WHERE pensioner_id=%s", (pid,));                                    bank       = qone(cur)
    cur.execute("SELECT * FROM payments WHERE pensioner_id=%s ORDER BY payment_date DESC LIMIT 1", (pid,));     last_pmt   = qone(cur)
    cur.execute("SELECT COUNT(*) as cnt FROM payments WHERE pensioner_id=%s", (pid,));                          pmt_count  = qone(cur)['cnt']
    cur.execute("SELECT COALESCE(SUM(amount),0) as t FROM payments WHERE pensioner_id=%s", (pid,));             total_recd = qone(cur)['t']
    cur.execute("SELECT COUNT(*) as cnt FROM notifications WHERE pensioner_id=%s AND is_read=0", (pid,));       unread     = qone(cur)['cnt']
    cur.execute("SELECT * FROM notifications WHERE pensioner_id=%s ORDER BY created_at DESC LIMIT 4", (pid,));  recent_notifs = qall(cur)
    cur.execute("SELECT * FROM payments WHERE pensioner_id=%s ORDER BY payment_date DESC LIMIT 3", (pid,));     recent_pmts   = qall(cur)
    conn.close()
    next_payment = None
    if p['status'] in ('Approved','Disbursed') and last_pmt:
        try:
            ld = datetime.strptime(str(last_pmt['payment_date']), '%Y-%m-%d')
            np = (ld.replace(day=1) + timedelta(days=32)).replace(day=10)
            next_payment = np.strftime('%d %b %Y')
        except:
            pass
    return render_template('pensioner_dashboard.html',
        p=p, bank=bank, last_pmt=last_pmt,
        pmt_count=pmt_count, total_recd=total_recd,
        unread=unread, recent_notifs=recent_notifs,
        recent_pmts=recent_pmts, next_payment=next_payment)

@app.route('/pensioner/profile')
@pensioner_required
def pensioner_profile():
    pid  = session['pensioner_id']
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,));           p    = qone(cur)
    cur.execute("SELECT * FROM bank_details WHERE pensioner_id=%s",(pid,));bank = qone(cur)
    conn.close()
    return render_template('pensioner_profile.html', p=p, bank=bank)

@app.route('/pensioner/payments')
@pensioner_required
def pensioner_payments():
    pid  = session['pensioner_id']
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM payments WHERE pensioner_id=%s ORDER BY payment_date DESC", (pid,)); pmts  = qall(cur)
    cur.execute("SELECT COALESCE(SUM(amount),0) as t FROM payments WHERE pensioner_id=%s",(pid,)); total = qone(cur)['t']
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,));                                   p     = qone(cur)
    conn.close()
    return render_template('pensioner_payments.html', payments=pmts, total=total, p=p)

@app.route('/pensioner/notifications')
@pensioner_required
def pensioner_notifications():
    pid  = session['pensioner_id']
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM notifications WHERE pensioner_id=%s ORDER BY created_at DESC", (pid,))
    rows = qall(cur)
    cur.execute("UPDATE notifications SET is_read=1 WHERE pensioner_id=%s", (pid,))
    conn.commit(); conn.close()
    return render_template('pensioner_notifications.html', notifications=rows)

# ══════════════════════════════════════════════════════════════
#  PENSIONER PORTAL – EDIT PROFILE
# ══════════════════════════════════════════════════════════════
@app.route('/pensioner/edit_profile', methods=['GET','POST'])
@pensioner_required
def pensioner_edit_profile():
    pid  = session['pensioner_id']
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,));                p       = qone(cur)
    cur.execute("SELECT * FROM bank_details WHERE pensioner_id=%s", (pid,));    bank    = qone(cur)
    cur.execute("SELECT * FROM pensioner_accounts WHERE pensioner_id=%s",(pid,));acc    = qone(cur)
    cur.execute("SELECT * FROM profile_update_requests WHERE pensioner_id=%s ORDER BY created_at DESC LIMIT 20",(pid,))
    pending = qall(cur)

    if request.method == 'POST':
        f        = request.form
        EDITABLE = {
            'phone':          ('pensioner', p['phone']                     or ''),
            'email':          ('pensioner', p['email']                     or ''),
            'bank_name':      ('bank',      bank['bank_name']   if bank else ''),
            'account_number': ('bank',      bank['account_number'] if bank else ''),
            'ifsc_code':      ('bank',      bank['ifsc_code']   if bank else ''),
            'branch':         ('bank',      bank['branch']      if bank else ''),
        }
        changes_submitted = 0
        duplicate_skipped = 0

        for field, (table, old_val) in EDITABLE.items():
            new_val = f.get(field,'').strip()
            if not new_val or new_val == old_val:
                continue
            cur.execute('''SELECT id FROM profile_update_requests
                WHERE pensioner_id=%s AND field_name=%s AND status='Pending Review' ''', (pid, field))
            if cur.fetchone():
                duplicate_skipped += 1
                continue
            cur.execute('''INSERT INTO profile_update_requests
                (pensioner_id,field_name,old_value,new_value,status)
                VALUES (%s,%s,%s,%s,'Pending Review')''', (pid, field, old_val, new_val))
            changes_submitted += 1

        new_pw  = f.get('new_password','').strip()
        conf_pw = f.get('confirm_password','').strip()
        if new_pw:
            if new_pw != conf_pw:
                conn.close()
                flash('Passwords do not match.', 'danger')
                return redirect(url_for('pensioner_edit_profile'))
            if len(new_pw) < 6:
                conn.close()
                flash('Password must be at least 6 characters.', 'danger')
                return redirect(url_for('pensioner_edit_profile'))
            cur.execute('''SELECT id FROM profile_update_requests
                WHERE pensioner_id=%s AND field_name='password' AND status='Pending Review' ''',(pid,))
            if not cur.fetchone():
                cur.execute('''INSERT INTO profile_update_requests
                    (pensioner_id,field_name,old_value,new_value,status)
                    VALUES (%s,'password','[current password]',%s,'Pending Review')''',
                    (pid, generate_password_hash(new_pw)))
                changes_submitted += 1

        if changes_submitted > 0:
            conn.commit()
            add_notification('admin','Profile Update Request',
                f"Pensioner {p['name']} submitted {changes_submitted} update request(s).")
            add_notification('pensioner','Update Request Submitted',
                "Your profile update is pending admin review.", pid)
            flash(f'{changes_submitted} update request(s) submitted for admin review.', 'success')
        elif duplicate_skipped > 0:
            flash('You already have pending requests for those fields.', 'warning')
        else:
            flash('No changes detected.', 'info')

        conn.close()
        return redirect(url_for('pensioner_edit_profile'))

    conn.close()
    return render_template('edit_profile.html', p=p, bank=bank, acc=acc, pending=pending)

# ══════════════════════════════════════════════════════════════
#  ADMIN – PROFILE UPDATE REQUESTS
# ══════════════════════════════════════════════════════════════
@app.route('/admin/profile_requests')
@admin_required
def profile_update_requests():
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute('''SELECT pur.*, p.name as pensioner_name, p.pension_id as pensioner_pid
        FROM profile_update_requests pur JOIN pensioners p ON pur.pensioner_id=p.id
        ORDER BY CASE pur.status WHEN 'Pending Review' THEN 0 ELSE 1 END, pur.created_at DESC''')
    requests_list = qall(cur)
    cur.execute("SELECT COUNT(*) as cnt FROM profile_update_requests WHERE status='Pending Review'")
    pending_count = qone(cur)['cnt']
    conn.close()
    return render_template('profile_update_requests.html',
                           requests=requests_list, pending_count=pending_count)

@app.route('/admin/profile_requests/approve/<int:req_id>', methods=['POST'])
@admin_required
def approve_profile_request(req_id):
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM profile_update_requests WHERE id=%s", (req_id,))
    req  = qone(cur)
    if not req or req['status'] != 'Pending Review':
        conn.close()
        flash('Request not found or already actioned.', 'warning')
        return redirect(url_for('profile_update_requests'))

    pid, field, new_value = req['pensioner_id'], req['field_name'], req['new_value']
    BANK_FIELDS = {'bank_name','account_number','ifsc_code','branch'}

    if field == 'password':
        cur.execute("UPDATE pensioner_accounts SET password=%s WHERE pensioner_id=%s", (new_value, pid))
    elif field == 'email':
        cur.execute("UPDATE pensioners SET email=%s WHERE id=%s", (new_value, pid))
        cur.execute("UPDATE pensioner_accounts SET email=%s WHERE pensioner_id=%s", (new_value, pid))
    elif field in BANK_FIELDS:
        cur.execute("SELECT id FROM bank_details WHERE pensioner_id=%s", (pid,))
        if cur.fetchone():
            cur.execute(f"UPDATE bank_details SET {field}=%s WHERE pensioner_id=%s", (new_value, pid))
        else:
            cur.execute(f"INSERT INTO bank_details (pensioner_id, {field}) VALUES (%s, %s)", (pid, new_value))
    else:
        cur.execute(f"UPDATE pensioners SET {field}=%s WHERE id=%s", (new_value, pid))

    cur.execute("UPDATE profile_update_requests SET status='Approved' WHERE id=%s", (req_id,))
    conn.commit()
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,))
    p = qone(cur)
    conn.close()

    field_label = field.replace('_',' ').title()
    add_notification('pensioner','Profile Update Approved',
        f"Your {field_label} update has been approved.", pid)
    add_notification('admin','Profile Request Approved',
        f"Profile update for {p['name']}: '{field_label}' approved.")
    flash(f"Approved: {field_label} updated for {p['name']}.", 'success')
    return redirect(url_for('profile_update_requests'))

@app.route('/admin/profile_requests/reject/<int:req_id>', methods=['POST'])
@admin_required
def reject_profile_request(req_id):
    conn = get_db()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM profile_update_requests WHERE id=%s", (req_id,))
    req  = qone(cur)
    if not req or req['status'] != 'Pending Review':
        conn.close()
        flash('Request not found or already actioned.', 'warning')
        return redirect(url_for('profile_update_requests'))

    pid, field = req['pensioner_id'], req['field_name']
    cur.execute("UPDATE profile_update_requests SET status='Rejected' WHERE id=%s", (req_id,))
    conn.commit()
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,))
    p = qone(cur)
    conn.close()

    field_label = field.replace('_',' ').title()
    add_notification('pensioner','Profile Update Rejected',
        f"Your {field_label} update request was not approved. Please contact the office.", pid)
    add_notification('admin','Profile Request Rejected',
        f"Profile update for {p['name']}: '{field_label}' rejected.")
    flash(f"Rejected: {field_label} update for {p['name']}.", 'warning')
    return redirect(url_for('profile_update_requests'))

# ══════════════════════════════════════════════════════════════
#  API
# ══════════════════════════════════════════════════════════════
@app.route('/api/unread_count')
def api_unread_count():
    if session.get('role') == 'admin':
        conn  = get_db()
        cur   = conn.cursor(dictionary=True)
        cur.execute("SELECT COUNT(*) as cnt FROM notifications WHERE is_read=0 AND recipient_type='admin'")
        count = qone(cur)['cnt']
        conn.close()
        return jsonify({'count': count})
    if session.get('role') == 'pensioner':
        pid   = session.get('pensioner_id')
        conn  = get_db()
        cur   = conn.cursor(dictionary=True)
        cur.execute("SELECT COUNT(*) as cnt FROM notifications WHERE pensioner_id=%s AND is_read=0", (pid,))
        count = qone(cur)['cnt']
        conn.close()
        return jsonify({'count': count})
    return jsonify({'count': 0})

@app.route('/api/status_update/<int:pid>', methods=['POST'])
@admin_required
def api_status_update(pid):
    data  = request.get_json()
    new_s = data.get('status')
    conn  = get_db()
    cur   = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM pensioners WHERE id=%s", (pid,))
    p = qone(cur)
    if not p:
        conn.close(); return jsonify({'error':'Not found'}), 404
    cur.execute("UPDATE pensioners SET status=%s WHERE id=%s", (new_s, pid))
    conn.commit(); conn.close()
    add_notification('pensioner', f'Status: {new_s}', STATUS_MSG.get(new_s, f'Status: {new_s}.'), pid)
    return jsonify({'success': True})

if __name__ == '__main__':
    init_db()
    app.run(debug=False, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
