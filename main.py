from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
import uvicorn, os, secrets, hashlib, time
import stripe
from jose import jwt as jose_jwt
from datetime import datetime, timedelta, timezone
from supabase import create_client

app = FastAPI()

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY      = os.environ.get("SUPABASE_SERVICE_KEY", "")
STRIPE_SECRET     = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_PRICE_ID   = os.environ.get("STRIPE_PRICE_ID", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
SITE_URL          = os.environ.get("SITE_URL", "http://localhost:8000")
SECRET_KEY        = os.environ.get("SECRET_KEY", secrets.token_hex(32))
JWT_SECRET        = os.environ.get("JWT_SECRET", SECRET_KEY)

def make_app_token(email: str) -> str:
    """Generate a 30-day signed JWT for app access."""
    expire = datetime.now(timezone.utc) + timedelta(days=30)
    return jose_jwt.encode({"sub": email, "exp": expire}, JWT_SECRET, algorithm="HS256")


stripe.api_key = STRIPE_SECRET
db = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

ADMIN_EMAIL    = os.environ.get("ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

SESSIONS: dict[str, str] = {}

def hash_pw(pw: str) -> str:
    return hashlib.sha256((pw + SECRET_KEY).encode()).hexdigest()

def get_user(request: Request) -> str:
    sid = request.cookies.get("sid")
    return SESSIONS.get(sid, "") if sid else ""

# ── HTML ───────────────────────────────────────────────────────────────────────
BASE_STYLE = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;700;900&family=Source+Sans+Pro:wght@300;400;600;700&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0f0f0f;color:#fff;font-family:'Source Sans Pro',sans-serif;min-height:100vh}
.font-display{font-family:'Playfair Display',serif}
nav{position:fixed;top:0;width:100%;background:rgba(10,10,10,.95);backdrop-filter:blur(12px);border-bottom:1px solid #1c1c1c;z-index:100;padding:0 32px;height:80px;display:flex;align-items:center;justify-content:space-between}
.logo{font-family:'Playfair Display',serif;font-size:42px;font-weight:900;color:#f59e0b;letter-spacing:.02em;line-height:1}
.logo span{color:#fff}
.nav-links{display:flex;align-items:center;gap:20px}
.nav-link{color:#9ca3af;font-size:13px;text-decoration:none;font-weight:600;transition:color .2s}
.nav-link:hover{color:#fff}
.btn{display:inline-block;background:#f59e0b;color:#000;font-weight:700;padding:10px 24px;border-radius:8px;text-decoration:none;font-size:14px;border:none;cursor:pointer;transition:all .2s;font-family:'Source Sans Pro',sans-serif}
.btn:hover{background:#fbbf24;transform:translateY(-1px);box-shadow:0 4px 20px rgba(245,158,11,.4)}
.btn-lg{font-size:18px;padding:16px 40px;border-radius:12px}
.btn-outline{background:transparent;color:#f59e0b;border:2px solid #f59e0b}
.btn-outline:hover{background:#f59e0b;color:#000}
.card{background:#161616;border:1px solid #262626;border-radius:20px;padding:32px;transition:all .2s}
.card:hover{border-color:rgba(245,158,11,.3);transform:translateY(-2px)}
.gold{color:#f59e0b}
.error-box{background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.2);color:#f87171;border-radius:10px;padding:14px 16px;font-size:13px;margin-bottom:16px}
.success-box{background:rgba(74,222,128,.08);border:1px solid rgba(74,222,128,.2);color:#4ade80;border-radius:10px;padding:14px 16px;font-size:13px;margin-bottom:16px}
input[type=email],input[type=password],input[type=text]{width:100%;background:#0a0a0a;border:1px solid #2a2a2a;border-radius:10px;padding:13px 16px;color:#fff;font-size:14px;font-family:'Source Sans Pro',sans-serif;outline:none;transition:border .2s;margin-bottom:4px}
input:focus{border-color:#f59e0b}
label{display:block;color:#9ca3af;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;margin-top:16px}
form button[type=submit]{width:100%;margin-top:20px}
footer{border-top:1px solid #1a1a1a;padding:28px 32px;text-align:center;color:#374151;font-size:11px;line-height:1.7}
</style>
"""

HOME_HTML = BASE_STYLE + """
<nav>
  <div class="logo">Money <span>Picks</span> Arena</div>
  <div class="nav-links">
    <a href="/login" class="nav-link">Member Login</a>
    <a href="/subscribe" class="btn">Subscribe — $50/mo</a>
  </div>
</nav>

<div style="padding-top:80px">
  <!-- HERO -->
  <section style="padding:100px 24px 80px;text-align:center;position:relative;overflow:hidden">
    <div style="position:absolute;inset:0;background:radial-gradient(ellipse at 50% 40%,rgba(245,158,11,.04),transparent 65%);pointer-events:none"></div>
    <div style="position:relative;max-width:760px;margin:0 auto">
      <div style="display:inline-flex;align-items:center;gap:8px;background:rgba(245,158,11,.07);border:1px solid rgba(245,158,11,.15);border-radius:999px;padding:6px 18px;margin-bottom:28px">
        <span style="width:7px;height:7px;background:#4ade80;border-radius:50%;animation:p 2s infinite"></span>
        <span style="font-size:11px;font-weight:700;letter-spacing:.12em;color:#f59e0b">PICKS UPDATED DAILY</span>
      </div>
      <style>@keyframes p{0%,100%{opacity:1}50%{opacity:.35}}</style>
      <h1 class="font-display" style="font-size:clamp(42px,7vw,76px);line-height:1.05;margin-bottom:20px">
        Score Big in the<br><span class="gold">Money Picks Arena</span>
      </h1>
      <p style="color:#9ca3af;font-size:18px;margin-bottom:10px;max-width:520px;margin-left:auto;margin-right:auto;line-height:1.6">
        Data-driven picks for <strong style="color:#fff">4 sports</strong> — MLB, NHL, NBA &amp; NFL — powered by real stats and sportsbook lines.
      </p>
      <p style="color:#4b5563;font-size:13px;letter-spacing:.14em;margin-bottom:40px">ONE SUBSCRIPTION. ALL 4 SPORTS.</p>
      <div style="display:flex;flex-direction:column;align-items:center;gap:12px">
        <a href="/subscribe" class="btn btn-lg" style="box-shadow:0 0 40px rgba(245,158,11,.3)">⚡ SUBSCRIBE NOW — $50/MO</a>
        <a href="/login" style="color:#4b5563;font-size:13px;text-decoration:none">Already a member? Login →</a>
      </div>
    </div>
  </section>

  <!-- SPORTS -->
  <section style="padding:60px 24px;max-width:1000px;margin:0 auto">
    <h2 class="font-display" style="text-align:center;font-size:32px;margin-bottom:10px">Choose Your Sport</h2>
    <p style="text-align:center;color:#6b7280;margin-bottom:40px">Money Picks Arena shows you the plays — you choose what to do.</p>
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:16px">
      <div class="card" style="text-align:center">
        <div style="font-size:44px;margin-bottom:12px">⚾</div>
        <div style="font-size:10px;font-weight:700;letter-spacing:.1em;color:#fff;background:#1d4ed8;padding:3px 10px;border-radius:4px;display:inline-block;margin-bottom:10px">BASEBALL</div>
        <h3 class="font-display" style="font-size:18px;margin-bottom:8px">MLB MoneyBall</h3>
        <p style="color:#6b7280;font-size:12px;line-height:1.6">Career BA vs pitcher, H/A splits, hot streaks. Top 9 picks daily.</p>
      </div>
      <div class="card" style="text-align:center">
        <div style="font-size:44px;margin-bottom:12px">🏒</div>
        <div style="font-size:10px;font-weight:700;letter-spacing:.1em;color:#fff;background:#15803d;padding:3px 10px;border-radius:4px;display:inline-block;margin-bottom:10px">HOCKEY</div>
        <h3 class="font-display" style="font-size:18px;margin-bottom:8px">NHL Money Shots</h3>
        <p style="color:#6b7280;font-size:12px;line-height:1.6">Shots on goal picks with live FanDuel sportsbook lines.</p>
      </div>
      <div class="card" style="text-align:center">
        <div style="font-size:44px;margin-bottom:12px">🏀</div>
        <div style="font-size:10px;font-weight:700;letter-spacing:.1em;color:#fff;background:#7e22ce;padding:3px 10px;border-radius:4px;display:inline-block;margin-bottom:10px">BASKETBALL</div>
        <h3 class="font-display" style="font-size:18px;margin-bottom:8px">NBA Money Buckets</h3>
        <p style="color:#6b7280;font-size:12px;line-height:1.6">75%+ hit rate picks for Pts, Reb, Ast, 3PM.</p>
      </div>
      <div class="card" style="text-align:center">
        <div style="font-size:44px;margin-bottom:12px">🏈</div>
        <div style="font-size:10px;font-weight:700;letter-spacing:.1em;color:#fff;background:#b45309;padding:3px 10px;border-radius:4px;display:inline-block;margin-bottom:10px">FOOTBALL</div>
        <h3 class="font-display" style="font-size:18px;margin-bottom:8px">NFL Money Bombs</h3>
        <p style="color:#6b7280;font-size:12px;line-height:1.6">Weekly NFL player prop picks with matchup analysis.</p>
      </div>
    </div>
  </section>

  <!-- PRICING -->
  <section style="padding:60px 24px;max-width:460px;margin:0 auto">
    <div class="card" style="border-color:rgba(245,158,11,.35);text-align:center;padding:44px">
      <h2 class="font-display" style="font-size:26px;margin-bottom:4px">All Access Pass</h2>
      <p style="color:#6b7280;margin-bottom:24px;font-size:14px">One subscription. Every sport.</p>
      <div style="font-size:68px;font-weight:900;color:#fff;line-height:1;font-family:'Playfair Display',serif">$50</div>
      <div style="color:#6b7280;margin-bottom:28px">per month</div>
      <div style="text-align:left;margin-bottom:28px;display:flex;flex-direction:column;gap:10px">
        <div style="color:#d1d5db;font-size:13px">⚾&nbsp; MLB MoneyBall — Daily Baseball Picks</div>
        <div style="color:#d1d5db;font-size:13px">🏒&nbsp; NHL Money Shots — Daily Hockey Picks</div>
        <div style="color:#d1d5db;font-size:13px">🏀&nbsp; NBA Money Buckets — Daily Basketball Picks</div>
        <div style="color:#d1d5db;font-size:13px">🏈&nbsp; NFL Money Bombs — Weekly Football Picks</div>
        <div style="color:#d1d5db;font-size:13px">✅&nbsp; Real sportsbook lines included</div>
        <div style="color:#d1d5db;font-size:13px">✅&nbsp; Cancel anytime</div>
      </div>
      <a href="/subscribe" class="btn btn-lg" style="display:block;width:100%;text-align:center;box-shadow:0 0 30px rgba(245,158,11,.25)">SUBSCRIBE NOW</a>
    </div>
  </section>
</div>

<footer>
  <div style="font-family:'Playfair Display',serif;font-size:16px;color:#4b5563;margin-bottom:10px">Money Picks Arena</div>
  <p style="color:#4b5563;font-size:12px;max-width:600px;margin:0 auto 8px;line-height:1.8">
    For entertainment and informational purposes only. We do not accept bets or guarantee results. 
    Please gamble responsibly. Must be 18+ (21+ in some states).
  </p>
  <p style="margin-top:4px;color:#374151">
    <a href="https://www.ncpgambling.org" target="_blank" style="color:#4b5563;text-decoration:underline">Problem Gambling Help</a>
    &nbsp;·&nbsp; 1-800-522-4700
  </p>
  <p style="margin-top:8px">© 2026 Money Picks Arena. All Rights Reserved.</p>
</footer>
"""

LOGIN_HTML = BASE_STYLE + """
<nav>
  <div class="logo">Money <span>Picks</span> Arena</div>
  <div class="nav-links">
    <a href="/subscribe" class="btn">Subscribe — $50/mo</a>
  </div>
</nav>
<div style="min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;padding-top:100px">
  <div style="width:100%;max-width:400px">
    <div style="text-align:center;margin-bottom:28px">
      <div class="font-display gold" style="font-size:22px;margin-bottom:4px">Money Picks Arena</div>
      <h1 style="font-size:26px;font-weight:900;margin-bottom:4px">Member Login</h1>
      <p style="color:#6b7280;font-size:13px">Access your picks dashboard</p>
    </div>
    <div class="card">
      {error}
      <form method="post" action="/login">
        <label>Email Address</label>
        <input type="email" name="email" placeholder="you@example.com" required autocomplete="email"/>
        <label>Password</label>
        <input type="password" name="password" placeholder="••••••••" required autocomplete="current-password"/>
        <button type="submit" class="btn" style="font-size:16px;padding:14px">LOGIN →</button>
      </form>
      <p style="text-align:center;margin-top:18px;font-size:13px;color:#4b5563">
        Not a member? <a href="/subscribe" style="color:#f59e0b;text-decoration:none">Subscribe for $50/mo</a>
      </p>
    </div>
    <p style="text-align:center;margin-top:16px"><a href="/" style="color:#374151;font-size:12px;text-decoration:none">← Back to home</a></p>
  </div>
</div>
"""

REGISTER_HTML = BASE_STYLE + """
<nav>
  <div class="logo">Money <span>Picks</span> Arena</div>
</nav>
<div style="min-height:100vh;display:flex;align-items:center;justify-content:center;padding:24px;padding-top:100px">
  <div style="width:100%;max-width:420px">
    <div style="text-align:center;margin-bottom:28px">
      <div style="font-size:48px;margin-bottom:8px">🎉</div>
      <h1 class="font-display" style="font-size:26px;margin-bottom:4px">Payment Successful!</h1>
      <p style="color:#6b7280;font-size:14px">Create your account to access all 4 sports picks.</p>
    </div>
    <div class="card">
      {error}
      <form method="post" action="/register">
        <input type="hidden" name="session_id" value="{session_id}"/>
        <label>Email Address</label>
        <input type="email" name="email" value="{email}" readonly style="background:#1a1a1a;color:#9ca3af;cursor:not-allowed"/>
        <label>Create a Password</label>
        <input type="password" name="password" placeholder="Choose a strong password (min 6 chars)" required minlength="6" autocomplete="new-password"/>
        <label>Confirm Password</label>
        <input type="password" name="confirm" placeholder="Repeat your password" required minlength="6" autocomplete="new-password"/>
        <button type="submit" class="btn" style="font-size:16px;padding:14px">CREATE ACCOUNT &amp; LOGIN →</button>
      </form>
    </div>
  </div>
</div>
"""

DASHBOARD_HTML = BASE_STYLE + """
<nav>
  <div class="logo">Money <span>Picks</span> Arena</div>
  <div class="nav-links">
    <span style="color:#4b5563;font-size:12px">{email}</span>
    <span style="background:rgba(74,222,128,.08);border:1px solid rgba(74,222,128,.2);color:#4ade80;font-size:11px;font-weight:700;padding:4px 12px;border-radius:999px">✓ ACTIVE</span>
    <a href="/admin" style="color:#f59e0b;font-size:12px;font-weight:700;text-decoration:none" class="nav-link">⚙️ Admin</a>
    <a href="/logout" class="nav-link">Logout</a>
  </div>
</nav>
<div style="max-width:1000px;margin:0 auto;padding:100px 24px 60px">
  <h1 class="font-display" style="font-size:36px;margin-bottom:6px">Welcome back! 🏆</h1>
  <p style="color:#6b7280;margin-bottom:16px">Choose your sport below and get today's picks.</p>
  <div style="background:rgba(245,158,11,.06);border:1px solid rgba(245,158,11,.2);border-radius:10px;padding:12px 18px;margin-bottom:32px;display:flex;align-items:center;gap:12px;font-size:12px">
    <span style="font-size:20px">🔐</span>
    <span style="color:#6b7280">These picks are exclusively for <strong style="color:#f59e0b">{email}</strong> — sharing your account or picks violates our terms and will result in immediate cancellation.</span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:20px">
    <div class="card" style="text-align:center;display:flex;flex-direction:column;gap:14px;align-items:center">
      <div style="font-size:52px">⚾</div>
      <span style="font-size:10px;font-weight:700;letter-spacing:.1em;background:#1d4ed8;color:#fff;padding:3px 10px;border-radius:4px">BASEBALL</span>
      <h3 class="font-display" style="font-size:20px">MLB MoneyBall</h3>
      <p style="color:#6b7280;font-size:12px;line-height:1.6">Career stats vs pitcher, H/A splits, hot streaks. Top 9 picks daily.</p>
      <a href="/launch/mlb" target="_blank" class="btn" style="width:100%;text-align:center">🎯 OPEN PICKS</a>
    </div>
    <div class="card" style="text-align:center;display:flex;flex-direction:column;gap:14px;align-items:center">
      <div style="font-size:52px">🏒</div>
      <span style="font-size:10px;font-weight:700;letter-spacing:.1em;background:#15803d;color:#fff;padding:3px 10px;border-radius:4px">HOCKEY</span>
      <h3 class="font-display" style="font-size:20px">NHL Money Shots</h3>
      <p style="color:#6b7280;font-size:12px;line-height:1.6">Shots on goal picks with live FanDuel sportsbook lines.</p>
      <a href="/launch/nhl" target="_blank" class="btn" style="width:100%;text-align:center">🎯 OPEN PICKS</a>
    </div>
    <div class="card" style="text-align:center;display:flex;flex-direction:column;gap:14px;align-items:center">
      <div style="font-size:52px">🏀</div>
      <span style="font-size:10px;font-weight:700;letter-spacing:.1em;background:#7e22ce;color:#fff;padding:3px 10px;border-radius:4px">BASKETBALL</span>
      <h3 class="font-display" style="font-size:20px">NBA Money Buckets</h3>
      <p style="color:#6b7280;font-size:12px;line-height:1.6">75%+ hit rate picks for Pts, Reb, Ast, 3PM vs today's opponent.</p>
      <a href="/launch/nba" target="_blank" class="btn" style="width:100%;text-align:center">🎯 OPEN PICKS</a>
    </div>
    <div class="card" style="text-align:center;display:flex;flex-direction:column;gap:14px;align-items:center">
      <div style="font-size:52px">🏈</div>
      <span style="font-size:10px;font-weight:700;letter-spacing:.1em;background:#b45309;color:#fff;padding:3px 10px;border-radius:4px">FOOTBALL</span>
      <h3 class="font-display" style="font-size:20px">NFL Money Bombs</h3>
      <p style="color:#6b7280;font-size:12px;line-height:1.6">Weekly NFL player prop picks with matchup analysis.</p>
      <a href="/launch/nfl" target="_blank" class="btn" style="width:100%;text-align:center">🎯 OPEN PICKS</a>
    </div>
  </div>
</div>
"""

# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home():
    return HOME_HTML

# ── Stripe Checkout ────────────────────────────────────────────────────────────
@app.get("/subscribe")
async def subscribe():
    try:
        if not STRIPE_SECRET:
            return HTMLResponse("<pre style='color:red;padding:40px'>ERROR: STRIPE_SECRET_KEY not set in Render env vars</pre>")
        if not STRIPE_PRICE_ID:
            return HTMLResponse("<pre style='color:red;padding:40px'>ERROR: STRIPE_PRICE_ID not set in Render env vars</pre>")
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
            success_url=f"{SITE_URL}/register?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{SITE_URL}/",
        )
        return RedirectResponse(url=session.url)
    except Exception as e:
        import traceback
        return HTMLResponse(f"<pre style='color:red;font-family:monospace;padding:40px;background:#111'>STRIPE ERROR: {repr(e)}\n\n{traceback.format_exc()}\n\nKey starts with: {STRIPE_SECRET_KEY[:12]}...\nPrice ID: {STRIPE_PRICE_ID}\nSite URL: {SITE_URL}</pre><a href='/' style='color:#f59e0b;padding:40px;display:block'>Go back</a>")

# ── Register (after Stripe payment) ───────────────────────────────────────────
@app.get("/register", response_class=HTMLResponse)
async def register_get(session_id: str = ""):
    if not session_id:
        return RedirectResponse(url="/")
    try:
        session = stripe.checkout.Session.retrieve(session_id)
        # Try multiple ways to get email from Stripe session
        email = (
            getattr(getattr(session, 'customer_details', None), 'email', None) or
            getattr(session, 'customer_email', None) or
            ""
        )
        return REGISTER_HTML.replace("{email}", email).replace("{session_id}", session_id).replace("{error}", "")
    except Exception as e:
        return HTMLResponse(f"<pre style='color:red;padding:40px;font-family:monospace;background:#111'>Register error: {repr(e)}\nSession ID: {session_id}</pre><a href='/' style='color:#f59e0b;padding:40px;display:block'>Go back</a>")

@app.post("/register", response_class=HTMLResponse)
async def register_post(
    email: str = Form(...),
    password: str = Form(...),
    confirm: str = Form(...),
    session_id: str = Form(...)
):
    if password != confirm:
        return REGISTER_HTML.replace("{email}", email).replace("{session_id}", session_id).replace(
            "{error}", '<div class="error-box">❌ Passwords do not match.</div>')

    if len(password) < 6:
        return REGISTER_HTML.replace("{email}", email).replace("{session_id}", session_id).replace(
            "{error}", '<div class="error-box">❌ Password must be at least 6 characters.</div>')

    # Check if account already exists
    existing = db.table("subscribers").select("id").eq("email", email).execute()
    if existing.data:
        return REGISTER_HTML.replace("{email}", email).replace("{session_id}", session_id).replace(
            "{error}", '<div class="error-box">❌ An account with this email already exists. <a href="/login" style="color:#f59e0b">Login here.</a></div>')

    # Get Stripe details
    try:
        session = stripe.checkout.Session.retrieve(session_id)
        customer_id = session.customer
        subscription_id = session.subscription
    except:
        customer_id = ""
        subscription_id = ""

    # Create account in Supabase
    db.table("subscribers").insert({
        "email": email,
        "password_hash": hash_pw(password),
        "stripe_customer_id": customer_id,
        "stripe_subscription_id": subscription_id,
        "is_active": True
    }).execute()

    # Auto-login
    sid = secrets.token_hex(32)
    SESSIONS[sid] = email
    resp = RedirectResponse(url="/dashboard", status_code=302)
    resp.set_cookie("sid", sid, httponly=True, samesite="lax", max_age=60 * 60 * 24 * 30)
    return resp

# ── Login ──────────────────────────────────────────────────────────────────────
@app.get("/login", response_class=HTMLResponse)
async def login_get():
    return LOGIN_HTML.replace("{error}", "")

@app.post("/login", response_class=HTMLResponse)
async def login_post(request: Request, email: str = Form(...), password: str = Form(...)):
    # ── Admin bypass ──────────────────────────────────────────────────────
    if ADMIN_EMAIL and email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
        sid = secrets.token_hex(32)
        SESSIONS[sid] = email
        resp = RedirectResponse(url="/dashboard", status_code=302)
        resp.set_cookie("sid", sid, httponly=True, samesite="lax", max_age=60 * 60 * 24 * 365)  # 1 year
        return resp

    result = db.table("subscribers").select("*").eq("email", email).execute()
    if not result.data:
        return LOGIN_HTML.replace("{error}", '<div class="error-box">❌ Email not found. <a href="/subscribe" style="color:#f59e0b">Subscribe here.</a></div>')

    user = result.data[0]
    if user["password_hash"] != hash_pw(password):
        return LOGIN_HTML.replace("{error}", '<div class="error-box">❌ Incorrect password.</div>')

    if not user.get("is_active"):
        return LOGIN_HTML.replace("{error}", '<div class="error-box">❌ Your subscription is inactive. <a href="/subscribe" style="color:#f59e0b">Renew here.</a></div>')

    # Log this login attempt for IP tracking (skip for admin)
    if email != ADMIN_EMAIL:
        try:
            ip = (request.headers.get("X-Forwarded-For") or (request.client.host if request.client else "unknown")).split(",")[0].strip()
            ua = request.headers.get("User-Agent", "")[:200]
            db.table("login_log").insert({"email": email, "ip": ip, "user_agent": ua}).execute()
            # Check for suspicious activity (5+ unique IPs in last 24h)
            from datetime import datetime, timedelta, timezone
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
            logs = db.table("login_log").select("ip").eq("email", email).gte("logged_at", since).execute()
            unique_ips = len(set(l["ip"] for l in logs.data))
            if unique_ips >= 5:
                db.table("subscribers").update({"notes": f"⚠️ SUSPICIOUS: {unique_ips} IPs in 24h"}).eq("email", email).execute()
        except Exception:
            pass

    sid = secrets.token_hex(32)
    SESSIONS[sid] = email
    resp = RedirectResponse(url="/dashboard", status_code=302)
    resp.set_cookie("sid", sid, httponly=True, samesite="lax", max_age=60 * 60 * 24 * 30)
    return resp

# ── Dashboard ──────────────────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = get_user(request)
    if not user:
        return RedirectResponse(url="/login")
    return DASHBOARD_HTML.replace("{email}", user)

# ── Logout ─────────────────────────────────────────────────────────────────────

# ── App Launch Endpoints (hub → app with JWT) ─────────────────────────────────
APP_URLS = {
    "mlb": "https://moneyball-1.onrender.com",
    "nhl": "https://nhl-shots.onrender.com",
    "nba": "https://nba-money-buckets.onrender.com",
    "nfl": "https://nfl-money-bombs.onrender.com",
}

@app.get("/launch/{sport}")
async def launch_app(sport: str, request: Request):
    user = get_user(request)
    if not user:
        return RedirectResponse(url="/login")
    app_url = APP_URLS.get(sport)
    if not app_url:
        return RedirectResponse(url="/dashboard")
    token = make_app_token(user)
    return RedirectResponse(url=f"{app_url}/?token={token}", status_code=302)

@app.get("/logout")
async def logout(request: Request):
    sid = request.cookies.get("sid")
    if sid and sid in SESSIONS:
        del SESSIONS[sid]
    resp = RedirectResponse(url="/")
    resp.delete_cookie("sid")
    return resp


# ── Admin Dashboard ────────────────────────────────────────────────────────────
def is_admin(request: Request) -> bool:
    sid = request.cookies.get("sid")
    email = SESSIONS.get(sid, "") if sid else ""
    return email == ADMIN_EMAIL and bool(ADMIN_EMAIL)

@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    if not is_admin(request):
        return RedirectResponse(url="/login")

    from datetime import datetime, timedelta, timezone
    from collections import defaultdict

    # Get all subscribers (wrapped in try/except)
    try:
        subs = db.table("subscribers").select("*").execute().data or []
        subs.sort(key=lambda x: x.get("created_at",""), reverse=True)
    except Exception as e:
        return HTMLResponse(f"<h2>DB Error fetching subscribers: {e}</h2>")

    # Get login logs (may not exist yet — handled gracefully)
    all_logs = []
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        all_logs = db.table("login_log").select("email,ip,logged_at").gte("logged_at", since).execute().data or []
    except Exception:
        pass  # login_log table may not exist yet — that's OK

    # Build per-user stats
    from collections import defaultdict
    ip_map = defaultdict(set)
    last_login_map = {}
    for log in all_logs:
        ip_map[log["email"]].add(log["ip"])
        ts = log.get("logged_at","")
        if ts > last_login_map.get(log["email"],""):
            last_login_map[log["email"]] = ts

    rows = ""
    for s in subs:
        em = s["email"]
        active = s.get("is_active", False)
        ips = ip_map.get(em, set())
        ip_count = len(ips)
        last_ip = list(ips)[-1] if ips else "—"
        last_seen = last_login_map.get(em, "—")[:16].replace("T"," ") if last_login_map.get(em) else "—"
        notes = s.get("notes","") or ""
        suspicious = "⚠️" in notes
        status_badge = '<span style="color:#4ade80;font-weight:700">✅ Active</span>' if active else '<span style="color:#f87171;font-weight:700">❌ Inactive</span>'
        sus_badge = '<span style="background:#7f1d1d;color:#fca5a5;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">⚠️ SUSPICIOUS</span>' if suspicious else ""
        ip_color = "#fca5a5" if ip_count >= 5 else "#f59e0b" if ip_count >= 3 else "#4ade80"
        cancel_btn = f'<form method="post" action="/admin/cancel" style="display:inline"><input type="hidden" name="email" value="{em}"><button style="background:#7f1d1d;color:#fca5a5;border:1px solid #991b1b;border-radius:6px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:700">❌ Cancel</button></form>' if active else f'<form method="post" action="/admin/reinstate" style="display:inline"><input type="hidden" name="email" value="{em}"><button style="background:#14532d;color:#86efac;border:1px solid #166534;border-radius:6px;padding:4px 12px;font-size:11px;cursor:pointer;font-weight:700">✅ Reinstate</button></form>'
        rows += f"""<tr style="border-bottom:1px solid #1f2937">
          <td style="padding:12px 14px;color:#e5e7eb;font-size:13px">{em}</td>
          <td style="padding:12px 14px">{status_badge}</td>
          <td style="padding:12px 14px;color:{ip_color};font-weight:700;font-size:13px">{ip_count} IPs {sus_badge}</td>
          <td style="padding:12px 14px;color:#9ca3af;font-size:12px;font-family:monospace">{last_ip}</td>
          <td style="padding:12px 14px;color:#9ca3af;font-size:12px">{last_seen}</td>
          <td style="padding:12px 14px;color:#9ca3af;font-size:11px;max-width:180px">{notes}</td>
          <td style="padding:12px 14px">{cancel_btn}</td>
        </tr>"""

    total = len(subs)
    active_count = sum(1 for s in subs if s.get("is_active"))
    suspicious_count = sum(1 for s in subs if "⚠️" in (s.get("notes","") or ""))

    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>MPA Admin</title>
<style>
  body{{background:#0a0a0a;color:#e5e7eb;font-family:'Segoe UI',sans-serif;padding:32px}}
  h1{{color:#f59e0b;font-size:28px;margin-bottom:4px}}
  .stats{{display:flex;gap:20px;margin:20px 0}}
  .stat{{background:#111;border:1px solid #1f2937;border-radius:10px;padding:16px 24px;text-align:center}}
  .stat .n{{font-size:28px;font-weight:900;color:#f59e0b}}
  .stat .l{{font-size:11px;color:#6b7280;text-transform:uppercase;letter-spacing:1px;margin-top:4px}}
  table{{width:100%;border-collapse:collapse;background:#111;border-radius:10px;overflow:hidden;border:1px solid #1f2937}}
  th{{background:#0a0a0a;padding:10px 14px;text-align:left;color:#f59e0b;font-size:11px;text-transform:uppercase;letter-spacing:1px;white-space:nowrap}}
  tr:hover td{{background:#1a1a1a}}
  .back{{color:#f59e0b;text-decoration:none;font-size:13px;display:inline-block;margin-bottom:20px}}
</style></head><body>
  <a href="/dashboard" class="back">← Back to Dashboard</a>
  <h1>🔐 Money Picks Arena — Admin</h1>
  <p style="color:#6b7280;margin-bottom:20px">Manage subscribers, detect sharing, cancel accounts.</p>
  <div class="stats">
    <div class="stat"><div class="n">{total}</div><div class="l">Total Members</div></div>
    <div class="stat"><div class="n" style="color:#4ade80">{active_count}</div><div class="l">Active</div></div>
    <div class="stat"><div class="n" style="color:#fca5a5">{total-active_count}</div><div class="l">Inactive</div></div>
    <div class="stat"><div class="n" style="color:#fca5a5">{suspicious_count}</div><div class="l">Suspicious ⚠️</div></div>
  </div>
  <table>
    <thead><tr>
      <th>Email</th><th>Status</th><th>Unique IPs (30d)</th>
      <th>Last IP</th><th>Last Seen</th><th>Notes</th><th>Action</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
  <p style="color:#374151;font-size:11px;margin-top:16px">⚠️ = 5+ unique IPs in 24h &nbsp;|&nbsp; 🟡 = 3-4 IPs &nbsp;|&nbsp; 🟢 = 1-2 IPs</p>

  <div style="background:#111;border:1px solid #1f2937;border-radius:10px;padding:24px;margin-top:28px;max-width:480px">
    <h3 style="color:#f59e0b;font-size:16px;margin-bottom:4px">➕ Create User (No Stripe needed)</h3>
    <p style="color:#6b7280;font-size:12px;margin-bottom:16px">Use for test accounts, comped users, or friends.</p>
    <form method="post" action="/admin/create-user">
      <div style="margin-bottom:12px">
        <label style="display:block;color:#9ca3af;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Email</label>
        <input name="email" type="email" required placeholder="user@example.com"
               style="width:100%;background:#0a0a0a;border:1px solid #374151;border-radius:8px;padding:10px 14px;color:#fff;font-size:13px;outline:none;box-sizing:border-box">
      </div>
      <div style="margin-bottom:12px">
        <label style="display:block;color:#9ca3af;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Password</label>
        <input name="password" type="text" required placeholder="Choose a password for them"
               style="width:100%;background:#0a0a0a;border:1px solid #374151;border-radius:8px;padding:10px 14px;color:#fff;font-size:13px;outline:none;box-sizing:border-box">
      </div>
      <div style="margin-bottom:16px">
        <label style="display:block;color:#9ca3af;font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Notes (optional)</label>
        <input name="notes" type="text" placeholder="e.g. Test user - John"
               style="width:100%;background:#0a0a0a;border:1px solid #374151;border-radius:8px;padding:10px 14px;color:#fff;font-size:13px;outline:none;box-sizing:border-box">
      </div>
      <button type="submit"
              style="background:linear-gradient(135deg,#f59e0b,#d97706);color:#000;border:none;border-radius:8px;padding:12px 28px;font-size:13px;font-weight:900;cursor:pointer;width:100%">
        ➕ Create User
      </button>
    </form>
  </div>
</body></html>"""
    return HTMLResponse(html)


@app.post("/admin/cancel")
async def admin_cancel(request: Request, email: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/login")
    if email == ADMIN_EMAIL:  # Never cancel the master account
        return RedirectResponse(url="/admin", status_code=302)
    db.table("subscribers").update({
        "is_active": False,
        "notes": (db.table("subscribers").select("notes").eq("email",email).execute().data or [{}])[0].get("notes","") + " | CANCELLED BY ADMIN"
    }).eq("email", email).execute()
    # Cancel Stripe subscription if exists
    try:
        sub = db.table("subscribers").select("stripe_subscription_id").eq("email",email).execute().data
        if sub and sub[0].get("stripe_subscription_id"):
            stripe.Subscription.cancel(sub[0]["stripe_subscription_id"])
    except Exception:
        pass
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/reinstate")
async def admin_reinstate(request: Request, email: str = Form(...)):
    if not is_admin(request):
        return RedirectResponse(url="/login")
    db.table("subscribers").update({"is_active": True}).eq("email", email).execute()
    return RedirectResponse(url="/admin", status_code=302)


@app.post("/admin/create-user")
async def admin_create_user(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    notes: str = Form("")
):
    if not is_admin(request):
        return RedirectResponse(url="/login")
    try:
        # Check if user already exists
        existing = db.table("subscribers").select("id").eq("email", email).execute().data
        if existing:
            return HTMLResponse(f"""<html><body style="background:#0a0a0a;color:#f87171;font-family:sans-serif;padding:40px">
                <h2>❌ User already exists: {email}</h2>
                <a href="/admin" style="color:#f59e0b">← Back to Admin</a>
            </body></html>""")
        # Create the user
        db.table("subscribers").insert({
            "email":         email,
            "password_hash": hash_pw(password),
            "is_active":     True,
            "notes":         notes or "Created by admin (no Stripe)"
        }).execute()
        return HTMLResponse(f"""<html><body style="background:#0a0a0a;color:#4ade80;font-family:sans-serif;padding:40px">
            <h2>✅ User created successfully!</h2>
            <p style="color:#9ca3af;margin:12px 0">Email: <strong style="color:#fff">{email}</strong></p>
            <p style="color:#9ca3af;margin:12px 0">Password: <strong style="color:#fff">{password}</strong></p>
            <p style="color:#6b7280;font-size:13px;margin-top:20px">Share these credentials with your test user. They can log in at your hub URL.</p>
            <a href="/admin" style="color:#f59e0b;display:inline-block;margin-top:20px">← Back to Admin</a>
        </body></html>""")
    except Exception as e:
        return HTMLResponse(f"""<html><body style="background:#0a0a0a;color:#f87171;font-family:sans-serif;padding:40px">
            <h2>❌ Error: {e}</h2>
            <a href="/admin" style="color:#f59e0b">← Back to Admin</a>
        </body></html>""")

# ── Stripe Webhook ─────────────────────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    sig = request.headers.get("stripe-signature", "")
    try:
        event = stripe.Webhook.construct_event(body, sig, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    if event["type"] == "customer.subscription.updated":
        sub = event["data"]["object"]
        is_active = sub["status"] == "active"
        db.table("subscribers").update({"is_active": is_active}).eq("stripe_subscription_id", sub["id"]).execute()

    elif event["type"] == "customer.subscription.deleted":
        sub = event["data"]["object"]
        db.table("subscribers").update({"is_active": False}).eq("stripe_subscription_id", sub["id"]).execute()

    return JSONResponse({"received": True})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
