NHL MONEY SHOTS — Final Backup
================================
GitHub: higgiAPPMASTER/NHL-SHOTS

ENV VARS (Render):
  USERS              yourusername:yourpassword
  STATMUSE_EMAIL     StatMuse email
  STATMUSE_PASSWORD  StatMuse password
  ODDS_API_KEY       from the-odds-api.com
  FD_EMAIL           FanDuel email (optional)
  FD_PASSWORD        FanDuel password (optional)
  PLAYWRIGHT_BROWSERS_PATH  /opt/render/project/.browsers

BUILD COMMAND:
  pip install --no-cache-dir -r requirements.txt && playwright install chromium

START COMMAND:
  uvicorn main:app --host 0.0.0.0 --port $PORT

ALGORITHM:
  Step 1: Odds API shot lines -> player pool
  Step 2: StatMuse career H/A vs opponent (80% hit rate)
  Step 3: StatMuse last 10 H/A games (80% hit rate)
  Step 4: Top 10 + Points picks section
