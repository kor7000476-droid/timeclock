# Synology Deployment (time.6788choi.synology.me)

This app can run on Synology without Docker (Python venv) or via Container Manager (Docker), and be exposed through DSM Reverse Proxy.

## 1) Copy Project To NAS

Recommended NAS folder:
- `/volume1/TLJ-Data/timeclock-mvp` (works without special permissions)
- Or: `/volume1/docker/timeclock-mvp` (if you already use that folder)

Files to copy:
- `Dockerfile`
- `docker-compose.yml`
- `requirements.txt`
- `app/`
- `sql/`
- `bin/` (start/stop scripts)

## 2) Prepare Persistent Data Folder

Create:
- `<project>/data` (example: `/volume1/TLJ-Data/timeclock-mvp/data`)

This will store:
- `timeclock.db` (SQLite)
- `backup` (overwritten daily at 03:00 ET)

## 3) Run The App

### Option A: Run Without Docker (recommended if Container Manager is not installed)

1) Install Synology Python package:
- DSM Package Center -> install `Python3.9` (or newer)

2) Create venv + install deps (SSH):
```sh
cd /volume1/TLJ-Data/timeclock-mvp
/volume1/@appstore/Python3.9/usr/bin/python3.9 -m venv .venv
./.venv/bin/pip install -r requirements.txt
```

3) Create `.env` on NAS (example):
```sh
cd /volume1/TLJ-Data/timeclock-mvp
cat > .env <<'EOF'
APP_NAME=Timeclock_MVP
DATABASE_URL=sqlite:////volume1/TLJ-Data/timeclock-mvp/data/timeclock.db
DATA_DIR=/volume1/TLJ-Data/timeclock-mvp/data
BACKUP_FILENAME=backup
KIOSK_DEVICE_ID=ipad-kiosk-1
ADMIN_PIN=onse0083
EOF
chmod 600 .env
```

4) Start the service (SSH):
```sh
/volume1/TLJ-Data/timeclock-mvp/bin/timeclock_start.sh
```

It listens on:
- NAS: `http://127.0.0.1:8010` (recommended: local-only; use Reverse Proxy for HTTPS)

To run it automatically at boot:
- DSM -> Control Panel -> Task Scheduler -> Create -> Triggered Task -> User-defined script
- User: `root`
- Event: `Boot-up`
- Command: `/volume1/TLJ-Data/timeclock-mvp/bin/timeclock_start.sh`

### Option B: Container Manager (Docker)

Option A: Container Manager -> Project -> Create
- Project folder: `/volume1/docker/timeclock-mvp`
- Compose file: `docker-compose.yml`
- Start the project

The container will listen on:
- NAS: `http://127.0.0.1:18010` (mapped from container port 8010)

## 4) DSM Reverse Proxy (HTTPS)

DSM -> Login Portal -> Advanced -> Reverse Proxy -> Create:
- Source:
  - Protocol: HTTPS
  - Hostname: `time.6788choi.synology.me`
  - Port: 443
- Destination:
  - Protocol: HTTP
  - Hostname: `127.0.0.1`
  - Port: 8010

Also attach a valid certificate for `time.6788choi.synology.me` in DSM certificate settings.

## 5) Environment Variables

If using Docker: edit `docker-compose.yml` environment values on NAS.
If using venv: edit `.env` on NAS.

Important:
- `ADMIN_PIN` (required)
- `SMTP_*` + `MAIL_FROM` (optional, required for email send features)

## 6) Smoke Test

From your laptop:
- `https://time.6788choi.synology.me/api/health`

Expected:
- `{"status":"ok","service":"Timeclock MVP"}`
