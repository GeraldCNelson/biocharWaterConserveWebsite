# Biochar Website Deployment Checklist (Test → Production)

## Overview
Production = `biochar-webserver`  
Test = `biochar-test-fetch`  
Code = GitHub (`main` ← `etl-refactor`)  
Data = rsync (parquet + downloads)

---

## 1. Prepare Code Locally

    cd ~/Documents/workspace/biocharWaterConserveWebsite

    git checkout etl-refactor
    git pull origin etl-refactor

    git add .
    git commit -m "Prepare production release"
    git push origin etl-refactor

---

## 2. Promote to Production Branch

    git checkout main
    git pull origin main
    git merge etl-refactor
    git push origin main

---

## 3. Update Production Server

    ssh biochar-webserver

    cd /home/ubuntu/biocharWaterConserveWebsite
    git checkout main
    git pull origin main

---

## 4. Sync Large Data (LOCAL → SERVER)

IMPORTANT: Use absolute paths. Do not use `~` in the rsync destination.

    rsync -av \
      biochar_app/data-processed/parquet/ \
      biochar-webserver:/home/ubuntu/biocharWaterConserveWebsite/biochar_app/data-processed/parquet/

    rsync -av \
      biochar_app/data-processed/downloads/ \
      biochar-webserver:/home/ubuntu/biocharWaterConserveWebsite/biochar_app/data-processed/downloads/

---

## 4a. Verify Data Sync (CRITICAL STEP)

    ssh biochar-webserver

    cd /home/ubuntu/biocharWaterConserveWebsite

    find biochar_app/data-processed/parquet/summary/daily -type f | grep 2025

Expected output includes:

    .../summary/daily/2025_daily.parquet

---

## 4b. Check for a BAD `~` DIRECTORY

    ls biochar_app/data-processed/parquet/

If you see a literal directory named `~`, fix it:

    mv biochar_app/data-processed/parquet/~ \
       biochar_app/data-processed/parquet/_bad_tilde_backup

Test the site, then remove it:

    rm -rf biochar_app/data-processed/parquet/_bad_tilde_backup

---

## 5. Activate Environment + Install Dependencies

    cd /home/ubuntu/biocharWaterConserveWebsite
    source venv/bin/activate

    pip install -r requirements.txt

---

## 6. Regenerate Derived Files

    python biochar_app/scripts/convert_word_to_html.py
    python biochar_app/scripts/convert_ward_docx_to_html.py

---

## 7. Restart Application

    sudo systemctl daemon-reload
    sudo systemctl restart biochar
    sudo systemctl status biochar

Expected:

    Active: active (running)

---

## 8. Verify Production Site

Open:

    https://biocharresearch.org/

Hard refresh:

    Cmd + Shift + R

Check:
- Intro tab loads
- Links work and dropdown closes
- Ward images render
- Tables look correct
- Plots load
- Summary tab works
- Lab tabs work
- Downloads work

---

## Common Failure Modes

### 500 Internal Server Error

    sudo journalctl -u biochar -n 100 --no-pager

### Missing parquet file

Error:

    No summary raw file for granularity 'daily'

Fix by re-running rsync, then verify:

    find biochar_app/data-processed/parquet/summary/daily -type f

### Bad `~` directory

Symptom:
- Files exist but wrong path
- App cannot find them

Fix:

    mv parquet/~ parquet/_bad_tilde_backup

### Module not found

Error:

    ModuleNotFoundError

Fix:

    pip install -r requirements.txt

### App won’t start

    python -c "from biochar_app.scripts.wsgi import app; print(app)"

---

## Required Config

### wsgi.py

    from biochar_app.scripts.app import app

    if __name__ == "__main__":
        app.run()

### systemd service

    ExecStart=/home/ubuntu/biocharWaterConserveWebsite/venv/bin/gunicorn \
      --workers 3 \
      --worker-class uvicorn.workers.UvicornWorker \
      --bind 127.0.0.1:8000 \
      biochar_app.scripts.wsgi:app

---

## Rollback

    cd /home/ubuntu/biocharWaterConserveWebsite
    git log --oneline -n 5
    git checkout <last-good-commit>

    sudo systemctl restart biochar

---

## Future Improvements
- Add deploy.sh script
- Automate rsync + restart
- Add dataset validation checks

---

## Key Lesson
Always use absolute paths in rsync.  
Never rely on `~` across SSH; it can silently create bad directories.
