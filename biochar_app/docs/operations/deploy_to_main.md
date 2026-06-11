# Deploying Updates to the Biochar Website

Test site:

- https://test.biocharresearch.org

Main site:

- https://biocharresearch.org


## Key idea

Deployment has two parts:

1. Git updates for tracked code/config/templates/docs.
2. `rsync` updates for data files that are not reliably managed through Git.

A `git pull` on the server will not automatically bring every locally generated or untracked data file.

## Github branch workflow

Normal development should happen on the `etl-refactor` branch used by https://test.biocharresearch.org.

The `main` branch represents the production website state used by `https://biocharresearch.org`.

Typical workflow:

1. Make code and data-processing changes on `etl-refactor`.
2. Test locally.
3. Deploy/test on `test.biocharresearch.org` when needed.
4. When everything is working, merge `etl-refactor` into `main`.
5. Push `main`.
6. Pull `main` on the production server.

## 1. Local checks before deployment

Run from the local project root on the Mac:

```bash
git status
python biochar_app/tests/playwright_smoke.py
```

If the smoke test passes (✅ Playwright smoke test completed), push `main`:

```bash
git push origin main
```

## 2. Copy required data files to the production server

Run from the local project root on the Mac.

Downloads:

```bash
rsync -av --exclude='.DS_Store' \
  biochar_app/data-processed/downloads/ \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/downloads/
```

Parquet files:

```bash
rsync -av --exclude='.DS_Store' \
  biochar_app/data-processed/parquet/ \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/parquet/
```

Lab-test files:

```bash
rsync -av --exclude='.DS_Store' \
  biochar_app/data-processed/lab-tests/ \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/lab-tests/
```

Irrigation clean file:

```bash
rsync -av --exclude='.DS_Store' \
  biochar_app/data-processed/management/irrigation/irrigation_clean.csv \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/management/irrigation/
```

## 3. SSH to the production server

Run from the local Mac:

```bash
ssh biochar-webserver
```

Server aliases such as `biochar-webserver` are defined locally in:

```bash
~/.ssh/config
```

To edit:

```bash
nano ~/.ssh/config
```

## 4. Pull code on the production server

Run on the server:

```bash
cd ~/biocharWaterConserveWebsite
git status
git pull origin main
```

If `git pull` fails because local files would be overwritten, do not force anything immediately. First preserve local server files:

```bash
git stash push -u -m "server-local-files-before-main-pull"
```

If obsolete CR1000 test files block the pull, remove them:

```bash
rm -f tests/test_1_pakbus.py tests/test_2_utils.py tests/test_3_device.py
git pull origin main
```

## 5. Update the server Python environment

The app runs inside the project virtual environment:

```bash
cd ~/biocharWaterConserveWebsite
source venv/bin/activate
```

Verify:

```bash
which python
which pip
```

Expected paths should start with:

```text
/home/ubuntu/biocharWaterConserveWebsite/venv/bin/
```

Install or update Python packages inside the venv:

```bash
pip install -r requirements.txt
```

Important: do not use system `pip install` outside the venv. Ubuntu may block it with an `externally-managed-environment` error.

## 6. Restart the website service

Run on the server:

```bash
sudo systemctl restart biochar
sudo systemctl status biochar
```

## 7. If the site shows 502 Bad Gateway

Check the app logs:

```bash
sudo journalctl -u biochar -n 80 --no-pager
```

Check nginx logs:

```bash
sudo tail -n 80 /var/log/nginx/error.log
```

Common causes:

- missing Python package in `venv`
- app import error
- missing data file
- Gunicorn worker crash

Example fix for missing multipart support:

```bash
cd ~/biocharWaterConserveWebsite
source venv/bin/activate
pip install python-multipart
sudo systemctl restart biochar
```

## 8. Server maintenance

Periodic maintenance:

```bash
sudo apt update
sudo apt upgrade -y
```

Only reboot if required:

```bash
[ -f /var/run/reboot-required ] && echo "Reboot required"
```

If reboot is required:

```bash
sudo reboot
```

## 9. Optional log cleanup

Clear nginx error log before testing:

```bash
sudo truncate -s 0 /var/log/nginx/error.log
```

## 10. Post-deployment checks

Open:

- https://biocharresearch.org

Check:

- Home page loads
- Interactive plots load
- Bulk downloads work
- Soil chemistry tab loads
- Soil biology tab loads
- No obvious layout problems
- No 502 errors

Useful server checks:

```bash
sudo systemctl status biochar
sudo tail -n 100 /var/log/nginx/error.log
```

## Notes

Playwright smoke tests are currently run locally on the Mac before deployment.

Playwright is not currently installed on the production server.