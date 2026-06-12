# Deploying Updates to the Biochar Website

This document describes the standard workflow for moving approved code and data from development into production.

Development is normally performed using PyCharm, but deployment itself uses Git, SSH, rsync, and standard Linux command-line tools.

---

# Website URLs

## Test site

Initial destination for testing new code and data:

- https://test.biocharresearch.org

## Production site

Public-facing website:

- https://biocharresearch.org

---

# Hosting Architecture

The Biochar website is hosted on Amazon Lightsail.

Current servers:

- biochar-test-fetch
  - Test environment
  - Runs the `etl-refactor` branch
  - URL: https://test.biocharresearch.org

- biochar-webserver
  - Production environment
  - Runs the `main` branch
  - URL: https://biocharresearch.org

Both servers run Ubuntu Linux and are accessed through SSH aliases defined in:

```bash
~/.ssh/config

# Current Python Virtual Environments

A Python virtual environment is an isolated folder that contains its own Python interpreter and installed packages. It allows project-specific dependencies to be managed independently of the operating system Python installation.

Current locations:

### Local development computer

```text
~/.biochar_py313
```

### Test server

```text
~/.biochar_py313
```

### Production server

```text
~/biocharWaterConserveWebsite/venv
```

Commands in the production sections below assume the production venv path.

## Future goal

Standardize both servers on:

```text
~/biocharWaterConserveWebsite/venv
```

---

# Key Idea

Deployment consists of two separate activities.

## 1. Git updates

Git manages:

- source code
- configuration files
- templates
- documentation

GitHub repository:

```text
https://github.com/GeraldCNelson/biocharWaterConserveWebsite
```

The production branch is:

```text
main
```

## 2. Data updates

Many generated data products are not reliably managed through Git.

Examples include:

- parquet files
- download packages
- lab-test outputs
- irrigation outputs

These files are transferred using:

```bash
rsync
```

A `git pull` only updates files tracked by Git. It does not automatically update generated data files.

---

# GitHub Branch Workflow

Normal development occurs on:

```text
etl-refactor
```

The test website runs from:

```text
etl-refactor
```

The production website runs from:

```text
main
```

## Typical Workflow

1. Make code and data-processing changes locally on the `etl-refactor` branch.
2. Commit periodically and push to GitHub.
3. Transfer updated data files to the test server using `rsync`.
4. Verify behavior locally.
5. Verify behavior on https://test.biocharresearch.org.
6. Run Playwright smoke tests locally and on the test server.
7. When everything works correctly on the test server, merge `etl-refactor` into `main`.
8. Push `main` to GitHub.
9. Pull `main` on the production server.
10. Transfer approved data from the test server to the production server.
11. Restart the production website.
12. Verify https://biocharresearch.org.

---

# 1. Local Checks Before Deployment

Run from the local project root:

```bash
git status

python biochar_app/tests/playwright_smoke.py
```

Expected result:

```text
✅ Playwright smoke test completed
```

If the smoke test passes:

```bash
git add <changed files>

git commit -m "Brief description"

git push origin etl-refactor
```

---

# 2. Test Server Validation

Update the test server:

```bash
ssh biochar-test-fetch

cd ~/biocharWaterConserveWebsite

git checkout etl-refactor

git pull origin etl-refactor

source ~/.biochar_py313/bin/activate

python biochar_app/tests/playwright_smoke.py
```

Expected result:

```text
✅ Playwright smoke test completed
```

Verify that the test website behaves correctly:

- interactive plots
- downloads
- soil chemistry
- soil biology
- management data
- irrigation overlays
- any newly added functionality

---

# 3. Promote etl-refactor to main

Run locally:

```bash
git checkout main

git pull origin main

git merge etl-refactor

python biochar_app/tests/playwright_smoke.py

git push origin main

git checkout etl-refactor
```

---

# 4. Transfer Approved Data to Production

After the test server has been verified, it becomes the approved source for production data.

Run these commands from the local computer.

## Downloads

```bash
rsync -av --exclude='.DS_Store' \
  biochar-test-fetch:~/biocharWaterConserveWebsite/biochar_app/data-processed/downloads/ \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/downloads/
```

## Parquet files

```bash
rsync -av --exclude='.DS_Store' \
  biochar-test-fetch:~/biocharWaterConserveWebsite/biochar_app/data-processed/parquet/ \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/parquet/
```

## Lab-test files

```bash
rsync -av --exclude='.DS_Store' \
  biochar-test-fetch:~/biocharWaterConserveWebsite/biochar_app/data-processed/lab-tests/ \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/lab-tests/
```

## Irrigation clean file

```bash
rsync -av --exclude='.DS_Store' \
  biochar-test-fetch:~/biocharWaterConserveWebsite/biochar_app/data-processed/management/irrigation/irrigation_clean.csv \
  biochar-webserver:~/biocharWaterConserveWebsite/biochar_app/data-processed/management/irrigation/
```

---

# 5. SSH to the Production Server

```bash
ssh biochar-webserver
```

SSH aliases are defined in:

```bash
~/.ssh/config
```

To edit:

```bash
nano ~/.ssh/config
```

---

# 6. Pull Code on the Production Server

Run on the production server:

```bash
cd ~/biocharWaterConserveWebsite

git status

git pull origin main
```

---

# 7. Update the Production Python Environment

Activate the project virtual environment:

```bash
cd ~/biocharWaterConserveWebsite

source venv/bin/activate
```

Verify:

```bash
which python

which pip
```

Expected:

```text
/home/ubuntu/biocharWaterConserveWebsite/venv/bin/python
/home/ubuntu/biocharWaterConserveWebsite/venv/bin/pip
```

Install any updated requirements:

```bash
pip install -r requirements.txt
```

Important:

Do not use system-wide `pip install` commands outside the virtual environment.

Ubuntu may return:

```text
externally-managed-environment
```

if the virtual environment is not activated.

---

# 8. Restart the Production Website

```bash
sudo systemctl restart biochar

sudo systemctl status biochar
```

Confirm that the service status is:

```text
active (running)
```

---

# 9. Troubleshooting 502 Bad Gateway Errors

Check application logs:

```bash
sudo journalctl -u biochar -n 80 --no-pager
```

Check nginx logs:

```bash
sudo tail -n 80 /var/log/nginx/error.log
```

Common causes:

- missing Python package
- import error
- missing data file
- Gunicorn worker crash
- incorrect virtual environment

Example:

```bash
cd ~/biocharWaterConserveWebsite

source venv/bin/activate

pip install python-multipart

sudo systemctl restart biochar
```

---

# 10. Server Maintenance

Update operating-system packages periodically:

```bash
sudo apt update

sudo apt upgrade -y
```

Check whether a reboot is required:

```bash
[ -f /var/run/reboot-required ] && echo "Reboot required"
```

Reboot if necessary:

```bash
sudo reboot
```

---

# 11. Optional Log Cleanup

Clear nginx error logs before testing:

```bash
sudo truncate -s 0 /var/log/nginx/error.log
```

---

# 12. Post-Deployment Verification

Open:

- https://biocharresearch.org

Verify:

- home page loads
- interactive plots load
- bulk downloads work
- soil chemistry tab loads
- soil biology tab loads
- irrigation data displays correctly
- no obvious layout problems
- no 502 errors

Useful server checks:

```bash
sudo systemctl status biochar

sudo tail -n 100 /var/log/nginx/error.log
```

---

# Notes

Playwright smoke tests currently run:

- locally
- on the test server

Playwright is not currently installed on the production server.

These instructions assume:

- macOS or Linux
- Git installed
- SSH configured
- rsync available

Windows users can run the same workflow using:

- Git Bash
- WSL (Windows Subsystem for Linux)

Most local development for this project is performed using PyCharm.