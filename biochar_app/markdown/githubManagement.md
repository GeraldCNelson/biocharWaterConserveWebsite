# GitHub Management for Precompute Script and Project

## 1. Set Up `.gitignore`
Create a `.gitignore` file in the root of your project with the following content:

# Python caches
__pycache__/
*.pyc

# Logs
*.log

# Data files
flask/data-raw/
flask/data-processed/

# Environment files
.venv/

This ensures only necessary files are tracked by Git.

---

## 2. Create a New Branch
Always use a separate branch for development and testing. Merge it into the `main` branch after verifying the changes.

### Steps:
# Create and switch to a new branch
git checkout -b precompute-enhancements

# Stage the updated script and commit
git add flask/precompute_calculations.py
git commit -m "Enhance precompute script with summary and fixes"

# Push the branch to the remote repository
git push origin precompute-enhancements

---

## 3. Test Before Merging
After pushing the branch, thoroughly test the script locally and in the Flask app.

1. Go to your GitHub repository.
2. Create a pull request from `precompute-enhancements` to the `main` branch.
3. Review the changes and confirm functionality.
4. Merge the pull request after testing.

---

## 4. Use Git Tags for Milestones
Tag your repository for important milestones, such as the completion of this script.

### Steps:
# Tag the current commit with a version number and message
git tag -a v1.0 -m "Precompute script completed and tested"

# Push the tag to the remote repository
git push origin v1.0

---

## 5. Document Changes in `README.md`
Update your repository documentation with details about the precompute script.

### Example for `README.md`:
## Precompute Script

The precompute script processes raw data into structured outputs for the Flask app. It performs the following steps:
1. Parses and renames columns in the raw data files.
2. Calculates Soil Water Content (SWC) values.
3. Computes ratios for variables.
4. Aggregates 15-minute data to daily granularity.

### How to Run
1. Place raw data files in `flask/data-raw/`.
2. Run the script:
   python flask/precompute_calculations.py
3. Processed data will be saved in `flask/data-processed/`.

### Milestones
- **v1.0**: Precompute script completed and tested.

---

## Summary
1. **Set up `.gitignore`** to exclude unnecessary files.
2. **Create and test a new branch** for updates.
3. Use **Git tags** to mark significant milestones.
4. **Update `README.md`** to document the project and usage instructions.