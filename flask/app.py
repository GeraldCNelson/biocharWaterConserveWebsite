import os
from flask import Flask, url_for
from routes import main  # Import the blueprint from routes.py
from precompute_calculations import process_all_datasets
import logging
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize the Flask app
app = Flask(__name__)

# ✅ Ensure data directory exists
data_dir = os.path.join(os.getcwd(), "flask/data-processed")
if not os.path.exists(data_dir):
    os.makedirs(data_dir)  # ✅ Prevents errors when checking files

# ✅ Automatically run pre-computation on startup (if needed)
try:
    if not any(fname.endswith(".zip") for fname in os.listdir(data_dir)):
        print(f"No ZIP files found in {data_dir}. Running process_data_2023.py and process_data.py...")

        # ✅ Use os.path.join() to avoid missing files due to working directory
        subprocess.run(["python", os.path.join("flask", "process_data_2023.py")], check=True)
        subprocess.run(["python", os.path.join("flask", "process_data.py")], check=True)

except Exception as e:
    print(f"❌ Error during pre-computation: {e}. Continuing without precomputed data.")

# ✅ Register the Blueprint
print("Starting app...")
print("Registering blueprint 'main'...")
app.register_blueprint(main, url_prefix='/')

# ✅ Print registered routes safely
if app.url_map.iter_rules():
    print("Registered routes:")
    for rule in app.url_map.iter_rules():
        print(rule)
else:
    print("⚠️ No routes registered. Check Blueprint registration.")

# ✅ Run Flask
if __name__ == "__main__":
    app.run(debug=False, port=8000)