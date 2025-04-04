import os
from flask import Flask
from biochar_app.routes import main, load_logger_data
#from biochar_app.precompute_calculations import process_all_datasets
import logging
import subprocess

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "biochar_app.log"))
        #logging.StreamHandler()  # Optional: keep printing to terminal too
    ]
)

# ✅ Initialize the Flask app with correct paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Get current directory of app.py
app = Flask(__name__,
            static_folder=os.path.join(BASE_DIR, "static"),  # ✅ Now correctly points to flask/static
            template_folder=os.path.join(BASE_DIR, "templates"))  # ✅ Now correctly points to flask/templates

# ✅ Ensure data directory exists
data_dir = os.path.join(BASE_DIR, "data-processed")
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# ✅ Automatically run pre-computation on startup (if needed)
try:
    if not any(fname.endswith(".zip") for fname in os.listdir(data_dir)):
        print(f"No ZIP files found in {data_dir}. Running process_data_2023.py and process_data.py...")

        subprocess.run(["python", os.path.join(BASE_DIR, "process_data_2023.py")], check=True)
        subprocess.run(["python", os.path.join(BASE_DIR, "process_data.py")], check=True)

except Exception as e:
    print(f"❌ Error during pre-computation: {e}. Continuing without precomputed data.")

# ✅ Register the Blueprint
logging.info("Starting app...")
app.register_blueprint(main, url_prefix='/')

# ✅ Print registered routes
if app.url_map.iter_rules():
    logging.info("Registered routes:")
    for rule in app.url_map.iter_rules():
        logging.info(rule)
else:
    print("⚠️ No routes registered. Check Blueprint registration.")# ✅ Preload static logger datasets (2023 & 2024)

# Preload static datasets into memory (2023, 2024 only)
# This avoids repeated ZIP file loading during requests

for preload_year in [2023, 2024]:
    for granularity in ["15min", "1hour", "daily", "monthly"]: #, "growingseason"
        try:
            load_logger_data(preload_year, granularity)
            logging.info(f"✅ Preloaded {preload_year}-{granularity}")
        except Exception as e:
            logging.warning(f"⚠️ Could not preload {preload_year}-{granularity}: {e}")



# ✅ Run Flask
if __name__ == "__main__":
    if app.jinja_loader and hasattr(app.jinja_loader, "searchpath"):
        logging.info(f"Flask is searching for templates in: {app.jinja_loader.searchpath}")
    else:
        logging.info("⚠️ Warning: jinja_loader is not properly initialized.")
    app.run(debug=False, port=5000)
