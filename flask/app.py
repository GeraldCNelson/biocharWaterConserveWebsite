import os
from flask import Flask, url_for
from routes import main  # Import the blueprint from routes.py
from precompute_calculations import process_all_datasets
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize the Flask app
app = Flask(__name__)

# Automatically run pre-computation on startup
try:
    data_dir = os.path.join(os.getcwd(), "flask/data-processed")
    if not any(fname.endswith(".zip") for fname in os.listdir(data_dir)):
        print(f"No ZIP files found in {data_dir}. Running precompute_data...")
        process_all_datasets()
except Exception as e:
    print(f"Error during pre-computation: {e}. Continuing without precomputed data.")

# Register the blueprint
print("Starting app...")
print("Registering blueprint 'main'...")
app.register_blueprint(main, url_prefix='/')
print("Registered routes:")
for rule in app.url_map.iter_rules():
    print(rule)
 #   print("Blueprint 'main' registered!")

if __name__ == "__main__":
    app.run(debug=False, port=8000)
