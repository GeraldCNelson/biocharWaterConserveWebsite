import os
from flask import Flask, url_for
from routes import main  # Import the blueprint from routes.py
from precompute_calculations import precompute_data  # Import the precompute function

# Initialize the Flask app
app = Flask(__name__, static_folder="../static")

# Automatically run pre-computation on startup
data_dir = os.path.join(os.getcwd(), "data")
if not any(fname.endswith(".csv") for fname in os.listdir(data_dir)):
    precompute_data()

# Register the blueprint
app.register_blueprint(main, url_prefix='/')

if __name__ == "__main__":
    app.run(debug=True)
