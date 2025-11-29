import json
from datetime import datetime, timezone
import csv

# 1. Load the JSON (either paste it directly into this dict, or save it as 'data.json' and load it)
with open('data.json') as f:
    data = json.load(f)

# 2. Prepare CSV
output_path = 'purple_air.csv'
with open(output_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    # write header
    writer.writerow(['date', 'id', 'sensor_index', 'created'])

    # 3. Iterate and convert
    for m in data['members']:
        # created (seconds since 1970-01-01) → timezone-aware ISO date (UTC)
        dt = datetime.fromtimestamp(m['created'], timezone.utc)
        date_iso = dt.isoformat().replace('+00:00', 'Z')
        writer.writerow([
            date_iso,
            m['id'],
            m['sensor_index'],
            m['created']
        ])

print(f"Written {len(data['members'])} rows to {output_path}")