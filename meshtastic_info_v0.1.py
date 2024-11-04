import json
import os
import subprocess
import time
from influxdb import InfluxDBClient

# Environment variables
INFLUXDB_HOST = os.getenv('INFLUXDB_HOST')
INFLUXDB_PORT = int(os.getenv('INFLUXDB_PORT', 8086))
INFLUXDB_USER = os.getenv('INFLUXDB_USER')
INFLUXDB_PASSWORD = os.getenv('INFLUXDB_PASSWORD')
INFLUXDB_DB = os.getenv('INFLUXDB_DB')
MESH_NODE_HOST = os.getenv('MESH_NODE_HOST')
TIME_OFFSET = int(os.getenv('TIME_OFFSET', 60))  # in seconds

cur_time = time.time()

# Initialize InfluxDB client
try:
    client = InfluxDBClient(
        host=INFLUXDB_HOST, port=INFLUXDB_PORT,
        username=INFLUXDB_USER, password=INFLUXDB_PASSWORD,
        database=INFLUXDB_DB
    )
except Exception as e:
    print(f"Failed to connect to InfluxDB: {e}")
    exit(1)

# Command to fetch node information
cmd = ['./python/bin/meshtastic', '--host', MESH_NODE_HOST, '--info']
try:
    result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
    result_text = result.stdout.decode('utf-8')
except subprocess.CalledProcessError as e:
    print(f"Command execution failed: {e}")
    exit(1)

# Extract JSON chunk from the command output
start_pos = result_text.find('Nodes in mesh: ') + len('Nodes in mesh: ')
end_pos = result_text.find('Preferences:')
json_chunk = result_text[start_pos:end_pos].replace("\\r", "").replace("\\n", "")

# Parse JSON
try:
    parsed_json = json.loads(json_chunk)
except json.JSONDecodeError:
    print("Error parsing JSON data.")
    exit(1)

print("Appending Nodes for DB upload:")
data = []

# Process each node and prepare data for InfluxDB
for key, value in parsed_json.items():
    lastHeard = value.get("lastHeard", 0)

    # Skip nodes that haven't been heard from recently
    if lastHeard <= cur_time - TIME_OFFSET:
        continue

    # Prepare InfluxDB line protocol data
    fields = {
        "batteryLevel": value.get("deviceMetrics", {}).get("batteryLevel"),
        "voltage": value.get("deviceMetrics", {}).get("voltage"),
        "channelUtilization": value.get("deviceMetrics", {}).get("channelUtilization"),
        "airUtilTx": value.get("deviceMetrics", {}).get("airUtilTx"),
        "uptime": value.get("deviceMetrics", {}).get("uptimeSeconds"),
        "snr": value.get("snr")
    }
    fields = {k: v for k, v in fields.items() if v is not None}  # Remove None values

    shortName = value.get("user", {}).get("shortName", "")
    timestamp_ns = f"{lastHeard}000000000"  # Convert to nanoseconds

    field_set = ",".join([f"{k}={v}" for k, v in fields.items()])
    line_protocol = f"nodeinfo,shortName={shortName} {field_set} {timestamp_ns}"

    print(line_protocol)
    data.append(line_protocol)

# Write data to InfluxDB
try:
    client.write_points(data, protocol='line')
    print("Data uploaded successfully!")
except Exception as e:
    print(f"Failed to write to InfluxDB: {e}")
