import os
import json
import urllib.request
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"

def get_json(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)

disc = get_json(DISCOVERY_URL)
data = disc["data"]
lang = data.get("en") or next(iter(data.values()))
feeds = {f["name"]: f["url"] for f in lang["feeds"]}
raw = get_json(feeds["station_information"])["data"]["stations"]

# duplicate station_id ek hi batch mein na ho, isliye dict
rows = {}
for s in raw:
    rows[s["station_id"]] = (
        s["station_id"],
        s["name"],
        s.get("region_id"),
        int(s.get("capacity") or 0),
        s.get("lat"),
        s.get("lon"),
    )

load_dotenv()
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
execute_values(cur, """
    INSERT INTO stations (station_id, station_name, region, capacity, lat, lon)
    VALUES %s
    ON CONFLICT (station_id) DO UPDATE SET
        station_name = EXCLUDED.station_name,
        region       = EXCLUDED.region,
        capacity     = EXCLUDED.capacity,
        lat          = EXCLUDED.lat,
        lon          = EXCLUDED.lon,
        updated_at   = now()
    WHERE (stations.station_name, stations.region, stations.capacity)
          IS DISTINCT FROM (EXCLUDED.station_name, EXCLUDED.region, EXCLUDED.capacity)
""", list(rows.values()))
conn.commit()

cur.execute("SELECT count(*) FROM stations;")
print("Stations in table:", cur.fetchone()[0])
conn.close()