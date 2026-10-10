import os
import random
import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()

ISSUES = {
    "brake": ["Brake pad worn out, replaced on site.", "Front brake cable loose, tightened.", "Rider reported weak braking, needs full check."],
    "tire": ["Rear tire punctured, swapped tube.", "Tire pressure very low, inflated.", "Front tire worn, scheduled replacement."],
    "dock": ["Dock 5 not locking the bike properly.", "Dock sensor not responding, rebooted.", "Dock stuck, bike cannot be released."],
    "screen": ["Kiosk screen is blank, restarted.", "Payment screen frozen, needs technician.", "Touch not working on kiosk."],
    "battery": ["E-bike battery low, swapped.", "Battery not charging at dock.", "Battery cover damaged."],
}

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute("SELECT station_id FROM stations WHERE capacity > 0 ORDER BY random() LIMIT 300;")
station_ids = [r[0] for r in cur.fetchall()]

# 1) naye maintenance tickets
for _ in range(20):
    cat = random.choice(list(ISSUES))
    note = random.choice(ISSUES[cat]) + " " + fake.sentence(nb_words=6)
    cur.execute(
        "INSERT INTO maintenance_tickets (station_id, issue_category, technician_note) VALUES (%s, %s, %s)",
        (random.choice(station_ids), cat, note),
    )

# 2) kuch purane open tickets ko close karo (updated_at badlega -> watermark test)
cur.execute("""
    UPDATE maintenance_tickets SET status = 'closed', updated_at = now()
    WHERE ticket_id IN (
        SELECT ticket_id FROM maintenance_tickets
        WHERE status = 'open' ORDER BY random() LIMIT 5
    )
""")

# 3) naye rebalancing tasks
for _ in range(10):
    a, b = random.sample(station_ids, 2)
    cur.execute(
        "INSERT INTO rebalancing_tasks (from_station_id, to_station_id, bikes_count) VALUES (%s, %s, %s)",
        (a, b, random.randint(3, 15)),
    )

# 4) kuch pending tasks complete karo
cur.execute("""
    UPDATE rebalancing_tasks SET status = 'completed', completed_at = now(), updated_at = now()
    WHERE task_id IN (
        SELECT task_id FROM rebalancing_tasks
        WHERE status = 'pending' ORDER BY random() LIMIT 4
    )
""")

conn.commit()
for t in ("maintenance_tickets", "rebalancing_tasks"):
    cur.execute(f"SELECT count(*) FROM {t};")
    print(t, cur.fetchone()[0])
conn.close()