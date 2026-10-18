import os
import random
import psycopg2
from dotenv import load_dotenv

load_dotenv()
random.seed(42)

TEMPLATES = {
    ("bike_availability", "negative"): [
        "Waited 20 minutes and there was not a single bike at the station.",
        "Station was empty again, had to walk to the next one.",
        "No bikes available at rush hour, super frustrating.",
        "every morning this station has zero bikes, cant get to work on time",
        "Came here three times this week and nothing to ride.",
    ],
    ("dock_availability", "negative"): [
        "Could not return my bike, all docks were full.",
        "Circled the block twice looking for a free dock.",
        "no empty slots to park, ended up paying extra for a late return",
        "Station is always packed in the evening, nowhere to put the bike back.",
    ],
    ("bike_condition", "negative"): [
        "The seat was loose and the chain kept slipping.",
        "Handlebar felt wobbly, I did not feel safe.",
        "got a bike with a flat rear wheel, had to swap it",
        "Gears were stuck and the bell did not work.",
    ],
    ("app_payment", "negative"): [
        "App crashed while unlocking and I got charged twice.",
        "Payment failed at the kiosk but money left my account.",
        "unlock code never arrived, wasted 10 minutes",
        "The app keeps logging me out when I try to rent.",
    ],
    ("pricing", "negative"): [
        "Membership price went up again, hard to justify.",
        "Extra minute fees are too high for short trips.",
        "paid more than the posted fare, please check billing",
    ],
    ("praise", "positive"): [
        "Great ride along the river, bike was smooth and the dock was easy.",
        "Love this service, saves me 20 minutes every day.",
        "Staff fixed my issue quickly, thank you!",
        "Smooth ride and plenty of bikes today.",
    ],
    ("general", "neutral"): [
        "Used it for the first time today, it was okay.",
        "Average experience, nothing special.",
        "Rode from the park to the office, took about 15 minutes.",
    ],
}
WEIGHTS = {"bike_availability": 30, "dock_availability": 20, "bike_condition": 15,
           "app_payment": 10, "pricing": 5, "praise": 12, "general": 8}


def messy(text):
    r = random.random()
    if r < 0.25:
        text = text.lower()
    elif r < 0.40:
        text = text.upper()
    if random.random() < 0.25:
        i = random.randrange(len(text))
        text = text[:i] + text[i + 1:]
    if random.random() < 0.2:
        text += random.choice([" pls fix", " !!", " thanks", " (again)"])
    return text


conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute("SELECT count(*) FROM rider_feedback;")
if cur.fetchone()[0] > 0:
    print("rider_feedback already has rows. Not adding more (run only once).")
    conn.close()
    raise SystemExit

cur.execute("SELECT station_id FROM stations WHERE capacity > 0 ORDER BY random() LIMIT 300;")
station_ids = [r[0] for r in cur.fetchall()]

keys = list(TEMPLATES)
weights = [WEIGHTS[k[0]] for k in keys]

for _ in range(200):
    cat, sent = random.choices(keys, weights=weights)[0]
    text = messy(random.choice(TEMPLATES[(cat, sent)]))
    cur.execute(
        """INSERT INTO rider_feedback
           (station_id, rider_type, feedback_text, true_category, true_sentiment, created_at)
           VALUES (%s, %s, %s, %s, %s, now() - random() * interval '14 days')""",
        (random.choice(station_ids), random.choice(["member", "casual"]), text, cat, sent),
    )

conn.commit()
cur.execute("SELECT count(*) FROM rider_feedback;")
print("rider_feedback rows:", cur.fetchone()[0])
conn.close()