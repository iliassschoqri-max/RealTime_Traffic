from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
import sys
from datetime import datetime, timedelta
import pytz

# ================= CONFIGURATION =================
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC  = "traffic-events"
# 30% dirty, 70% clean
CLEAN_DATA_PROBABILITY = 0.7
TANGIER_CENTER_LAT = 35.7595
TANGIER_CENTER_LNG = -5.8340
# =================================================

fake = Faker()

# Initialisation du Producteur avec gestion d'erreur
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=30000
    )
    print(f"[OK] Connecte avec succès a Kafka sur {KAFKA_SERVER}")
except Exception as e:
    print(f"[ERR] Erreur de connexion a Kafka : {e}")
    print("Assurez-vous que Docker et Kafka sont lances !")
    sys.exit(1)

# Donnees de reference — Tanger
roads = [
    {"id": "MA-TNG-001", "name": "Boulevard Mohamed V", "lat": 35.7673, "lng": -5.8127},
    {"id": "MA-TNG-002", "name": "Avenue Pasteur",      "lat": 35.7712, "lng": -5.8178},
    {"id": "MA-TNG-003", "name": "Avenue d'Espagne",    "lat": 35.7765, "lng": -5.8090},
    {"id": "MA-TNG-004", "name": "Boulevard de la Corniche", "lat": 35.7820, "lng": -5.8025},
    {"id": "MA-TNG-005", "name": "Boulevard Moulay Youssef", "lat": 35.7630, "lng": -5.8220},
    {"id": "MA-TNG-006", "name": "Avenue Mohammed VI",  "lat": 35.7550, "lng": -5.8300},
    {"id": "MA-TNG-007", "name": "Rue de la Liberte",   "lat": 35.7690, "lng": -5.8160},
    {"id": "MA-TNG-008", "name": "Boulevard Abdelkrim Khattabi", "lat": 35.7580, "lng": -5.8260},
]
zones = ["Centre Ville", "Port de Tanger", "Corniche", "Ville Nouvelle", "Ancienne Medina", "Gare TGV"]
weather = ["CLEAR", "RAIN", "FOG", "STORM"]
vehicle_cache = []

def generate_clean_event():
    """Genère un evenement de trafic sans erreur pour Tanger."""
    vid = fake.uuid4()

    if len(vehicle_cache) > 1000:
        vehicle_cache.pop(0)
    vehicle_cache.append(vid)

    road = random.choice(roads)
    now = datetime.now(pytz.utc)
    hour = now.hour

    if 6 <= hour <= 9:
        speed = random.randint(15, 45)
        congestion = random.randint(3, 5)
    elif 16 <= hour <= 19:
        speed = random.randint(20, 50)
        congestion = random.randint(2, 5)
    elif 10 <= hour <= 15:
        speed = random.randint(35, 80)
        congestion = random.randint(1, 3)
    else:
        speed = random.randint(50, 100)
        congestion = random.randint(1, 2)

    return {
        "vehicle_id": vid,
        "road_id": road["id"],
        "road_name": road["name"],
        "latitude": round(random.uniform(road["lat"] - 0.002, road["lat"] + 0.002), 6),
        "longitude": round(random.uniform(road["lng"] - 0.002, road["lng"] + 0.002), 6),
        "city_zone": random.choice(zones),
        "city": "Tanger",
        "speed": speed,
        "congestion_level": congestion,
        "weather": random.choice(weather),
        "event_time": now.isoformat()
    }

def generate_dirty_event():
    """Genère un evenement avec une anomalie volontaire pour tester le pipeline."""
    dirty_type = random.choice([
        "null_speed", "negative_speed", "extreme_speed", 
        "duplicate_vehicle", "late_event", "future_event", 
        "wrong_datatype", "schema_drift", "corrupt_json"
    ])

    base = generate_clean_event()

    if dirty_type == "null_speed":
        base["speed"] = None
    elif dirty_type == "negative_speed":
        base["speed"] = -40
    elif dirty_type == "extreme_speed":
        base["speed"] = 420
    elif dirty_type == "duplicate_vehicle" and vehicle_cache:
        base["vehicle_id"] = random.choice(vehicle_cache)
    elif dirty_type == "late_event":
        base["event_time"] = (datetime.now(pytz.utc) - timedelta(minutes=random.randint(10, 120))).isoformat()
    elif dirty_type == "future_event":
        base["event_time"] = (datetime.now(pytz.utc) + timedelta(minutes=random.randint(5, 60))).isoformat()
    elif dirty_type == "wrong_datatype":
        base["speed"] = "FAST"
    elif dirty_type == "schema_drift":
        base["road_condition"] = random.choice(["GOOD", "BAD", "UNDER_CONSTRUCTION"])
    elif dirty_type == "corrupt_json":
        return {"raw": "###CORRUPTED_EVENT###"}

    return base

def main():
    print(f"[START] Demarrage du Producteur... Topic: {KAFKA_TOPIC}")
    print("[INFO] Appuyez sur Ctrl+C pour arreter.")
    
    count = 0
    try:
        while True:
            # Choisir si on envoie une donnee propre ou sale
            if random.random() < CLEAN_DATA_PROBABILITY:
                event = generate_clean_event()
                event_label = "CLEAN"
            else:
                event = generate_dirty_event()
                event_label = "DIRTY"

            # Envoi avec gestion d'erreur
            try:
                producer.send(KAFKA_TOPIC, value=event)
            except Exception as send_error:
                print(f"[WARN] Erreur lors de l'envoi : {send_error}")

            count += 1
            if count % 20 == 0:
                road_name = event.get("road_name", event.get("road_id", "N/A")) if isinstance(event, dict) else "CORRUPTED"
                zone = event.get("city_zone", "N/A") if isinstance(event, dict) else "CORRUPTED"
                print(f"[DATA] Total: {count} | {event_label} | {road_name} | {zone} | Vitesse: {event.get('speed', '?') if isinstance(event, dict) else '?'} km/h")

            time.sleep(random.uniform(0.5, 1.5))

    except KeyboardInterrupt:
        print("\n[STOP] Arret demande par l'utilisateur.")
    finally:
        producer.flush()
        producer.close()
        print("[OK] Producteur ferme proprement.")

if __name__ == "__main__":
    main()