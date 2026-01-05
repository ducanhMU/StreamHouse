import psycopg2 # connect & work w/ postgresql
import uuid # generate UUIDs
import random # generate random values
import os # read environment variables & operate system
from datetime import datetime, timedelta, timezone # generate timestamps
import json # working with JSON
import logging # print logs, useful for Docker
from faker import Faker # generate fake data
from time import sleep # pause execution for a while

# Set up logging for Docker
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Initialize Faker instance
fake = Faker()

# Database connection parameters from environment variables
DB_PARAMS = { # need to match the schema in docs
    'database': os.getenv('POSTGRES_DB', 'jadc2_db'), # not 'dbname' ??
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'password'),
    'host': os.getenv('POSTGRES_HOST', 'postgres-source'),
    'port': os.getenv('POSTGRES_PORT', '5432')
}
# Configurable record counts from environment variables
# if not set, use default values
# default number rows per table
RECORD_COUNTS = { # set it smaller for testing purpose
    'regions': int(os.getenv('REGIONS_COUNT', 5)),
    'targets': int(os.getenv('TARGETS_COUNT', 5)),
    'users': int(os.getenv('USERS_COUNT', 5)),
    'units': int(os.getenv('UNITS_COUNT', 5)),
    'weapons': int(os.getenv('WEAPONS_COUNT', 5)),
    'sensors': int(os.getenv('SENSORS_COUNT', 5)),
    'detections': int(os.getenv('DETECTIONS_COUNT', 5)),
    'weather_events': int(os.getenv('WEATHER_EVENTS_COUNT', 5)),
    'unit_status_updates': int(os.getenv('UNIT_STATUS_UPDATES_COUNT', 5)),
    'supply_status': int(os.getenv('SUPPLY_STATUS_COUNT', 5)),
    'cyber_ew_events': int(os.getenv('CYBER_EW_EVENTS_COUNT', 5)),
    'engagement_events': int(os.getenv('ENGAGEMENT_EVENTS_COUNT', 5)),
    'roe_updates': int(os.getenv('ROE_UPDATES_COUNT', 5)),
    'alerts': int(os.getenv('ALERTS_COUNT', 5)),
    'commands': int(os.getenv('COMMANDS_COUNT', 5))
}

# ENUM values from schema
UNIT_STATUSES = ['ready', 'engaged', 'damaged', 'destroyed', 'retreat']
DOMAINS = ['land', 'sea', 'air', 'space', 'cyber']
ALERT_STATUSES = ['active', 'resolved', 'acknowledged', 'dismissed']
EW_EFFECTS = ['jammed', 'spoofed', 'disabled', 'ddos']
WEATHER_TYPES = ['rain', 'fog', 'storm', 'solar_flare']
ENGAGEMENT_RESULTS = ['destroyed', 'damaged', 'escaped', 'missed']
THREAT_LEVELS = ['low', 'medium', 'high', 'critical']
IFF_STATUSES = ['friend', 'foe', 'neutral', 'unknown']
USER_ROLES = ['commander', 'analyst', 'operator']
TERRAIN_TYPES = ['mountain', 'desert', 'urban', 'forest', 'swamp']
TARGET_TYPES = ['missile', 'aircraft', 'tank', 'ship', 'drone']
INTENTS = ['attack', 'recon', 'retreat', 'unknown']
CLASSIFICATIONS = ['unclassified', 'confidential', 'secret', 'top_secret']
SOURCES = ['radar_feed', 'sonar_feed', 'sat_feed', 'c2_system', 'ew_monitor', 'metoc', 'logistics_feed', 'isr_feed', 'auth_system', 'gis_feed']
#------------------------
REGIONS = ['Ha Noi', 'Thanh Hoa', 'Lang Son', 'Hue', 'Nghe An', 'Ha Tinh', 'Lai Chau',
           'Dien Bien', 'Cao Bang', 'Quang Ninh', 'Son La', 'Tuyen Quang', 'Lao Cai',
           'Thai Nguyen', 'Phu Tho', 'Bac Ninh', 'Hung Yen', 'Hai Phong', 'Ninh Binh',
           'Quang Tri', 'Da Nang', 'Quang Ngai', 'Gia Lai', 'Khanh Hoa', 'Lam Dong',
           'Dak Lak', 'Ho Chi Minh', 'Dong Nai', 'Tay Ninh', 'Can Tho', 'Vinh Long',
           'Dong Thap', 'Ca Mau', 'An Giang',
           'Sakhir', 'Melbourne', 'Shang Hai', 'Tokyo', 'Bahrain', 
           'Saudi Arabia', 'Miami', 'Emilia Romagna', 'Silverstone', 'Catalunya',
           'Monaco', 'Azerbaijan', 'Canada', 'France', 'Austria', 'Britain', 
           'Hungary', 'Belgium', 'Netherlands', 'Italy', 'Singapore', 'Russia', 
           'Japan', 'USA', 'Mexico', 'Brazil', 'Abu Dhabi', 'Qatar']
#------------------------
NAMES_LAND = ["tank", "infantry", "artillery"]
UNITS_LAND = ['T-90', 'T-62', 'T-54', 'T-55', 'T-59', 'T-34', 'PT-76', 'T-63', 'BMP-1',
         'BMP-2', 'XCB-01', 'BTR-40', 'BTR-50', 'BTR-60', 'BTR-152', 'M-133', 'XTC-02',
         'BRDM-2', 'V-100', 'V-150', 'SU-100', 'M578', 'BTS-4', 'BREM-1M', 'MAZ-537',
         'KZKT-7428', 'M8A1', 'M24', 'IS-2', 'ZiS-3', 'BS-3', 'T-12', 'MT-12', 'D-44',
         'M101', 'D-74', 'D-30', 'M-46', 'D-20', 'MT-LB', 'AT-L', 'ATS-59', 'M548',
         'BM-14', 'BM-21', 'T306', '2S1', '2S3', 'M106', 'SS-1', 'TMM-3M', 'AM-50S',
         'MS-20', 'IMR-2', 'EOV-4421', 'PZM-2', 'TMK-2', 'PTS-M', 'GSP-55', 'PMP',
         'VSN-1500', 'BMK-T', 'BMK-150', 'BMK-130', 'RAID-M100', 'IHER', 'RBH-18',
         'V-025', 'ARS-14', 'ARS-15', 'TMVA-17', 'UAZ-469RH', 'BRDM-2RKh']

NAMES_SEA = ["frigate", "submarine", "destroyer"]
UNITS_SEA = ['EXTRA', 'ACCULAR', '4K51', 'R-M', 'VCM-B', 'K-300P', 'Kh-35', 'VCM-01',
             '3M-14', '3M-54', 'P-5', 'P-15', 'P-800', 'G-3.9', 'P-II', 'P-III', 
             'HQ-011', 'HQ-012', 'HQ-015', 'HQ-016', 'HQ-09', 'HQ-11', 'HQ-13', 
             'HQ-924', 'HQ-954', 'HQ-16', 'HQ-18', 'FC-624', 'HQ-270', 'HQ-377', 
             'K-122', 'K-123', 'HQ-905', 'RGS-9316', 'FET-10', 'HSV-6613']

NAMES_AIR = ["fighter", "bomber", "drone"]
UNITS_AIR = ['Tatra-815', 'MAZ-6371', 'HD170', 'MAN HX58', 'ME160',
             'ZU-23-2', '61-K', 'AZP S-60', 'KS-19', 'ZSU-23-4', 'S-300', 'SPYDER',
             'SA-2', 'S-125', '9K35', 'Su-30 MK2', 'Su-27', 'Su-22', 'L-39', 'Yak-130',
             'Yak-52', 'T-6', 'An-2', 'CN-295', 'C-212', 'Mi-8', 'Mi-17', 'AS365',
             'EC225', 'AS332', 'AW189', 'M-400 UAV', 'VT', 'HS-6L']

NAMES_SPACE = ["satellite", "orbiter", "space_station"]
UNITS_SPACE = ['ZIL-130', 'ZIL-131', 'ZIL-157', 'GAZ-66', 'KrAZ-255', 'KrAZ-6322', 
               'GAZ-3308''Mi-4', 'Mi-6', 'Mi-24', 'Ka-25', 'CH-47', 'J-6', 'MiG-15', 'MiG-17',
              'MiG-19', 'MiG-21', 'MiG-23', 'F-5', 'Su-7', 'A-1', 'A-37', 'Il-28', 'Be-12',
              'An-24', 'An-26', 'Il-14', 'C-47', 'C-119', 'C-130', 'DHC-2', 'DHC-4']

NAMES_CYBER = ["firewall", "intrusion_system", "malware_scanner"]
UNITS_CYBER = ['Yak-11', 'Yak-18', 'T-37', 'O-1', 'C-185', 'L-29', 'HL-1', 'HL-2', 'TL-1']
#------------------------
RADARS = ['KamAZ-6560', 'KamAZ-4350', 'KamAZ-4326', 'Ural-375', 'Ural-4320', 
          'P-12', 'P-14', 'P-15', 'P-18', 'P-19', 'P-35', 'P-37', 'PRV-16', 'Nebo-UE',
          'Kolchuga', 'Kasta 2E2', '36D6', 'RSP-10', 'RSP-10M', 'Vostok E', 'ELM-2228ER',
          'VERA-E', 'CW-100', 'S-75 SAM-2', 'S-125 SAM-3', 'S-300 SAM-20']
WEAPONS = ['K-13', 'R-60', 'R-27', 'R-73', 'R-77', 'Kh-28', 'Kh-25', 'Kh-59', 'Kh-29',
           'Kh-31', 'S-5', 'S-8', 'S-13', 'KAB-500KR', 'KAB-1500L', 'OFAB-100-120', 
           'OFAB-250-270', 'KamAZ-43118', 'KamAZ-65224', 'KamAZ-43253', 'KamAZ-5350']


# Connect to PostgreSQL with retry
def get_db_connection(max_retries=3, retry_delay=5):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            logging.info("Database connection established")
            return conn
        except Exception as e:
            logging.error(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                sleep(retry_delay)
            else:
                raise Exception("Max retries reached for database connection")

# template: generate_* (functions that generate data for each table)
# 1. create empty list []
# 2. for loop in 'range(num_records)'
# 3. create each row (dict format), append into list
# 4. return list

# Generate regions_raw data
def generate_regions(num_records):
    regions = []
    for _ in range(num_records):
        region = {
            'id': str(uuid.uuid4()),
            'region_name': random.choice(REGIONS),
            'terrain_type': random.choice(TERRAIN_TYPES),
            'infrastructure_level': round(random.uniform(0.1, 1.0), 4),
            'area_sqkm': round(random.uniform(100, 10000), 4),
            'lat_center': round(random.uniform(-90, 90), 8),
            'lon_center': round(random.uniform(-180, 180), 8),
            'classification': random.choice(CLASSIFICATIONS),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'gis_feed'
        }
        regions.append(region)
    return regions

# Generate targets_raw data
def generate_targets(num_records):
    targets = []
    for _ in range(num_records):
        target = {
            'id': str(uuid.uuid4()),
            'target_id': f'HOSTILE_{random.randint(100, 999)}',
            'target_type': random.choice(TARGET_TYPES),
            'domain': random.choice(DOMAINS),
            'estimated_intent': random.choice(INTENTS),
            'threat_level': random.choice(THREAT_LEVELS),
            'iff_status': random.choice(IFF_STATUSES),
            'classification': random.choice(CLASSIFICATIONS),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'isr_feed'
        }
        targets.append(target)
    return targets

# Generate users_raw data
def generate_users(num_records):
    users = []
    for _ in range(num_records):
        user = {
            'id': str(uuid.uuid4()),
            'username': fake.user_name(),
            'role': random.choice(USER_ROLES),
            'classification': random.choice(CLASSIFICATIONS),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'auth_system'
        }
        users.append(user)
    return users

# Generate units_raw data
def generate_units(num_records):
    units = []
    for _ in range(num_records):
        domain = random.choice(DOMAINS)

        if domain == "land":
            unit_type = random.choice(UNITS_LAND)
            unit_name = random.choice(NAMES_LAND)
        elif domain == "sea":
            unit_type = random.choice(UNITS_SEA)
            unit_name = random.choice(NAMES_SEA)
        elif domain == "air":
            unit_type = random.choice(UNITS_AIR)
            unit_name = random.choice(NAMES_AIR)
        elif domain == "space":
            unit_type = random.choice(UNITS_SPACE)
            unit_name = random.choice(NAMES_SPACE)
        else:  # cyber
            unit_type = random.choice(UNITS_CYBER)
            unit_name = random.choice(NAMES_CYBER)

        unit = {
            'id': str(uuid.uuid4()),
            'name': f'UNIT_{unit_name.upper()}_{random.randint(1, 999)}',
            'domain': domain,
            'unit_type': unit_type,
            'status': random.choice(UNIT_STATUSES),
            'classification': random.choice(CLASSIFICATIONS),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'c2_system'
        }
        units.append(unit)
    return units

# Generate weapons_raw data
def generate_weapons(units, num_records):
    weapons = []
    for _ in range(num_records):
        weapon = {
            'id': str(uuid.uuid4()),
            'unit_id': random.choice([u['id'] for u in units]),
            'name': f'WEAPON_{fake.word().upper()}_{random.randint(1, 999)}',
            'type': random.choice(WEAPONS),
            'effective_domain': random.choice(DOMAINS),
            'range_km': round(random.uniform(10, 500), 4),
            'hit_probability': round(random.uniform(0.5, 0.95), 4),
            'speed_kmh': round(random.uniform(100, 5000), 4),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'weapon_db'
        }
        weapons.append(weapon)
    return weapons

# Generate sensors_raw data
def generate_sensors(units, num_records):
    sensors = []
    for _ in range(num_records):
        sensor = {
            'id': str(uuid.uuid4()),
            'unit_id': random.choice([u['id'] for u in units]),
            'sensor_type': random.choice(RADARS),
            'range_km': round(random.uniform(10, 1000), 4),
            'status': random.choice(['active', 'inactive', 'degraded']),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'sensor_log'
        }
        sensors.append(sensor)
    return sensors

# Generate detections_raw data
def generate_detections(sensors, targets, regions, num_records):
    detections = []

    if not sensors or not targets or not regions:
        logging.warning("generate_detections skipped: missing sensors, targets, or regions")
        return detections

    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        detection = {
            'id': str(uuid.uuid4()),
            'sensor_id': random.choice([s['id'] for s in sensors]),
            'target_id': random.choice([t['id'] for t in targets]),
            'target_domain': random.choice(DOMAINS),
            'region_id': random.choice([r['id'] for r in regions]),
            'lat': round(random.uniform(-90, 90), 8),
            'lon': round(random.uniform(-180, 180), 8),
            'altitude_depth': round(random.uniform(-1000, 20000), 4),
            'speed_kmh': round(random.uniform(0, 2000), 4),
            'heading_deg': round(random.uniform(0, 360), 4),
            'confidence': round(random.uniform(0.5, 1.0), 4),
            'iff_status': random.choice(IFF_STATUSES),
            'threat_level': random.choice(THREAT_LEVELS),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': random.choice(['radar_feed', 'sonar_feed', 'sat_feed'])
        }
        detections.append(detection)
    return detections

# Generate weather_events_raw data
def generate_weather_events(regions, num_records):
    weather_events = []
    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        event = {
            'id': str(uuid.uuid4()),
            'region_id': random.choice([r['id'] for r in regions]),
            'type': random.choice(WEATHER_TYPES),
            'intensity': round(random.uniform(0.1, 1.0), 4),
            'wind_speed_kmh': round(random.uniform(0, 100), 4),
            'precipitation_mm': round(random.uniform(0, 50), 4),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'metoc'
        }
        weather_events.append(event)
    return weather_events

# Generate unit_status_updates_raw data
def generate_unit_status_updates(units, regions, num_records):
    updates = []
    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        update = {
            'id': str(uuid.uuid4()),
            'unit_id': random.choice([u['id'] for u in units]),
            'region_id': random.choice([r['id'] for r in regions]),
            'lat': round(random.uniform(-90, 90), 8),
            'lon': round(random.uniform(-180, 180), 8),
            'health_percent': round(random.uniform(20, 100), 4),
            'max_range_km': round(random.uniform(10, 500), 4),
            'status': random.choice(UNIT_STATUSES),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'unit_telemetry'
        }
        updates.append(update)
    return updates

# Generate supply_status_raw data
def generate_supply_status(units, regions, num_records):
    supplies = []
    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        supply = {
            'id': str(uuid.uuid4()),
            'unit_id': random.choice([u['id'] for u in units]),
            'region_id': random.choice([r['id'] for r in regions]),
            'supply_level': round(random.uniform(10, 100), 4),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'logistics_feed'
        }
        supplies.append(supply)
    return supplies

# Generate cyber_ew_events_raw data
def generate_cyber_ew_events(units, sensors, num_records):
    events = []
    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        event = {
            'id': str(uuid.uuid4()),
            'source_id': random.choice([u['id'] for u in units]),
            'target_sensor_id': random.choice([s['id'] for s in sensors]),
            'effect': random.choice(EW_EFFECTS),
            'impact_domain': random.choice(DOMAINS),
            'level': random.choice(THREAT_LEVELS),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'ew_monitor'
        }
        events.append(event)
    return events

# Generate engagement_events_raw data
def generate_engagement_events(units, targets, weapons, num_records):
    engagements = []
    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        engagement = {
            'id': str(uuid.uuid4()),
            'attacker_id': random.choice([u['id'] for u in units]),
            'target_id': random.choice([t['id'] for t in targets]),
            'weapon_id': random.choice([w['id'] for w in weapons]),
            'hit': random.choice([True, False]),
            'result': random.choice(ENGAGEMENT_RESULTS),
            'roe_compliant': random.choice([True, False]),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'fire_control_system'
        }
        engagements.append(engagement)
    return engagements

# Generate roe_updates_raw data
def generate_roe_updates(num_records):
    roe_updates = []
    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        rules = random.sample(['no_collateral', 'restrict_airspace', 'engage_foe_only', 'minimize_civilian_risk'], k=random.randint(1, 3))
        update = {
            'id': str(uuid.uuid4()),
            'rules': json.dumps(rules),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'c2_policy_feed'
        }
        roe_updates.append(update)
    return roe_updates

# Generate alerts_raw data
def generate_alerts(detections, num_records):
    alerts = []
    base_time = datetime.now() - timedelta(hours=24)
    for _ in range(num_records):
        alert = {
            'id': str(uuid.uuid4()),
            'detection_id': random.choice([d['id'] for d in detections]),
            'status': random.choice(ALERT_STATUSES),
            'msg': f"{random.choice(['Inbound threat', 'Hostile detected', 'Missile alert'])} in sector",
            'threat_level': random.choice(THREAT_LEVELS),
            'created_at': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'alert_system'
        }
        alerts.append(alert)
    return alerts

# Generate commands_raw data
def generate_commands(alerts, units, users, num_records):
    commands = []
    base_time = datetime.now(timezone.utc) - timedelta(hours=24)

    commanders = [u['id'] for u in users if u['role'] == 'commander']
    if not commanders:
        logging.warning("No commanders available, skipping commands generation.")
        return commands  # empty list

    for _ in range(num_records):
        command = {
            'id': str(uuid.uuid4()),
            'alert_id': random.choice([a['id'] for a in alerts]) if alerts else None,
            'unit_id': random.choice([u['id'] for u in units]) if units else None,
            'user_id': random.choice(commanders),
            'action': random.choice(['Engage target', 'Hold position', 'Retreat', 'Recon area']),
            'event_time': (base_time + timedelta(minutes=random.randint(0, 1440))).isoformat(),
            'ingest_timestamp': datetime.now(timezone.utc).isoformat(),
            'src': 'c2_system'
        }
        # Only append if essential IDs exist
        if command['alert_id'] and command['unit_id']:
            commands.append(command)

    return commands

# Insert data into PostgreSQL
def insert_data(conn, table_name, data):
    if not data:
        logging.warning(f"No data to insert for {table_name}")
        return
    columns = data[0].keys()
    query = f"""
        INSERT INTO raw.{table_name} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT DO NOTHING
    """
    try:
        with conn.cursor() as cur:
            cur.executemany(query, [tuple(d[c] for c in columns) for d in data])
            conn.commit()
            logging.info(f"Inserted {len(data)} records into raw.{table_name}")

            # Select lại những record thực sự có trong DB
            cur.execute(f"SELECT * FROM raw.{table_name}")
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            result = [dict(zip(colnames, row)) for row in rows]
            return result
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to insert into {table_name}: {e}")
        raise

def main():
    # Continuous run option
    continuous = os.getenv('CONTINUOUS_RUN', 'false').lower() == 'true'
    loop_interval = int(os.getenv('LOOP_INTERVAL_SECONDS', 3600))  # Default: 1 hour
    while True:
        conn = get_db_connection()
        try:
            # Generate and insert data in dependency order
            regions = generate_regions(RECORD_COUNTS['regions'])
            regions = insert_data(conn, 'regions', regions)

            targets = generate_targets(RECORD_COUNTS['targets'])
            targets = insert_data(conn, 'targets', targets)

            users = generate_users(RECORD_COUNTS['users'])
            users = insert_data(conn, 'users', users)

            units = generate_units(RECORD_COUNTS['units'])
            units = insert_data(conn, 'units', units)

            weapons = generate_weapons(units, RECORD_COUNTS['weapons'])
            weapons = insert_data(conn, 'weapons', weapons)

            sensors = generate_sensors(units, RECORD_COUNTS['sensors'])
            sensors = insert_data(conn, 'sensors', sensors)

            detections = generate_detections(sensors, targets, regions, RECORD_COUNTS['detections'])
            detections = insert_data(conn, 'detections', detections)

            weather_events = generate_weather_events(regions, RECORD_COUNTS['weather_events'])
            weather_events = insert_data(conn, 'weather_events', weather_events)

            unit_status_updates = generate_unit_status_updates(units, regions, RECORD_COUNTS['unit_status_updates'])
            unit_status_updates = insert_data(conn, 'unit_status_updates', unit_status_updates)

            supply_status = generate_supply_status(units, regions, RECORD_COUNTS['supply_status'])
            supply_status = insert_data(conn, 'supply_status', supply_status)

            cyber_ew_events = generate_cyber_ew_events(units, sensors, RECORD_COUNTS['cyber_ew_events'])
            cyber_ew_events = insert_data(conn, 'cyber_ew_events', cyber_ew_events)

            engagement_events = generate_engagement_events(units, targets, weapons, RECORD_COUNTS['engagement_events'])
            engagement_events = insert_data(conn, 'engagement_events', engagement_events)

            roe_updates = generate_roe_updates(RECORD_COUNTS['roe_updates'])
            roe_updates = insert_data(conn, 'roe_updates', roe_updates)

            alerts = generate_alerts(detections, RECORD_COUNTS['alerts'])
            alerts = insert_data(conn, 'alerts', alerts)

            commands = generate_commands(alerts, units, users, RECORD_COUNTS['commands'])
            if commands:  # only insert if not empty
                commands = insert_data(conn, 'commands', commands)
            else:
                logging.info("Skipped inserting commands (no commanders or missing data).")

            if not continuous:
                break
            logging.info(f"Completed data generation cycle. Sleeping for {loop_interval} seconds.")
            sleep(loop_interval)

        except Exception as e:
            logging.error(f"Error in data generation cycle: {e}")
            if not continuous:
                raise
        finally:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()