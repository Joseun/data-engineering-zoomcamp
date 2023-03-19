INPUT_FHV_DATA_PATH = 'resources/rides_fhv.csv'
INPUT_GREEN_DATA_PATH = 'resources/rides_green.csv'

RIDE_KEY_SCHEMA_PATH = 'resources/schemas/taxi_ride_key.avsc'
RIDE_VALUE_SCHEMA_PATH = 'resources/schemas/taxi_ride_value.avsc'

SCHEMA_REGISTRY_URL = 'http://localhost:8081'
BOOTSTRAP_SERVERS = 'localhost:9092'
FHV_KAFKA_TOPIC = 'rides_fhv'
GREEN_KAFKA_TOPIC = 'rides_green'
