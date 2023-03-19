import pyspark.sql.types as T

INPUT_FHV_DATA_PATH = '../../resources/rides_fhv.csv'
INPUT_GREEN_DATA_PATH = '../../resources/rides_green.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_PULOCATION_ID_COUNT = 'pickup_counts_windowed'

GREEN_PRODUCE_TOPIC_RIDES_CSV = GREEN_CONSUME_TOPIC_RIDES_CSV = 'rides_green'
FHV_PRODUCE_TOPIC_RIDES_CSV = FHV_CONSUME_TOPIC_RIDES_CSV = 'rides_fhv'


RIDE_SCHEMA = T.StructType([
                T.StructField("source", T.StringType()),
                T.StructField("pickup_location", T.IntegerType()),
                T.StructField("dropoff_location", T.IntegerType()),
            ])
