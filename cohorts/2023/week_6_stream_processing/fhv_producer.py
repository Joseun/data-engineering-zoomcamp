import os
import csv
from time import sleep
from typing import Dict
from multiprocessing import Process

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride_record_key import RideRecordKey, ride_record_key_to_dict
from ride_record import RideRecord, ride_record_to_dict
from settings import RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, \
    SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVERS, INPUT_GREEN_DATA_PATH, \
    GREEN_KAFKA_TOPIC, INPUT_FHV_DATA_PATH, FHV_KAFKA_TOPIC


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideAvroProducer:
    def __init__(self, props: Dict):
        # Schema Registry and Serializer-Deserializer Configurations
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])
        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)
        self.key_serializer = AvroSerializer(schema_registry_client, key_schema_str, ride_record_key_to_dict)
        self.value_serializer = AvroSerializer(schema_registry_client, value_schema_str, ride_record_to_dict)

        # Producer Configuration
        producer_props = {'bootstrap.servers': props['bootstrap.servers']}
        self.producer = Producer(producer_props)

    @staticmethod
    def load_schema(schema_path: str):
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print("Delivery failed for record {}: {}".format(msg.key(), err))
            return
        print('Record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))

    @staticmethod
    def read_records(resource_path: str, trips: str):
        ride_records, ride_keys = [], []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                if trips == "green":
                    ride_records.append(RideRecord(arr=[trips, row[5], row[6]]))
                else:
                    ride_records.append(RideRecord(arr=[trips, row[3], row[4]]))
                ride_keys.append(RideRecordKey(source=trips))
        return zip(ride_keys, ride_records)

    def publish(self, topic: str, records: [RideRecordKey, RideRecord]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.poll(0)
                self.producer.produce(topic=topic,
                                      key=self.key_serializer(key, SerializationContext(topic=topic,
                                                                                        field=MessageField.KEY)),
                                      value=self.value_serializer(value, SerializationContext(topic=topic,
                                                                                              field=MessageField.VALUE)),
                                      on_delivery=delivery_report)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(10)


if __name__ == "__main__":
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'schema_registry.url': SCHEMA_REGISTRY_URL,
        'schema.key': RIDE_KEY_SCHEMA_PATH,
        'schema.value': RIDE_VALUE_SCHEMA_PATH
    }
    producer = RideAvroProducer(props=config)
    # green_ride_records = producer.read_records(resource_path=INPUT_GREEN_DATA_PATH, trips='green')
    fhv_ride_records = producer.read_records(resource_path=INPUT_FHV_DATA_PATH, trips='fhv')
    # p2 = Process(target = producer.publish(topic=GREEN_KAFKA_TOPIC, records=green_ride_records))
    # p1 = Process(target = producer.publish(topic=FHV_KAFKA_TOPIC, records=fhv_ride_records))
    # p1.start()
    # sleep(20)
    # p2.start()
    # p1.join()
    # p2.join()
    # producer.publish(topic=GREEN_KAFKA_TOPIC, records=green_ride_records)
    producer.publish(topic=FHV_KAFKA_TOPIC, records=fhv_ride_records)
