import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer
from multiprocessing import Process

from settings import BOOTSTRAP_SERVERS, INPUT_GREEN_DATA_PATH, GREEN_PRODUCE_TOPIC_RIDES_CSV, \
            INPUT_FHV_DATA_PATH, FHV_PRODUCE_TOPIC_RIDES_CSV


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    # def read_records(resource_path: str):
    #     records, ride_keys = [], []
    #     i = 0
    #     with open(resource_path, 'r') as f:
    #         reader = csv.reader(f)
    #         header = next(reader)  # skip the header
    #         for row in reader:
    #             # vendor_id, passenger_count, trip_distance, payment_type, total_amount
    #             records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[9]}, {row[16]}')
    #             ride_keys.append(str(row[0]))
    #             i += 1
    #             if i == 5:
    #                 break
    #     return zip(ride_keys, records)

    def read_records(resource_path: str, trips: str = 'green'):
        ride_records, ride_keys = [], []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                if trips == "green":
                    ride_records.append(f'{trips}, {row[5]}, {row[6]}')
                else:
                    ride_records.append(f'{trips}, {row[3]}, {row[4]}')
                ride_keys.append(f'{trips}')
        return zip(ride_keys, ride_records)

    def publish(self, topic: str, records: [str, str]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.send(topic=topic, key=key, value=value)
                print(f"Producing record for <key: {key}, value:{value}>")
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }
    producer = RideCSVProducer(props=config)
    green_ride_records = producer.read_records(resource_path=INPUT_GREEN_DATA_PATH)
    fhv_ride_records = producer.read_records(resource_path=INPUT_FHV_DATA_PATH, trips="fhv")
    print(green_ride_records)
    print(fhv_ride_records)
    # producer.publish(topic=GREEN_PRODUCE_TOPIC_RIDES_CSV, records=ride_records)
    p2 = Process(target = producer.publish(topic=GREEN_PRODUCE_TOPIC_RIDES_CSV, records=green_ride_records))
    p1 = Process(target = producer.publish(topic=FHV_PRODUCE_TOPIC_RIDES_CSV, records=fhv_ride_records))
    p1.start()
    sleep(20)
    p2.start()
    p1.join()
    p2.join()
