from typing import List, Dict


class RideRecord:

    def __init__(self, arr: List[str]):
        self.source = str(arr[0])
        try:
            self.pickup_location = int(arr[1])
        except:
            self.pickup_location = None
        try:
            self.dropoff_location = int(arr[2])
        except:
            self.dropoff_location = None

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
                d['source'],
                d['pickup_location'],
                d['dropoff_location']
            ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


def dict_to_ride_record(obj, ctx):
    if obj is None:
        return None

    return RideRecord.from_dict(obj)


def ride_record_to_dict(ride_record: RideRecord, ctx):
    return ride_record.__dict__
