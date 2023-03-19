from typing import Dict


class RideRecordKey:
    def __init__(self, source: str):
        self.source = source

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(source=d['source'])

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


def dict_to_ride_record_key(obj, ctx):
    if obj is None:
        return None

    return RideRecordKey.from_dict(obj)


def ride_record_key_to_dict(ride_record_key: RideRecordKey, ctx):
    return ride_record_key.__dict__
