from pandas import Series
from threading import Queue


class RelocationCityETL:
    def __init__(self):
        pass
    
    @classmethod
    def with_incoming_cities(cls, cities):
        etl = cls()
        cities = Series(cities).unique()
        