from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Integer, String

from .base import Base
from .trip import Trip


class Frequency(Base):
    __tablename__ = 'frequencies'

    required_fields = ['trip_gen_id', 'start_time', 'end_time', 'headway_secs']
    proposed_fields = ['exact_times']

    trip_gen_id = Column(String, primary_key=True)
    start_time = Column(String, primary_key=True)
    end_time = Column(String)
    headway_secs = Column(Integer)
    exact_times = Column(Integer)
