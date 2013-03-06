from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, Numeric, String, DateTime
from sqlalchemy.orm import relationship
import datetime

from .base import Base
from .trip import Trip


class StopTime(Base):
    __tablename__ = 'stop_times'

    required_fields = [
        'trip_id',
        'arrival_time',
        'departure_time',
        'stop_id',
        'stop_sequence'
    ]
    optional_fields = [
        'stop_headsign',
        'pickup_type',
        'drop_off_type',
        'shape_dist_traveled'
    ]
    proposed_fields = ['timepoint']

    dump_id = Column(Integer, primary_key=True, nullable=False)
    trip_id = Column(Integer, primary_key=True, nullable=False)
    arrival_time = Column(String)
    departure_time = Column(String)
    stop_id = Column(Integer, nullable=False)
    stop_sequence = Column(Integer, primary_key=True, nullable=False)
    stop_headsign = Column(String)
    pickup_type = Column(Integer, default=0)
    drop_off_type = Column(Integer, default=0)
    shape_dist_traveled = Column(Numeric)
    timepoint = Column(Boolean, default=False)
    arrival_timestamp = Column(DateTime)
    
    #stop = relationship('Stop', primaryjoin="and_(StopTime.stop_id==Stop.stop_id, StopTime.dump_id==Stop.dump_id)")
#    trip = relation(Trip, backref='stop_times')

    @classmethod
    def make_record(cls, row, engine, dump_id):
        # clean dict
        for k, v in row.items():
            if isinstance(v, basestring):
                v = v.strip()
            if (k not in cls.__table__.c):
                del row[k]
            elif not v:
                row[k] = None
            elif k.endswith('date'):
                row[k] = datetime.datetime.strptime(v, '%Y%m%d').date()
    
        # add dump id
        row['dump_id']=dump_id
        
        # add arrival_timestamp
        if row['arrival_time']:
            timecomps=row['arrival_time'].split(":")
            utcts=int(timecomps[0]) * 3600 + int(timecomps[1])*60 + int(timecomps[2])        
            row['arrival_timestamp']=datetime.datetime.utcfromtimestamp(utcts)
        
        return row

    def __init__(self, *args, **kwargs):
        super(StopTime, self).__init__(*args, **kwargs)
        if 'timepoint' not in kwargs:
            self.timepoint = ('arrival_time' in kwargs)

Index('%s_ix1' %(StopTime.__tablename__), StopTime.stop_id)
Index('%s_ix2' %(StopTime.__tablename__), StopTime.timepoint)
