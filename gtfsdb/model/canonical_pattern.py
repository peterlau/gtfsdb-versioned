from geoalchemy import GeometryColumn, GeometryDDL, LineString
from sqlalchemy import Column, ForeignKey, Index
from sqlalchemy.types import Integer, String, Numeric
from sqlalchemy.sql import func
from sqlalchemy.orm import sessionmaker

from .agency import Agency
from .base import Base
from sqlalchemy.schema import Sequence


class CanonicalPattern(Base):
    __tablename__ = 'canonical_patterns'


    dump_id = Column(Integer, primary_key=True, nullable=False)
    service_id = Column(Integer, primary_key=True, nullable=False)
    route_id = Column(Integer, primary_key=True, nullable=False)
    direction_id = Column(Integer, primary_key=True, nullable=False)
    max_len = Column(Numeric)
    trip_id = Column(Integer)
    
    @classmethod
    def add_geometry_column(cls):
        cls.geom = GeometryColumn(LineString(2))
        GeometryDDL(cls.__table__)
    
    @classmethod   
    def load(cls, engine, dump_id=None):
        Session = sessionmaker(bind=engine)
        session = Session()
        
        print " - canonical_patterns"
        sql='''
INSERT INTO gtfs_versioned.canonical_patterns SELECT DISTINCT ON (tmp2.dump_id, tmp2.service_id, tmp2.route_id, tmp2.direction_id) tmp2.dump_id, tmp2.service_id, tmp2.route_id, tmp2.direction_id, tmp2.maxlen, tmp2.trip_id, tmp2.geom FROM (
    SELECT tmp.dump_id, tmp.service_id, tmp.route_id, tmp.direction_id, tmp.geom, tmp.trip_id, ST_length_spheroid(tmp.geom,'SPHEROID["GRS 1980",6378137,298.257222101]') len,
    max(ST_length_spheroid(tmp.geom,'SPHEROID["GRS 1980",6378137,298.257222101]')) OVER (PARTITION BY tmp.dump_id, tmp.service_id, tmp.route_id, tmp.direction_id) maxlen
    FROM(
        SELECT distinct on (dump_id, service_id, route_id, direction_id, geom) dump_id, service_id, route_id, direction_id, geom, trip_id FROM gtfs_versioned.trips WHERE dump_id=:dump_id
    ) tmp
) tmp2
WHERE tmp2.len=tmp2.maxlen
'''
        session.execute(sql, {'dump_id':dump_id})
        session.commit()
        
        print " - backfilling shape_dist_traveled"
        sql='''
UPDATE gtfs_versioned.stop_times SET shape_dist_traveled=ST_Line_Locate_Point(canonical_patterns.geom, stops.geom) * canonical_patterns.max_len 
FROM gtfs_versioned.canonical_patterns, gtfs_versioned.stops
WHERE stop_times.dump_id=:dump_id AND 
canonical_patterns.dump_id=:dump_id AND 
stops.dump_id=:dump_id AND 
stop_times.trip_id=canonical_patterns.trip_id AND
stop_times.stop_id=stops.stop_id
'''
        session.execute(sql, {'dump_id':dump_id})
        session.commit()
        session.close()
   
    @classmethod
    def setupStopPatternsView(cls, engine):
        Session = sessionmaker(bind=engine)
        session = Session()
        session.execute('DROP VIEW IF EXISTS gtfs_versioned.pattern_stops')
        session.execute('''
CREATE VIEW gtfs_versioned.pattern_stops
AS
(SELECT canonical_patterns.dump_id, canonical_patterns.service_id, canonical_patterns.route_id, canonical_patterns.direction_id, stop_times.stop_id, stop_times.stop_sequence, stop_times.shape_dist_traveled, stops.geom, stop_times.timepoint, stops.stop_name
 FROM gtfs_versioned.canonical_patterns, gtfs_versioned.trips, gtfs_versioned.stop_times, gtfs_versioned.stops
 WHERE
 canonical_patterns.trip_id=trips.trip_id AND
 canonical_patterns.dump_id=trips.dump_id AND
 trips.trip_id=stop_times.trip_id AND
 canonical_patterns.dump_id=stop_times.dump_id AND
 canonical_patterns.dump_id=stops.dump_id AND
 stops.stop_id=stop_times.stop_id
)
''')
        session.commit()
        session.close()
