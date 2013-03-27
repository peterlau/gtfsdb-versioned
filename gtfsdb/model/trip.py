from geoalchemy import GeometryColumn, GeometryDDL, LineString, WKTSpatialElement
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relation
from sqlalchemy.sql import func
from gtfsdb.model.shape import Pattern
from gtfsdb.model.route import Route
from gtfsdb.model.calendar import UniversalCalendar

from .base import Base
from sqlalchemy.schema import Sequence
from sqlalchemy.orm import sessionmaker


class Trip(Base):
    __tablename__ = 'trips'

    required_fields = ['route_id', 'service_id', 'trip_id']
    optional_fields = [
        'trip_headsign',
        'trip_short_name',
        'direction_id',
        'block_id',
        'shape_id'
    ]
    proposed_fields = ['trip_type', 'trip_bikes_allowed']
    dump_id = Column(Integer, primary_key=True, nullable=False)
    trip_id = Column(Integer, primary_key=True, nullable=False)
    route_id = Column(String, nullable=False)
    service_id = Column(String, nullable=False)    
    trip_headsign = Column(String)
    trip_short_name = Column(String)
    direction_id = Column(Integer)
    block_id = Column(Integer)
    shape_id= Column(Integer, nullable=True)
    trip_type = Column(String)
    trip_bikes_allowed = Column(Integer)
    
    @classmethod
    def add_geometry_column(cls):
        cls.geom = GeometryColumn(LineString(2))
        GeometryDDL(cls.__table__)
            
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
    
        # add geometry from stop_times        
        trip_id=row['trip_id']
        Session = sessionmaker(engine)
        session = Session()
        
        q = session.execute('''
SELECT ST_AsText(ST_MakeLine(geom)) geom FROM ( 
    SELECT stops.geom
    FROM gtfs_versioned.stop_times, gtfs_versioned.stops
    WHERE stop_times.dump_id=stops.dump_id
    AND stop_times.stop_id=stops.stop_id
    AND stop_times.dump_id=:dump_id
    AND stop_times.trip_id=:trip_id
    ORDER BY stop_times.stop_sequence) tmp_stops''', {'dump_id':dump_id, 'trip_id':trip_id})
       
        wkt=q.first()[0]
        row['geom']=WKTSpatialElement(wkt)
        
        # add dump id
        row['dump_id']=dump_id
        
        session.close()
        return row
#
#    def load_geometry(self, session):
#        from gtfsdb.model.stop_time import StopTime
#
#        if hasattr(self, 'geom'):
#            s = func.st_collect(Pattern.geom)
#            s = func.st_multi(s)
#            s = func.st_astext(s).label('geom')
#            q = session.query(s)
#            q = q.filter(Pattern.trips.any((Trip.route == self)))
#            self.geom = q.first().geom

    #route = relation(Route)
    #pattern = relation(Pattern, backref='trips')
#    universal_calendar = relation(
#        UniversalCalendar,
#        primaryjoin=((service_id == UniversalCalendar.service_id)),
#        foreign_keys=(service_id),
#        viewonly=True,
#        backref='trips'
#    )
