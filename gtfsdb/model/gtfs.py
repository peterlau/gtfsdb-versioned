from contextlib import closing
import time
import pkg_resources
import shutil
import subprocess
import sys
import tempfile
from urllib import urlretrieve
import zipfile
import os
import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .agency import Agency
from .base import Base
from .calendar import Calendar, CalendarDate, UniversalCalendar
from .fare import FareAttribute, FareRule
from .feed_info import FeedInfo
from .frequency import Frequency
from .route import Route, RouteType
from .shape import Pattern, Shape
from .stop_time import StopTime
from .stop import Stop
from .transfer import Transfer
from .trip import Trip
from .canonical_pattern import CanonicalPattern
from .dump import Dump

class GTFS(object):

    def __init__(self, file):
        self.file = file
        (self.local_file, headers) = urlretrieve(file)

    def load(self, db):
        """Load GTFS into database"""
        gtfs_directory = self.unzip()
        data_directory = pkg_resources.resource_filename('gtfsdb', 'data')

        filename=os.path.basename(self.local_file)
        dump_timestamp=datetime.datetime.strptime(filename, 'san-francisco-municipal-transportation-agency_%Y%m%d_%H%M.zip').date()
        print "Dump Timestamp: %s" % dump_timestamp
        
        dump_id=Dump.load(db.engine, filename)
        
        # load GTFS data files & transform/derive additional data
        # due to foreign key constraints these files need to be loaded in the appropriate order
        RouteType.load(db.engine, data_directory, merge=True)
        FeedInfo.load(db.engine, gtfs_directory, merge=True)
        Agency.load(db.engine, gtfs_directory, merge=True)
        Calendar.load(db.engine, gtfs_directory, dump_id=dump_id)
        CalendarDate.load(db.engine, gtfs_directory, dump_id=dump_id)
        UniversalCalendar.load(db.engine, dump_id=dump_id, merge=True, dump_timestamp=dump_timestamp)

        Route.load(db.engine, gtfs_directory, dump_id=dump_id)
        Stop.load(db.engine, gtfs_directory, dump_id=dump_id)
        
        Shape.load(db.engine, gtfs_directory, dump_id=dump_id)
        Pattern.load(db.engine, dump_id=dump_id)
        
        StopTime.load(db.engine, gtfs_directory, dump_id=dump_id)
        Trip.load(db.engine, gtfs_directory, dump_id=dump_id, flush_interval=1000)
        CanonicalPattern.load(db.engine, dump_id=dump_id)
        CanonicalPattern.setupStopPatternsView(db.engine)

#  Do not enable
#        Transfer.load(db.engine, gtfs_directory)
#        Frequency.load(db.engine, gtfs_directory)
#        FareAttribute.load(db.engine, gtfs_directory)
#        FareRule.load(db.engine, gtfs_directory)
        
        shutil.rmtree(gtfs_directory)
        # load derived geometries
        # currently only written for postgresql
        
#        dialect_name = db.engine.url.get_dialect().name
#        if db.is_geospatial and dialect_name == 'postgresql':
#            s = ' - %s geom' %(Route.__tablename__)
#            sys.stdout.write(s)
#            start_seconds = time.time()
#            Session = sessionmaker(bind=db.engine)
#            session = Session()
#            q = session.query(Route)
#            for route in q:
#                route.load_geometry(session)
#                session.merge(route)
#            session.commit()
#            session.close()
#            process_time = time.time() - start_seconds
#            print ' (%.0f seconds)' %(process_time)
  

    def validate(self):
        """Run transitfeed.feedvalidator"""
        path = os.path.join(
            pkg_resources.get_distribution('transitfeed').egg_info,
            'scripts/feedvalidator.py'
        )

        stdout, stderr = subprocess.Popen(
            [sys.executable, path, '--output=CONSOLE', self.local_file],
            stdout=subprocess.PIPE
        ).communicate()

        is_valid = True
        for line in str(stdout).splitlines():
            if line.startswith('ERROR'):
                is_valid = 'errors' not in line.lower()
                continue
        return is_valid, stdout

    def unzip(self, path=None):
        """Unzip GTFS files from URL/directory to path."""
        path = path if path else tempfile.mkdtemp()
        with closing(zipfile.ZipFile(self.local_file)) as zip:
            zip.extractall(path)
        return path
