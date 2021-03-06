import datetime
import sys
import time

from sqlalchemy import Column, Index
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Boolean, Date, Integer, String

from .base import Base
from sqlalchemy.schema import Sequence


__all__ = ['Calendar', 'CalendarDate', 'UniversalCalendar']


class Calendar(Base):
    __tablename__ = 'calendar'

    required_fields = [                       
        'service_id',
        'monday',
        'tuesday',
        'wednesday',
        'thursday',
        'friday',
        'saturday',
        'sunday',
        'start_date',
        'end_date'
    ]

    dump_id = Column(Integer, primary_key=True, nullable=False)
    service_id = Column(String, primary_key=True, nullable=False)
    monday = Column(Boolean, nullable=False)
    tuesday = Column(Boolean, nullable=False)
    wednesday = Column(Boolean, nullable=False)
    thursday = Column(Boolean, nullable=False)
    friday = Column(Boolean, nullable=False)
    saturday = Column(Boolean, nullable=False)
    sunday = Column(Boolean, nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    def weekday_list(self):
        list = []
        if self.monday:
            list.append(0)
        if self.tuesday:
            list.append(1)
        if self.wednesday:
            list.append(2)
        if self.thursday:
            list.append(3)
        if self.friday:
            list.append(4)
        if self.saturday:
            list.append(5)
        if self.sunday:
            list.append(6)
        return list

    def to_date_list(self):
        date_list = []
        d = self.start_date
        delta = datetime.timedelta(days=1)
        weekdays = self.weekday_list()
        while d <= self.end_date:
            if d.weekday() in weekdays:
                dict = {}
                dict['service_id'] = self.service_id
                dict['date'] = d
                dict['dump_id']=self.dump_id
                date_list.append(dict)
            d += delta
        return date_list

Index('%s_ix1' %(Calendar.__tablename__), Calendar.start_date, Calendar.end_date)


class CalendarDate(Base):
    __tablename__ = 'calendar_dates'

    required_fields = ['service_id', 'date', 'exception_type']

    dump_id = Column(Integer, primary_key=True)
    service_id = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    exception_type = Column(Integer, nullable=False)

    @property
    def is_addition(self):
        return self.exception_type == 1

    @property
    def is_removal(self):
        return self.exception_type == 2


class UniversalCalendar(Base):
    __tablename__ = 'universal_calendar'

    dump_id = Column(Integer)
    service_id = Column(String)
    date = Column(Date, primary_key=True)

    @classmethod
    def get_filename(cls):
        return None

    @classmethod
    def from_calendar_date(cls, calendar_date):
        uc = cls()
        uc.service_id = calendar_date.service_id
        uc.date = calendar_date.date
        uc.dump_id = calendar_date.dump_id
        return uc

    @classmethod
    def load(cls, engine, dump_id, merge, dump_timestamp):
        start_time = time.time()
        s = ' - %s' %(cls.__tablename__)
        sys.stdout.write(s)
        Session = sessionmaker(bind=engine)
        session = Session()
        q = session.query(Calendar).filter(Calendar.dump_id==dump_id)
        for calendar in q:
            rows = calendar.to_date_list()
            for row in rows:
                existingcount = session.query(UniversalCalendar).filter(UniversalCalendar.date==row['date']).count()
                
                if row['date'] >= dump_timestamp or existingcount == 0:
                    uc = cls(**row)
                    session.merge(uc)
        session.commit()
        
        q = session.query(CalendarDate).filter(CalendarDate.dump_id==dump_id)
        for calendar_date in q:
            existingcount = session.query(UniversalCalendar).filter(UniversalCalendar.date==row['date']).count()
            if calendar_date.date >= dump_timestamp or existingcount == 0:
                if calendar_date.is_addition:
                    uc = cls.from_calendar_date(calendar_date)
                    session.merge(uc)
                if calendar_date.is_removal:
                    kwargs = dict(service_id=calendar_date.service_id, date=calendar_date.date)
                    session.query(cls).filter_by(**kwargs).delete()
        session.commit()
        session.close()
        processing_time = time.time() - start_time
        print ' (%.0f seconds)' %(processing_time)

Index('%s_ix1' %(UniversalCalendar.__tablename__), UniversalCalendar.date)
