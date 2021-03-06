import csv
import datetime
import os
import sys
import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from gtfsdb import util


class _Base(object):

    required_fields = []
    optional_fields = []
    proposed_fields = []

    @classmethod
    def from_dict(cls, attrs, engine, dump_id):
        clean_dict = cls.make_record(attrs, engine, dump_id)
        c = cls(**clean_dict)
        return c

    def to_dict(self):
        ''' convert a SQLAlchemy object into a dict that is serializable to JSON
        ''' 
        ret_val = self.__dict__.copy()

        ''' not crazy about this hack, but ... 
            the __dict__ on a SQLAlchemy object contains hidden crap that we delete from the class dict
        '''
        if set(['_sa_instance_state']).issubset(ret_val):
            del ret_val['_sa_instance_state']

        ''' we're using 'created' as the date parameter, so convert values to strings
            TODO: better would be to detect date & datetime objects, and convert those...
        '''
        if set(['created']).issubset(ret_val):
            ret_val['created'] = ret_val['created'].__str__();

        return ret_val 

    @classmethod
    def get_filename(cls):
        return '%s.txt' %(cls.__tablename__)

    @classmethod
    def load(cls, engine, directory=None, validate=True, dump_id=None, flush_interval=10000, merge=False):
        records = []
        file_path = '%s/%s' %(directory, cls.get_filename())
        
        if os.path.exists(file_path):
            start_time = time.time()
            file = open(file_path, 'r')
            utf8_file = util.UTF8Recoder(file, 'utf-8-sig')
            reader = csv.DictReader(utf8_file)
            if validate:
                cls.validate(reader.fieldnames)
            s = ' - %s ' %(cls.get_filename())
            sys.stdout.write(s)
            table = cls.__table__
            #engine.execute(table.delete())
            i = 0
            
            Session = sessionmaker(bind=engine)
            session = Session()
            
            for row in reader:
                if merge:
                    session.merge(cls.from_dict(row, engine, dump_id))
                else:
                    session.add(cls.from_dict(row, engine, dump_id))
#                records.append(cls.make_record(row, engine, dump_id))
                i += 1
#                # commit every 10,000 records to the database to manage memory usage
                if i >= flush_interval:
                    session.commit()
#                    engine.execute(table.insert(), records)
                    sys.stdout.write('*')
                    sys.stdout.flush()
#                    records = []
                    i = 0
#            if len(records) > 0:
#                engine.execute(table.insert(), records)
            session.commit()
            file.close()
            processing_time = time.time() - start_time
            print ' (%.0f seconds)' %(processing_time)

    @classmethod
    def make_record(cls, row, engine, dump_id):
        # clean dict
        for k, v in row.items():
            if isinstance(v, basestring):
                row[k] = v.strip()
            if (k not in cls.__table__.c):
                del row[k]
            elif not v:
                row[k] = None
            elif k.endswith('date'):
                row[k] = datetime.datetime.strptime(v, '%Y%m%d').date()
    
        # add geometry to dict
        if hasattr(cls, 'add_geom_to_dict'):
            cls.add_geom_to_dict(row)

        # add dump id
        if dump_id != None:
            row['dump_id']=dump_id
        
        return row

    @classmethod
    def set_schema(cls, schema):
        cls.__table__.schema = schema

    @classmethod
    def validate(cls, fieldnames):
        all_fields = cls.required_fields + cls.optional_fields + cls.proposed_fields

        # required fields
        fields = None
        if cls.required_fields and fieldnames:
            fields = set(cls.required_fields) - set(fieldnames)
        if fields:
            missing_required_fields = list(fields)
            if missing_required_fields:
                print ' %s missing fields: %s' %(cls.get_filename(), missing_required_fields)

        # all fields
        fields = None
        if all_fields and fieldnames:
            fields = set(fieldnames) - set(all_fields)
        if fields:
            unknown_fields = list(fields)
            if unknown_fields:
                print ' %s unknown fields: %s' %(cls.get_filename(), unknown_fields)


Base = declarative_base(cls=_Base)
