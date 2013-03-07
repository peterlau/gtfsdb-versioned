import datetime
from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from .base import Base

#Base = declarative_base()

class Dump(Base):
    __tablename__ = 'dumps'

    dump_id = Column(Integer, primary_key=True, nullable=False)
    dump_date = Column(DateTime, default=datetime.datetime.now())
    filename = Column(String, unique=True)

    def __init__(self, filename):
        self.filename = filename
    
    @classmethod
    def load(cls, engine, filename):
        # Update dump info
        Session = sessionmaker(bind=engine)
        session=Session()
        
        new_dump=session.query(Dump).filter_by(filename=filename).first()
        
        if new_dump == None:                
            new_dump = Dump(filename)
            session.add(new_dump)
            session.commit()
        else:            
            print "Filename exists. Not creating new dump."          
        
        print "dump_id: %d" % new_dump.dump_id
        session.close()
        
        return new_dump.dump_id