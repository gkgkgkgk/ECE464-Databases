from sqlalchemy import create_engine, func, Integer, String, Column, DateTime, exists, desc, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import rank
from random import randrange

base = declarative_base()
engine = create_engine("mysql+pymysql://root:gkgkgkgk@localhost/class")
db = Session(engine)

class sailors(base):
    __tablename__ = "sailors"
    sid = Column(Integer, primary_key=True)
    sname = Column(String)
    rating = Column(Integer)
    age = Column(Integer)

class boats(base):
    __tablename__ = "boats"
    bid = Column(Integer, primary_key=True)
    bname = Column(String)
    color = Column(String)
    length = Column(Integer)

class reserves(base):
    __tablename__ = "reserves"
    sid = Column(Integer, primary_key=True)
    bid = Column(Integer, primary_key=True)
    day = Column(DateTime, primary_key=True)

# boats need to be inspected once a month.
# the following table keeps track of every time a boat was inspected and the id of the person who inspected it.
class inspections(base):
    __tablename__ = "inspections"
    bid = Column(Integer, primary_key=True)
    day = Column(DateTime, primary_key=True)
    iid = Column(Integer)

base.metadata.create_all(engine)

connection = engine.connect()

def init_inspections():
    inspections.__table__.create(engine)
    # get all the boats
    boats_query = db.query(boats.bid, boats.bname, boats.color, boats.length).all()

    for b in boats_query:
        date = '1998/'+str(randrange(1, 12))+'/'+str(randrange(1, 30))
        i = inspections(bid=b[0], day=date, iid = 1)
        db.add(i)
        db.commit()

# generate a report to get the boats that need an inspection (apparently the year is 1998, so the current date will be 10/14/1998)
def get_inspections():
    q = db.query(inspections.bid, boats.bname).where(func.datediff('1998/10/14', inspections.day) > 30).join(boats, boats.bid == inspections.bid).all()
    return q
    
get_inspections()