from sqlalchemy import create_engine, func, Integer, String, Column, DateTime, exists, desc, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import rank

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

base.metadata.create_all(engine)

connection = engine.connect()


def query1(): # DONE
    # select DISTINCT reserves.bid, boats.bname, count(*) from reserves inner join boats using(bid) group by reserves.bid;
    # List, for every boat, the number of times it has been reserved, excluding those boats that have never been reserved (list the id and the name).
    q1 = db.query(boats.bid, boats.bname).where(exists().where(reserves.bid == boats.bid)).subquery()
    q = db.query(q1.c.bid, q1.c.bname, func.count(reserves.bid)).join(reserves, q1.c.bid == reserves.bid).group_by(q1.c.bid).all()
    return q

def query2(): # DONE
    # List those sailors who have reserved every red boat (list the id and the name).
    # select s.sname, s.sid from sailors s where not exists (select b.bid from boats b where b.color = 'red' and not exists (select * from reserves r where r.bid = b.bid and r.sid = s.sid));
    
    q1 = db.query(func.count(boats.bid)).filter(boats.color == 'red').scalar()
    q2 = db.query(reserves.sid, reserves.bid, boats.bid, boats.color).join(boats, reserves.bid == boats.bid).filter(boats.color == 'red').subquery()
    q = db.query(sailors.sid, sailors.sname, func.count(q2.c.sid)).join(q2, sailors.sid == q2.c.sid).group_by(sailors.sid).having(func.count(q2.c.sid) == q1).all()

    for i in q:
        print(i)
    return q
    

def query3(): # DONE
    #  List those sailors who have reserved only red boats.
    # select distinct s.sid, s.sname from sailors s, reserves r, boats b where r.bid = b.bid and s.sid = r.sid and b.color = "red" and s.sid not in(select s.sid from sailors s, reserves r, boats b where s.sid=r.sid and r.bid=b.bid and b.color != "red");
    # get all sailors who reserved non red boats
    q1 = db.query(reserves.sid).distinct().join(boats, reserves.bid == boats.bid).filter(boats.color != 'red').subquery()
    q2 = db.query(sailors.sid, sailors.sname).where(~exists().where(q1.c.sid == sailors.sid)).subquery()
    q = db.query(q2.c.sid, q2.c.sname).distinct().filter(reserves.sid == q2.c.sid).all()
    return q
    

def query4(): # DONE
    # For which boat are there the most reservations?
    # select boats.bid, boats.bname, count(*) from boats inner join reserves using(bid) group by boats.bid order by count(*) desc limit 1;
    q = db.query(boats.bid, boats.bname, func.count(reserves.bid)).join(reserves, boats.bid == reserves.bid).group_by(boats.bid).order_by(desc(func.count(reserves.bid))).first()
    return q

def query5(): # DONE
    # Select all sailors who have never reserved a red boat.
    # select sid, sname from sailors where not exists(select * from reserves where reserves.sid=sailors.sid and reserves.bid in (select bid from boats where color='red'));
    q1 = db.query(reserves.sid, boats.bid, boats.color).distinct().join(boats, reserves.bid == boats.bid).filter(boats.color == 'red').subquery()
    q = db.query(sailors.sid, sailors.sname).distinct().where(~exists().where(q1.c.sid == sailors.sid)).all()
    return q

def query6(): # DONE
    # Find the average age of sailors with a rating of 10.
    # select avg(age) from sailors where rating=10;
    q = db.query(func.avg(sailors.age)).filter(sailors.rating == 10).first()
    return q

def query7(): # DONE
    # For each rating, find the name and id of the youngest sailor.
    # select rating, sname, sid, age from (select rating, sid, sname, age, rank() over (partition by rating order by age) as r from sailors) as t where r=1;
    q1 = db.query(sailors.sname, sailors.sid, sailors.rating, sailors.age, func.rank().over(partition_by=sailors.rating, order_by=sailors.age).label('r')).subquery()
    q = db.query(q1.c.rating, q1.c.sname, q1.c.sid, q1.c.age).filter(q1.c.r == 1).all()
    return q

def query8():
    # Select, for each boat, the sailor who made the highest number of reservations for that boat.
    # select sid, sname, bid, c from (select sid, sname, bid, c, rank() over (partition by bid order by c desc) as rnk from (select s.sid, s.sname, b.bid, count(*) as c from sailors s, reserves r, boats b where b.bid = r.bid and s.sid = r.sid group by b.bid, s.sid) as t_1 ) as t_2 where rnk = 1;
    q1 = db.query(reserves.sid, sailors.sname, reserves.bid, func.count(reserves.bid).label('c')).group_by(boats.bid, sailors.sid).subquery()
    for row in q1:
        print(row)
    
    


query2()