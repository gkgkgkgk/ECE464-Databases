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


def query1():
    # List, for every boat, the number of times it has been reserved, excluding those boats that have never been reserved (list the id and the name).
    # select DISTINCT reserves.bid, boats.bname, count(*) from reserves inner join boats using(bid) group by reserves.bid;
    q1 = db.query(boats.bid, boats.bname).where(exists().where(reserves.bid == boats.bid)).subquery() # get all boats with a reservation
    q = db.query(q1.c.bid, q1.c.bname, func.count(reserves.bid)).join(reserves, q1.c.bid == reserves.bid).group_by(q1.c.bid).all()
    return q

def query2():
    # List those sailors who have reserved every red boat (list the id and the name).
    # select s.sname, s.sid from sailors s where not exists (select b.bid from boats b where b.color = 'red' and not exists (select * from reserves r where r.bid = b.bid and r.sid = s.sid));
    q1 = db.query(func.count(boats.bid)).filter(boats.color == 'red').scalar() # get amount of red boats
    q2 = db.query(reserves.sid, reserves.bid, boats.bid, boats.color).join(boats, reserves.bid == boats.bid).filter(boats.color == 'red').subquery() # get all reservations with red boats
    q = db.query(sailors.sid, sailors.sname, func.count(q2.c.sid)).join(q2, sailors.sid == q2.c.sid).group_by(sailors.sid).having(func.count(q2.c.sid) == q1).all() # get all sailors who reserved every red boat
    return q

def query3():
    #  List those sailors who have reserved only red boats.
    # select distinct s.sid, s.sname from sailors s, reserves r, boats b where r.bid = b.bid and s.sid = r.sid and b.color = "red" and s.sid not in(select s.sid from sailors s, reserves r, boats b where s.sid=r.sid and r.bid=b.bid and b.color != "red");
    # get all sailors who reserved non red boats
    q1 = db.query(reserves.sid).distinct().join(boats, reserves.bid == boats.bid).filter(boats.color != 'red').subquery() # get all reservations/boats that are not red
    q2 = db.query(sailors.sid, sailors.sname).where(~exists().where(q1.c.sid == sailors.sid)).subquery() # get all sailors who are not on that list
    q = db.query(q2.c.sid, q2.c.sname).distinct().filter(reserves.sid == q2.c.sid).all() # get all sailors who reserved boats and are not on the non-red boat list
    return q

def query4():
    # For which boat are there the most reservations?
    # select boats.bid, boats.bname, count(*) from boats inner join reserves using(bid) group by boats.bid order by count(*) desc limit 1;
    q = db.query(boats.bid, boats.bname, func.count(reserves.bid)).join(reserves, boats.bid == reserves.bid).group_by(boats.bid).order_by(desc(func.count(reserves.bid))).first() # count amount of reservations per boat, get the first one
    return q

def query5():
    # Select all sailors who have never reserved a red boat.
    # select sid, sname from sailors where not exists(select * from reserves where reserves.sid=sailors.sid and reserves.bid in (select bid from boats where color='red'));
    q1 = db.query(reserves.sid, boats.bid, boats.color).distinct().join(boats, reserves.bid == boats.bid).filter(boats.color == 'red').subquery() # get all reservations with red boats
    q = db.query(sailors.sid, sailors.sname).distinct().where(~exists().where(q1.c.sid == sailors.sid)).all() # get all sailor ids that are not on that list
    return q

def query6():
    # Find the average age of sailors with a rating of 10.
    # select avg(age) from sailors where rating=10;
    q = db.query(func.avg(sailors.age)).filter(sailors.rating == 10).scalar()
    return q

def query7():
    # For each rating, find the name and id of the youngest sailor.
    # select rating, sname, sid, age from (select rating, sid, sname, age, rank() over (partition by rating order by age) as r from sailors) as t where r=1;
    q1 = db.query(sailors.sname, sailors.sid, sailors.rating, sailors.age, func.rank().over(partition_by=sailors.rating, order_by=sailors.age).label('r')).subquery() # get all sailors and rank them over each rating, ordered by age
    q = db.query(q1.c.rating, q1.c.sname, q1.c.sid, q1.c.age).filter(q1.c.r == 1).all() # get all sailors with rank 1 (aka the youngest one)
    return q

def query8():
    # Select, for each boat, the sailor who made the highest number of reservations for that boat.
    # select sid, sname, bid, c from (select sid, sname, bid, c, rank() over (partition by bid order by c desc) as rnk from (select s.sid, s.sname, b.bid, count(*) as c from sailors s, reserves r, boats b where b.bid = r.bid and s.sid = r.sid group by b.bid, s.sid) as t_1 ) as t_2 where rnk = 1;
    q = db.query(reserves.sid, sailors.sname, reserves.bid, func.count(reserves.bid).label('c')).join(sailors, sailors.sid == reserves.sid).group_by(reserves.bid, reserves.sid).order_by(reserves.bid).all() # note that this includes tie breakers
    return q

print(query8())

def test_query1():
    assert query1() == [(101, 'Interlake', 2), (102, 'Interlake', 3), (103, 'Clipper', 3), (104, 'Clipper', 5), (105, 'Marine', 3), (106, 'Marine', 3), (109, 'Driftwood', 4), (112, 'Sooney', 1), (110, 'Klapser', 3), (107, 'Marine', 1), (111, 'Sooney', 1), (108, 'Driftwood', 1)]

def test_query2():
    assert query2() == []

def test_query3():
    assert query3() == [(23, 'emilio'), (24, 'scruntus'), (35, 'figaro'), (61, 'ossola'), (62, 'shaun')]

def test_query4():
    assert query4() == (104, 'Clipper', 5)

def test_query5():
    assert query5() == [(29, 'brutus'), (32, 'andy'), (58, 'rusty'), (60, 'jit'), (71, 'zorba'), (74, 'horatio'), (85, 'art'), (90, 'vin'), (95, 'bob')]

def test_query6():
    assert query6() == 35.0

def test_query7():
    assert query7() == [(1, 'scruntus', 24, 33), (1, 'brutus', 29, 33), (3, 'art', 85, 25), (3, 'dye', 89, 25), (7, 'ossola', 61, 16), (7, 'horatio', 64, 16), (8, 'andy', 32, 25), (8, 'stum', 59, 25), (9, 'horatio', 74, 25), (9, 'dan', 88, 25), (10, 'rusty', 58, 35), (10, 'jit', 60, 35), (10, 'shaun', 62, 35), (10, 'zorba', 71, 35)]

def test_query8():
    assert query8() == [(22, 'dusting', 101, 1), (64, 'horatio', 101, 1), (22, 'dusting', 102, 1), (31, 'lubber', 102, 1), (64, 'horatio', 102, 1), (22, 'dusting', 103, 1), (31, 'lubber', 103, 1), (74, 'horatio', 103, 1), (22, 'dusting', 104, 1), (23, 'emilio', 104, 1), (24, 'scruntus', 104, 1), (31, 'lubber', 104, 1), (35, 'figaro', 104, 1), (23, 'emilio', 105, 1), (35, 'figaro', 105, 1), (59, 'stum', 105, 1), (59, 'stum', 106, 1), (60, 'jit', 106, 2), (88, 'dan', 107, 1), (89, 'dye', 108, 1), (59, 'stum', 109, 1), (60, 'jit', 109, 1), (89, 'dye', 109, 1), (90, 'vin', 109, 1), (62, 'shaun', 110, 1), (88, 'dan', 110, 2), (88, 'dan', 111, 1), (61, 'ossola', 112, 1)]