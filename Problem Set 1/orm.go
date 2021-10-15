package main

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"fmt"
	"time"
)
  
type Sailor struct {
	Sid int `gorm:"primaryKey"`
	Sname string
	Rating int
	Age int
}

type Boat struct {
	Bid int `gorm:"primaryKey"`
	Bname string
	Color string
	Length int
}

type Reserves struct {
	Bid int
	Sid int
	Day time.Time
}

func main() {
	// refer https://github.com/go-sql-driver/mysql#dsn-data-source-name for details
	dsn := "root:gkgkgkgk@tcp(127.0.0.1:3306)/class?charset=utf8&parseTime=True"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	// check if the connection is successful
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Connected to database successfully.")
	}

	db.AutoMigrate(&Sailor{}, &Boat{})

	query8(db);
	
}

func query1 (db *gorm.DB) {	// WRONG
	// select DISTINCT reserves.bid, boats.bname, count(*) from reserves inner join boats using(bid) group by reserves.bid;
	var result []struct {
		Bid int
		Bname string
		Count int
	}
	
	db.Table("reserves").Select("DISTINCT reserves.bid, boats.bname, count(*)").Joins("inner join boats using(bid)").Group("reserves.bid").Scan(&result)
	fmt.Println("Query 1:", result)
}

func query2 (db *gorm.DB){
	//select s.sname, s.sid from sailors s where not exists (select b.bid from boats b where b.color = 'red' and not exists (select * from reserves r where r.bid = b.bid and r.sid = s.sid));
	var result []struct {
		Sname string
		Sid int
	}
	db.Table("sailors").Select("s.sname, s.sid").Joins("inner join boats b on b.color = 'red' and not exists (select * from reserves r where r.bid = b.bid and r.sid = s.sid)").Scan(&result)
	fmt.Println("Query 2:", result)
}

func query3(db *gorm.DB) { // WRONG
	// select distinct s.sid, s.sname from sailors s, reserves r, boats b where r.bid = b.bid and s.sid = r.sid and b.color = "red" and s.sid not in(select s.sid from sailors s, reserves r, boats b where s.sid=r.sid and r.bid=b.bid and b.color != "red");
	var result []struct {
		Sid int
		Sname string
	}

	db.Table("sailors").Select("DISTINCT s.sid, s.sname").Joins("inner join reserves r on r.bid = b.bid and s.sid = r.sid and b.color = 'red' and s.sid not in(select s.sid from sailors s, reserves r, boats b where s.sid=r.sid and r.bid=b.bid and b.color != 'red')").Scan(&result)
	fmt.Println("Query 3:", result)
}

func query4 (db *gorm.DB) { // wrong
	//select boats.bid, boats.bname, count(*) from boats inner join reserves using(bid) group by boats.bid order by count(*) desc limit 1;
	var result []struct {
		Bid int
		Bname string
		Count int
	}


	db.Table("boats").Select("boats.bid, boats.bname").Scan(&result)
	fmt.Println("Query 4:", result)
}


func query6 (db *gorm.DB) {
	var averageAge float64
	db.Table("sailors").Select("avg(age)").Where("sailors.rating = 10").Scan(&averageAge)
	fmt.Println("Query 6:", averageAge)	
}

func query7 (db *gorm.DB) { // wrong
	//select rating, sname, sid, age from (select rating, sid, sname, age, rank() over (partition by rating order by age) as r from sailors) as t where r=1;
	var result []struct {
		Rating int
		Sname string
		Sid int
		Age int
	}

	db.Table("sailors").Select("rating, sname, sid, age").Joins("inner join (select rating, sid, sname, age, rank() over (partition by rating order by age) as r from sailors) as t on t.sid = sailors.sid and t.r = 1").Scan(&result)
	fmt.Println("Query 7:", result)
}

func query8 (db *gorm.DB) { // wrong
	//select sid, sname, bid, c from (select sid, sname, bid, c, rank() over (partition by bid order by c desc) as rnk from (select s.sid, s.sname, b.bid, count(*) as c from sailors s, reserves r, boats b where b.bid = r.bid and s.sid = r.sid group by b.bid, s.sid) as t_1 ) as t_2 where rnk = 1;
	var result []struct {
		Sid int
		Sname string
		Bid int
		C int
	}

	db.Table("sailors").Select("sailors.sid, sailors.sname, boats.bid, count(*) as c").Joins("inner join (select sailors.sid, sailors.sname, boats.bid, count(*) as c from sailors s, reserves r, boats b where b.bid = r.bid and sailors.sid = r.sid group by b.bid, sailors.sid) as t_1 on t_1.bid = b.bid and t_1.sid = s.sid").Scan(&result)
	fmt.Println("Query 8:", result)
}