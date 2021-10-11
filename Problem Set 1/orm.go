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
	bid int `gorm:"primaryKey"`
	bname string
	color string
	length int
}
type Reserves struct {
	bid int
	sid int
	day time.Time
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
}