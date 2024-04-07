package models

import "time"

type Student struct {
	Id         int
	Name       string
	Email      string
	Address    string
	Birth_date time.Time
	Gender     string
}
