package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"testing"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "ufk"
	password = "your-password"
	dbname   = "mycw"
)

type fooTest struct {
	I1  int
	I2  *int
	S1  string
	S2  *string
	F1  pgtype.Numeric
	F2  *pgtype.Numeric
	Ff1 float64
	Ff2 *float64
}

func GetDbConnection() (*pgxpool.Pool, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	return pgxpool.Connect(context.Background(), psqlInfo)
}

func TestBasicTypesInStruct(t *testing.T) {
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var bar fooTest
		if err := MyQuery(context.Background(), conn, &bar, "select 1 as i1, 2 as i2,  'a' as s1, 'b' as s2, 5.2 as f1, 5.4 as f2, 6.1 as ff1, 6.2 as ff2"); err != nil {
			t.Error(err)
		} else {
			if bar.I1 != 1 {
				t.Error("foo.I1(int) != 1")
			}
			if *bar.I2 != 2 {
				t.Error("foo.I2(*int) != 2")
			}
			if bar.S1 != "a" {
				t.Error("foo.S1(string) != 'a'")
			}
			if *bar.S2 != "b" {
				t.Error("foo.S2(*string) != 'b'")
			}
			var f1 float64
			bar.F1.AssignTo(&f1)
			if f1 != 5.2 {
				t.Error("foo.F1(pgtype.Numeric) != 5.2")
			}
			var f2 float64
			bar.F2.AssignTo(&f2)
			if f2 != 5.4 {
				t.Error("foo.F2(*pgtype.Numeric) != 5.4")
			}
			if bar.Ff1 != 6.1 {
				t.Error("foo.FF1(float64) != 6.1")
			}
			if *bar.Ff2 != 6.2 {
				t.Error("foo.FF2(*float64) != 6.2")
			}
		}
	}
}
