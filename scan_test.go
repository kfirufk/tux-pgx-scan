package main

import (
	"context"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/r3labs/diff/v2"
	"log"
	"testing"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "ufk"
	password = "your-password"
	dbname   = "mycw"
)

const (
	queryFooTestRowA = `select 1 as i1, 2 as i2, 'a' as s1, 'b' as s2, 5.2 as f1, 5.4 as f2, 6.1 as ff1, 6.2 as ff2,
       '{1,2}'::int[] as ia1,'{3,4}'::int[] as ia2, '{6,7}'::int[] as ia3, '{foo,bar}'::text[] as sa1,
       '{moshe,haim}'::text[] as sa2, '{moshe2,haim2}'::text[] as sa3, '{3.3,2.3}'::float[] as fa1,
       '{4.5,6.43}'::float[] as fa2,'{1.123,2.342}'::float[] as faf1,'{63.233,6.245}'::float[] as faf2,
       '{2.222,54.32}'::float[] as faf3`
	queryFooTestRowB = `select 11 as i1, 12 as i2, '1a' as s1, '1b' as s2, 15.2 as f1, 15.4 as f2, 16.1 as ff1,16.2 as ff2,
		'{9,1,2}'::int[] as ia1,'{8,3,4}'::int[] as ia2, '{7,6,7}'::int[] as ia3, '{1,foo,bar}'::text[] as sa1,
		'{2,moshe,haim}'::text[] as sa2, '{3,moshe2,haim2}'::text[] as sa3, '{4,3.3,2.3}'::float[] as fa1,
		'{5,4.5,6.43}'::float[] as fa2,'{6,1.123,2.342}'::float[] as faf1,'{7,63.233,6.245}'::float[] as faf2,
		'{8,2.222,54.32}'::float[] as faf3`
)

type fooTest struct {
	I1   int
	I2   *int
	S1   string
	S2   *string
	F1   pgtype.Numeric
	F2   *pgtype.Numeric
	Ff1  float64
	Ff2  *float64
	Ia1  []int
	Ia2  []*int
	Ia3  *[]int
	Sa1  []string
	Sa2  []*string
	Sa3  *[]string
	Fa1  pgtype.Float8Array
	Fa2  *pgtype.Float8Array
	Faf1 []float64
	Faf2 []*float64
	Faf3 *[]float64
}


func GetDbConnection() (*pgxpool.Pool, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	return pgxpool.Connect(context.Background(), psqlInfo)
}

func TestBasicTypesInStuctInSlice(t *testing.T) {
	sqlQuery := queryFooTestRowA + " union " + queryFooTestRowB
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var bar []fooTest
		if err := MyQuery(context.Background(), conn, &bar, sqlQuery); err != nil {
			t.Error(err)
		} else {
				log.Print("A")
		}
	}
}

func TestBasicTypesInStruct(t *testing.T) {
	sqlQuery := queryFooTestRowA
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var bar fooTest
		if err := MyQuery(context.Background(), conn, &bar, sqlQuery); err != nil {
			t.Error(err)
		} else {

			faf20 := 63.233
			faf21 := 6.245
			faf3 := []float64{2.222,54.32}
			fa1f := []float64{1.123,2.342}
			fa1 := pgtype.Float8Array{

			}
			fa1.Set(fa1f)

			fa2f := []float64{4.5,6.43}
			fa2 := pgtype.Float8Array{

			}
			fa2.Set(fa2f)

			f1f := 5.2
			F1 := pgtype.Numeric{}
			F1.Set(f1f)

			f2f := 5.4
			F2 := pgtype.Numeric{}
			F2.Set(f2f)

			ia3:= []int{1,2}

			s2 := "b"
			moshe := "moshe"
			haim := "haim"
			sa3 := []string{moshe,haim}
			num2 := 2
			num1 := 1
			f62 := 6.2
			fooFoo := fooTest{
				Faf1: []float64{1.123,2.342},
				Faf2: []*float64{&faf20,&faf21},
				Faf3: &faf3,
				Fa1: fa1,
				Fa2: &fa2,
				F1: F1,
				F2: &F2,
				S1: "a",
				S2: &s2,
				Sa1: []string{"foo","bar"},
				Sa2: []*string{&moshe,&haim},
				Sa3: &sa3,
				I1: 1,
				I2: &num2,
				Ia1: []int{1,2},
				Ia2: []*int{&num1,&num2},
				Ia3: &ia3,
				Ff1: 6.1,
				Ff2: &f62,
			}
			changeLog, err := diff.Diff(fooFoo,bar,diff.)
			if err != nil {
				t.Errorf("internal error comparing structs: %v",err)
			}
			log.Println(changeLog)
			if !cmp.Equal(fooFoo, bar,cmp.AllowUnexported(fooTest{})) {
				t.Errorf("failed test: %v",cmp.Diff(fooFoo, bar))
			}
			if bar.Faf1[0] != 1.123 || bar.Faf1[1] != 2.342 {
				t.Errorf("foo.Faf1([]float64) != {1.123,2.342}")
			}
			if *bar.Faf2[0] != 63.233 || *bar.Faf2[1] != 6.245 {
				t.Errorf("foo.Faf2([]*float64) != {63.233,6.245}")
			}
			if (*bar.Faf3)[0] != 2.222 || (*bar.Faf3)[1] != 54.32 {
				t.Errorf("foo.Faf3([]*float64) != {2.222,54.34}")
			}
			if bar.Fa1.Elements[0].Float != 3.3 || bar.Fa1.Elements[1].Float != 2.3 {
				t.Errorf("foo.Fa1(pgtype.Float8Array) != {3.3,2.3}")
			}
			if bar.Fa2.Elements[0].Float != 4.5 || bar.Fa2.Elements[1].Float != 6.43 {
				t.Errorf("foo.Fa2(pgtype.Float8Array) != {4.5,6.43}")
			}

			if *bar.Ia2[0] != 3 || *bar.Ia2[1] != 4 {
				t.Errorf("foo.Ia2([]*int) != {3,4}")
			}
			if (*bar.Ia3)[0] != 6 || (*bar.Ia3)[1] != 7 {
				t.Errorf("foo.Ia3(*[]int) != {6,7}")
			}
			if bar.Sa1[0] != "foo" || bar.Sa1[1] != "bar" {
				t.Errorf("foo.Sa1([]string) != {foo,bar}")
			}

			if *bar.Sa2[0] != "moshe" || *bar.Sa2[1] != "haim" {
				t.Errorf("foo.Sa2([]*string) != {moshe,haim}")
			}
			if (*bar.Sa3)[0] != "moshe2" || (*bar.Sa3)[1] != "haim2" {
				t.Errorf("foo.Sa3(*[]string) != {moshe,haim}")
			}

			if bar.Ia1[0] != 1 || bar.Ia1[1] != 2 {
				t.Errorf("foo.Ia1([]int) != {1,2}")
			}
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
