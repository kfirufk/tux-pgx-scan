package main

import (
	"context"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"math/big"
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

func getTestRow1() fooTest {
	faf20 := 63.233
	faf21 := 6.245
	faf3 := []float64{2.222, 54.32}
	fa1f := []float64{3.3, 2.3}
	fa1 := pgtype.Float8Array{}
	fa1.Set(fa1f)

	fa2f := []float64{4.5, 6.43}
	fa2 := pgtype.Float8Array{}
	fa2.Set(fa2f)

	f1f := 5.2
	F1 := pgtype.Numeric{}
	F1.Set(f1f)

	f2f := 5.4
	F2 := pgtype.Numeric{}
	F2.Set(f2f)

	ia3 := []int{6, 7}

	s2 := "b"
	moshe := "moshe"
	haim := "haim"
	sa3 := []string{"moshe2", "haim2"}
	num2 := 2
	num3 := 3
	num4 := 4
	f62 := 6.2
	fooFoo := fooTest{
		Faf1: []float64{1.123, 2.342},
		Faf2: []*float64{&faf20, &faf21},
		Faf3: &faf3,
		Fa1:  fa1,
		Fa2:  &fa2,
		F1:   F1,
		F2:   &F2,
		S1:   "a",
		S2:   &s2,
		Sa1:  []string{"foo", "bar"},
		Sa2:  []*string{&moshe, &haim},
		Sa3:  &sa3,
		I1:   1,
		I2:   &num2,
		Ia1:  []int{1, 2},
		Ia2:  []*int{&num3, &num4},
		Ia3:  &ia3,
		Ff1:  6.1,
		Ff2:  &f62,
	}
	return fooFoo
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
			fooFoo := getTestRow1()
			if !cmp.Equal(fooFoo, bar, cmp.AllowUnexported(big.Int{})) {
				t.Errorf("failed test: %v", cmp.Diff(fooFoo, bar, cmp.AllowUnexported(big.Int{})))
			}

		}
	}
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
			fooFoo := getTestRow1()
			if !cmp.Equal(fooFoo, bar[0], cmp.AllowUnexported(big.Int{})) {
				t.Errorf("failed test: %v", cmp.Diff(fooFoo, bar, cmp.AllowUnexported(big.Int{})))
			}
		}
	}
}
