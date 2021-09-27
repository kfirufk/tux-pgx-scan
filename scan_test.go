package tux_pgx_scan

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"io"
	"math/big"
	"os"
	"testing"
	"time"
)

var host string
var port string
var user string
var pass string
var db string

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

type Role string

type UserInfo struct {
	Name       string
	Roles      *[]Role
	UserId     int
	ProfileDir string
}

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

type Profile struct {
	Name      string          `json:"name"`
	Articles  []*Article      `json:"articles"`
	Cocktails []*CocktailInfo `json:"cocktails"`
	JoinedAt  time.Time       `json:"joined_at"`
	Bio       *string         `json:"bio"`
}

type Article struct {
	AddedBy    *string        `json:"added_by"`
	ProfileDir string         `json:"profile_dir"`
	Title      string         `json:"title"`
	Ratings    *sql.NullInt64 `json:"ratings"`
	Desc       string         `json:"desc"`
	Source     *string        `json:"source"`
	Content    string         `json:"content"`
	CreatedAt  time.Time      `json:"created_at"`
}

type PurchaseProductSale struct {
	SaleText             string   `json:"saleText"`
	SaleProductPrice     float64  `json:"saleProductPrice"`
	BuyProductsLabels    []string `json:"BuyProductsLabels"`
	BuyProductsTitles    []string `json:"BuyProductsTitles"`
	SaleProductLabel     string   `json:"SaleProductLabel"`
	SaleProductTitle     string   `json:"SaleProductTitle"`
	ShowInCategoryLabels []string `json:"ShowInCategoryLabels"`
}

type CocktailInfo struct {
	Name       string         `json:"name"`
	BasedOn    pq.StringArray `json:"based_on"`
	AddedBy    *string        `json:"added_by"`
	ProfileDir *string        `json:"profile_dir"`
	Ratings    *sql.NullInt64 `json:"ratings"`
	CreatedAt  time.Time      `json:"created_at"`
}

type User struct {
	ProfileDir    string `json:"profile_dir"`
	IsImgVerified bool   `json:"is_img_verified"`
	Name          string `json:"name"`
}

type CocktailInfo2 struct {
	Name          string         `json:"name"`
	BasedOn       pq.StringArray `json:"based_on"`
	AddedBy       *User          `json:"added_by"`
	ProfileDir    *string        `json:"profile_dir"`
	Ratings       *sql.NullInt64 `json:"ratings"`
	CreatedAt     time.Time      `json:"created_at"`
	IsImgVerified bool           `json:"is_img_verified"`
}

type ContainsID struct {
	ID int `json:"id"`
}

func Initialize() {
	host = os.Getenv("PGSQLDB_HOST")
	if host == "" {
		host = "localhost"
	}
	port = os.Getenv("PGSQLDB_PORT")
	if port == "" {
		port = "5432"
	}
	db = os.Getenv("PGSQLDB_DB")
	if db == "" {
		db = "postgres"
	}
	user = os.Getenv("PGSQLDB_USER")
	if user == "" {
		user = "postgres"
	}
	pass = os.Getenv("PGSQLDB_PASS")

}

func TestMain(m *testing.M) {
	Initialize()
	os.Exit(m.Run())
}

func GetDbConnection() (*pgxpool.Pool, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"dbname=%s sslmode=disable password=%s",
		host, port, user, db, pass)
	conn, err := pgxpool.Connect(context.Background(), psqlInfo)
	if err != nil {
		io.WriteString(os.Stderr,
			"these tests are connecting to the database and querying static data, do not load any tables or anything\n"+
				"this tests needs the following ENV: PGSQLDB_HOST (default localhost),PGSQLDB_PORT (default 5432),PGSQLDB_DB (default postgres),PGSQLDB_USER (default postgres,PGSQLDB_PASS\n")
	}
	return conn, err
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
		if _, err := MyQuery(context.Background(), conn, &bar, sqlQuery); err != nil {
			t.Error(err)
		} else {
			fooFoo := getTestRow1()
			if !cmp.Equal(fooFoo, bar, cmp.AllowUnexported(big.Int{})) {
				t.Errorf("failed test: %v", cmp.Diff(fooFoo, bar, cmp.AllowUnexported(big.Int{})))
			}

		}
	}
}

func TestGetIdColumn(t *testing.T) {
	sqlQuery := `select 123 as id`
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var bar ContainsID
		if isEmpty, err := MyQuery(context.Background(), conn, &bar, sqlQuery); err != nil {
			t.Error(err)
		} else if isEmpty {
			t.Error("failed test: query result returned empty!")
		} else {
			if bar.ID != 123 {
				t.Errorf("failed test.  ID 123 != %v", bar.ID)
			}

		}
	}
}

func TestScan(t *testing.T) {
	sqlQuery := `select 5, 'moshe';`
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		if row, isEmpty, err := MyQueryScan(context.Background(), conn, sqlQuery); err != nil {
			t.Error(err)
		} else if isEmpty {
			t.Error("row resulted empty!")
		} else {
			var num int
			var name string
			if err := row.Scan(&num, &name); err != nil {
				t.Error(err)
			} else {
				if num != 5 {
					t.Errorf("num != original value 5 != '%v'", num)
				}
				if name != "moshe" {
					t.Errorf("name != original value moshe != '%v'", name)
				}
			}
		}
	}
}

func TestUUIDPgSql13(t *testing.T) {
	shortUuid := "4013f6517888474c90e2f68b74e12f99"
	sqlQuery := `select '4013f651-7888-474c-90e2-f68b74e12f99'::uuid`
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var bar string
		if _, err := MyQuery(context.Background(), conn, &bar, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if bar != shortUuid {
				t.Errorf("uuid test failed, return uuid %v is different then expected %v", bar, shortUuid)
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
		if _, err := MyQuery(context.Background(), conn, &bar, sqlQuery); err != nil {
			t.Error(err)
		} else {
			fooFoo := getTestRow1()
			if !cmp.Equal(fooFoo, bar[0], cmp.AllowUnexported(big.Int{})) {
				t.Errorf("failed test: %v", cmp.Diff(fooFoo, bar, cmp.AllowUnexported(big.Int{})))
			}
		}
	}
}

func TestIntVar(t *testing.T) {
	sqlQuery := "select 123"
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var foo int
		if _, err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if foo != 123 {
				t.Errorf("foo should be 123: '%v'", foo)
			}
		}

	}
}

func TestStringVar(t *testing.T) {
	sqlQuery := "select 'moshe'"
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var foo string
		if _, err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if foo != "moshe" {
				t.Errorf("foo should be moshe: '%v'", foo)
			}
		}

	}
}

func TestPointerIntVar(t *testing.T) {
	sqlQuery := "select 123"
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var foo *int
		if _, err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if *foo != 123 {
				t.Errorf("foo should be 123: '%v'", foo)
			}
		}

	}
}

func TestPointerStringVar(t *testing.T) {
	sqlQuery := "select 'foofoo'"
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var foo *string
		if _, err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if *foo != "foofoo" {
				t.Errorf("foo should be 123: '%v'", foo)
			}
		}

	}
}

func TestNumericToFloatPointer(t *testing.T) {
	sqlQuery := `select 10.5::decimal(10,2) as mynum`
	var theNum *float32
	conn, err := GetDbConnection()
	if err != nil {
		t.Errorf("could not connect to database: %v", err)
		return
	}
	if _, err := MyQuery(context.Background(), conn, &theNum, sqlQuery); err != nil {
		t.Error(err)
	} else {
		if *theNum != 10.5 {
			t.Error("returned number is not equal to 10.5")
		}
	}
}

func TestNumericToFloat(t *testing.T) {
	sqlQuery := `select 10.5::decimal(10,2) as mynum`
	var theNum float64
	conn, err := GetDbConnection()
	if err != nil {
		t.Errorf("could not connect to database: %v", err)
		return
	}
	if _, err := MyQuery(context.Background(), conn, &theNum, sqlQuery); err != nil {
		t.Error(err)
	} else {
		if theNum != 10.5 {
			t.Error("returned number is not equal to 10.5")
		}
	}
}

func TestSomethingElse(t *testing.T) {
	sqlQuery := `
select 'hello' as sale_text, 50 as sale_product_price, 
'{wd_in_window,wd_stick_on}'::text[] buy_products_labels, '{"משה","חיים"}'::text[] as buy_products_titles, 
'front_car_wipers' as sale_product_label, 'car wipers' as sale_product_title, '{wd}'::text[] as show_in_category_labels`
	var sales []*PurchaseProductSale
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		if _, err := MyQuery(context.Background(), conn, &sales, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if len(sales) != 1 {
				t.Errorf("sales length differs then one -> %v", len(sales))
			}
			if sales[0].SaleProductPrice != 50 {
				t.Errorf("sale price is not 50 => '%v'", sales[0].SaleProductPrice)
			}
		}
	}

}

func TestComplexStruct(t *testing.T) {
	sqlQuery := `select 'Kfir Ozer' as name,
       '2020-12-25 20:09:00.454253'::timestamptz joined_at, 'dodo' as bio,
       ('[{"title" : "test", "desc" : "bar", "content" : "foo", "added_by" : ' ||
       '"Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : 4, "created_at" : "2020-10-06T12:31:45.158479+00:00"}]')::json as articles,
       ('[{"name" : "Tiesto", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-25T01:11:57.320916+00:00"}, {"name" : "Tiesto", "based_on" : null, "added_by" : ' ||
       '"Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-25T01:11:53.783807+00:00"}, ' ||
       '{"name" : "Tiesto", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null,' ||
       ' "created_at" : "2020-12-25T01:10:48.109866+00:00"}, {"name" : "Testino", "based_on" : null, "added_by" : ' ||
       '"Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:33:59.123706+00:00"}, ' ||
       '{"name" : "Testino", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:33:49.559036+00:00"}, {"name" : "Mojito", "based_on" : ["Vodka"], ' ||
       '"added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:32:57.435833+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : ["Vodka"], "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:31:54.929065+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : ["Vodka"], "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:30:30.811609+00:00"}, {"name" : "Mojito", "based_on" : ["Vodka"], ' ||
       '"added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : ' ||
       '"2020-12-24T23:29:24.245228+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : ' ||
       '"Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:28:55.360083+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : ["Vodka"], "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:28:24.009733+00:00"}, {"name" : "Mojito", "based_on" : ["Vodka"], "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:28:08.989663+00:00"}, {"name" : "Mojito", "based_on" : ' ||
       '["Vodka"], "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:26:43.074638+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : ' ||
       '"2020-12-24T23:24:48.769873+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:24:22.436272+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:18:18.198108+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:18:10.462222+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:17:13.45658+00:00"}, {"name" : "Mojito", "based_on" : null, ' ||
       '"added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:13:15.265245+00:00"}, {"name" : "Mojito", "based_on" : null, ' ||
       '"added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:11:43.84442+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:10:56.786207+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:10:49.674992+00:00"}, {"name" : "Mojito", "based_on" : ' ||
       'null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:10:49.663171+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null,' ||
       ' "created_at" : "2020-12-24T23:09:37.351679+00:00"}, {"name" : "Mojito", "based_on" : null, ' ||
       '"added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T23:09:22.127443+00:00"},' ||
       ' {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T23:08:42.766788+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer",' ||
       ' "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:22:43.59536+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null,' ||
       ' "created_at" : "2020-12-24T22:22:43.594712+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:22:21.06514+00:00"}, {"name" : "Mojito", ' ||
       '"based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:20:30.85907+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T22:20:08.226166+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:19:36.407507+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T22:19:07.280318+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:17:49.776704+00:00"}, {"name" : ' ||
       '"Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T22:16:50.40818+00:00"}, {"name" : "Mojito", "based_on" : null, ' ||
       '"added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:14:37.716166+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T22:13:38.909119+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:10:03.559003+00:00"}, {"name" : "Mojito", ' ||
       '"based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T22:09:53.89664+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:09:04.863426+00:00"}, {"name" : "Mojito", ' ||
       '"based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T22:06:42.575867+00:00"}, {"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:06:18.087513+00:00"}, {"name" : "Mojito", ' ||
       '"based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, "created_at" : "2020-12-24T22:04:04.861014+00:00"}, ' ||
       '{"name" : "Mojito", "based_on" : null, "added_by" : "Kfir Ozer", "profile_dir" : "DjUFK", "ratings" : null, ' ||
       '"created_at" : "2020-12-24T22:02:23.154287+00:00"}, {"name" : "Mojito", "based_on" : ["Vodka"], "added_by" : "Kfir Ozer", ' ||
       '"profile_dir" : "DjUFK", "ratings" : 8, "created_at" : "2020-09-29T11:41:06.375723+00:00"}]')::json as cocktails;`
	var profile Profile
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		if _, err := MyQuery(context.Background(), conn, &profile, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if profile.Name != "Kfir Ozer" {
				t.Errorf("profile name is not Kfir Ozer => '%v'", profile.Name)
			}
			if *profile.Bio != "dodo" {
				t.Errorf("profile name is not Kfir Ozer => '%v'", profile.Name)
			}
			if profile.JoinedAt.IsZero() {
				t.Error("joined at should not be zero")
			}
			if profile.Articles[0].Ratings.Int64 != 4 {
				t.Errorf("profile.Articles[0].Ratings.Int64 != 4 => '%v'", profile.Articles[0].Ratings.Int64)
			}
			if profile.Articles[0].Content != "foo" {
				t.Errorf("profile.Articles[0].Content != 'foo' => '%v'", profile.Articles[0].Ratings.Int64)
			}
			if len(profile.Cocktails) != 45 {
				t.Errorf("len(profile.Cocktails) != 45 => '%v'", len(profile.Cocktails))
			}
			if profile.Cocktails[43].Name != "Mojito" {
				t.Errorf("profile.Cocktails[43].Name != 'Mojito' => '%v'", profile.Cocktails[43].Name)
			}
			if len(profile.Cocktails[44].BasedOn) != 1 {
				t.Errorf("len(profile.Cocktails[44].BasedOn) != 1 => '%v'", len(profile.Cocktails[44].BasedOn))
			}
			if profile.Cocktails[44].BasedOn[0] != "Vodka" {
				t.Errorf("profile.Cocktails[44].BasedOn[0] != 'Vodka' => '%v'", profile.Cocktails[44].BasedOn[0])
			}
		}
	}
}

func TestJsonType(t *testing.T) {
	sqlQuery := `select 'foo' as name, '{Vodka}'::text[] as based_on,
       json_build_object('name','dj. ufk','is_img_verified',false,'profile_dir','moshe') as added_by,
       'moshe' as profile_dir, now() as created_at, 5 as ratings, false as is_img_verified
union all
select 'foo2' as name, '{Monin}'::text[] as based_on,
       json_build_object('name','bar','is_img_verified',true,'profile_dir','haim') as added_by,
       'moshe3' as profile_dir, now() as created_at, 5 as ratings, true as is_img_verified`
	var ret []*CocktailInfo2
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else if _, err := MyQuery(context.Background(), conn, &ret, sqlQuery); err != nil {
		t.Error(err)
	} else {
		if len(ret) != 2 {
			t.Errorf("len(ret) != 2 => '%v'", len(ret))
		}
		if ret[0].AddedBy.Name != "dj. ufk" {
			t.Errorf("ret[0].AddedBy.Name != \"dj. ufk\" => %v", ret[0].AddedBy.Name)
		}
		if ret[1].AddedBy.Name != "bar" {
			t.Errorf("ret[1].AddedBy.Name != \"bar\" => %v", ret[1].AddedBy.Name)
		}

	}
}

func TestTypeCast(t *testing.T) {
	sqlQuery := `select 1 as user_id, 'DjUFK' as profile_dir, 'Kfir Ozer' as name, '{USER}'::text[] as roles`
	var u UserInfo
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		if _, err := MyQuery(context.Background(), conn, &u, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if len(*u.Roles) != 1 {
				t.Errorf("len(*u.Roles) != 1 => '%v'", len(*u.Roles))
			} else if (*u.Roles)[0] != "USER" {
				t.Errorf("(*u.Roles)[0] != 'USER' => '%v'", (*u.Roles)[0])
			}
		}
	}
}

func TestComplexStruct2(t *testing.T) {
	sqlQuery := `select 'Kfir Ozer' as added_by, 'DjUFk' as profile_dir,
       'test' as title, 'bar' as desc, 'foo' as content,
       '2020-10-06 12:31:45.158479 +00:00'::timestamptz as created_at, 4 as ratings;`
	var articles []*Article
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		if _, err := MyQuery(context.Background(), conn, &articles, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if len(articles) != 1 {
				t.Errorf("len(articles) != 1 => '%v'", len(articles))
			}
			article := articles[0]
			if *article.AddedBy != "Kfir Ozer" {
				t.Errorf("*article.AddedBy != 'Kfir Ozer' => %v", *article.AddedBy)
			}
			if article.ProfileDir != "DjUFk" {
				t.Errorf("article.ProfileDir != 'DjUFk' => '%v'", article.ProfileDir)
			}
			if article.Title != "test" {
				t.Errorf("article.Title != 'test' => '%v'", article.Title)
			}
			if article.Ratings.Int64 != 4 {
				t.Errorf("rticle.Ratings.Int64 != 4 => '%v'", article.Ratings.Int64)
			}
			if r, err := time.Parse(time.RFC3339, "2020-10-06T12:31:45.158479Z"); err != nil {
				t.Errorf("cannot create time object: %v", err)
			} else {
				if !article.CreatedAt.Equal(r) {
					t.Errorf("timeestamp %v doesn't match %v", article.CreatedAt, r)
				}
			}

		}
	}
}
