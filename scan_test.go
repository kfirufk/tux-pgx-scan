package tux_pgx_scan

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"math/big"
	"testing"
	"time"
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

type CocktailInfo struct {
	Name       string         `json:"name"`
	BasedOn    pq.StringArray `json:"based_on"`
	AddedBy    *string        `json:"added_by"`
	ProfileDir *string        `json:"profile_dir"`
	Ratings    *sql.NullInt64 `json:"ratings"`
	CreatedAt  time.Time      `json:"created_at"`
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

func TestIntVar(t *testing.T) {
	sqlQuery := "select 123"
	if conn, err := GetDbConnection(); err != nil {
		t.Errorf("could not connect to database: %v", err)
	} else {
		var foo int
		if err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
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
		if err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
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
		if err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
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
		if err := MyQuery(context.Background(), conn, &foo, sqlQuery); err != nil {
			t.Error(err)
		} else {
			if *foo != "foofoo" {
				t.Errorf("foo should be 123: '%v'", foo)
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
		if err := MyQuery(context.Background(), conn, &profile, sqlQuery); err != nil {
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
