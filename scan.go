package tux_pgx_scan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/iancoleman/strcase"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type dbconn interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

func getStructPropertyName(columnName string) string {
	if len(columnName) == 2 {
		return strings.ToUpper(columnName)
	} else {
		if strings.ToLower(columnName[len(columnName)-2:]) == "id" {
			return columnName[:len(columnName)-2] + strings.ToUpper(columnName[len(columnName)-2:])
		}
		return strcase.ToCamel(columnName)
	}
}

// https://stackoverflow.com/questions/54119616/ignore-case-in-golang-reflection-fieldbyname
func getStructProperty(name string, v reflect.Value) (reflect.Value, error) {
	name = strings.ToLower(strings.ReplaceAll(name, "_", ""))
	structColumn := v.FieldByNameFunc(func(n string) bool { return strings.ToLower(n) == name })
	if !structColumn.IsValid() {
		return reflect.Value{}, errors.Errorf("rowI returned column name %v which was not found in the destination address", name)
	} else {
		return structColumn, nil
	}
}

func placeData(structColumn reflect.Value, structColumnType reflect.Type, val interface{}) error {
	switch val.(type) {
	case string:
		switch structColumn.Interface().(type) {
		case time.Time:
			myVal := val.(string)
			if theTime, err := dateparse.ParseAny(myVal); err != nil {
				return err
			} else {
				structColumn.Set(reflect.ValueOf(theTime))

			}
		default:
			if structColumnType.Kind() == reflect.Slice ||
				structColumnType.Kind() == reflect.Struct {
				var result interface{}
				if err := json.Unmarshal([]byte(val.(string)), &result); err == nil {
					return placeData(structColumn, structColumnType, result)
				}
			}
			structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
		}

	case float64:
		myVal := val.(float64)
		switch structColumn.Interface().(type) {
		case sql.NullInt64:
			s := sql.NullInt64{
				Int64: int64(myVal),
				Valid: true,
			}
			structColumn.Set(reflect.ValueOf(s))
		default:
			structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
		}
	case int32:
		myVal := val.(int32)
		switch structColumn.Interface().(type) {
		case sql.NullInt64:
			s := sql.NullInt64{
				Int64: int64(myVal),
				Valid: true,
			}
			structColumn.Set(reflect.ValueOf(s))
		default:
			structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
		}
	case map[string]interface{}:
		if err := doSingleRowProperty(false, structColumn, val); err != nil {
			return err
		}
	case pgtype.TextArray:
		myVal := val.(pgtype.TextArray)
		var arr []string
		if err := myVal.AssignTo(&arr); err != nil {
			return errors.Errorf("could not assign pgtype.TextArray: %v", err)
		} else {
			switch structColumn.Kind() {
			case reflect.Slice:
				if !structColumn.CanAddr() {
					return errors.New("cannot get address of slice element for pgtype.TextArray")
				} else {
					structColumn.Set(reflect.MakeSlice(structColumn.Type(), len(arr), len(arr)))
					for idx, _ := range arr {
						switch structColumn.Type().Elem().Kind() {
						case reflect.Ptr:
							structColumn.Index(idx).Set(reflect.ValueOf(&arr[idx]))
						case reflect.String:
							structColumn.Index(idx).Set(reflect.ValueOf(arr[idx]).Convert(structColumn.Index(idx).Type()))
						default:
							return errors.New("unknown type when appending to slice")
						}
					}
				}
			default:
				structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
			}
		}
	case bool:
		structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
	case pgtype.Int4Array:
		myVal := val.(pgtype.Int4Array)
		var arr []int
		if err := myVal.AssignTo(&arr); err != nil {
			return errors.Errorf("could not assign pgtype.Int4Array: %v", err)
		} else {
			switch structColumn.Kind() {
			case reflect.Slice:
				if !structColumn.CanAddr() {
					return errors.New("cannot get address of slice element for pgtype.Int4Array")
				} else if err := myVal.AssignTo(structColumn.Addr().Interface()); err != nil {
					return errors.Errorf("could not scan to slice: %v", err)
				}
			default:
				structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
			}
		}
	case sql.NullInt64:
		myVal := val.(sql.NullInt64)
		val := myVal.Int64
		structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
	case pgtype.Numeric:
		myVal := val.(pgtype.Numeric)
		switch structColumn.Kind() {
		case reflect.Float64:
			var s float64
			if err := myVal.AssignTo(&s); err != nil {
				return errors.Errorf("could not set pgtype.Numeric: %v", err)
			}
			structColumn.Set(reflect.ValueOf(s))
		case reflect.Struct: // if both sides are pgtype.Numbric, so just set it, convert may not be neccesarry
			structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
		default:
			return errors.Errorf("uknown format %v", structColumn.Kind())
		}
	case pgtype.Float8Array:
		myVal := val.(pgtype.Float8Array)
		var arr []float64
		if err := myVal.AssignTo(&arr); err != nil {
			return errors.Errorf("could not assign pgtype.Float8Array: %v", err)
		} else {
			switch structColumn.Kind() {
			case reflect.Slice:
				if !structColumn.CanAddr() {
					return errors.New("cannot get address of slice element for pgtype.Int4Array")
				} else if err := myVal.AssignTo(structColumn.Addr().Interface()); err != nil {
					return errors.Errorf("could not scan to slice: %v", err)
				}
			default:
				structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
			}
		}

	default:
		if reflect.TypeOf(val).Kind() == reflect.Slice && structColumn.Kind() == reflect.Slice {
			if err := doSliceProperty(structColumn, val); err != nil {
				return err
			}
		} else {
			structColumn.Set(reflect.ValueOf(val).Convert(structColumn.Type()))
		}
	}
	return nil
}

func doStructColumnProperty(originalColumnName string, currentElement reflect.Value, val interface{}) error {
	structColumn, err := getStructProperty(originalColumnName, currentElement)
	if err != nil {
		return err
	}
	structColumnType := structColumn.Type()
	if structColumn.Kind() == reflect.Ptr { // check if pointer
		if structColumn.IsZero() { // check if pointer is not allocated
			structColumn.Set(reflect.New(structColumnType.Elem())) // allocate
		}
		structColumn = structColumn.Elem()
		structColumnType = structColumnType.Elem()

	}
	/**
	for example to convert from reflect.Int32 to reflect.Int
	TODO: i need to check her for errors and to provide proper error message with column
	      name and row number maybe, for example when getting a float to float64 instead of pg.Numeric
	*/
	if err := placeData(structColumn, structColumnType, val); err != nil {
		return err
	}
	return nil
}

func doSingleRowProperty(isSlice bool, element reflect.Value, val interface{}) error {
	var currentElement reflect.Value
	if isSlice {
		currentElement = reflect.New(element.Type().Elem())
	} else {
		currentElement = element
	}
	rowVal := reflect.ValueOf(val)
	dataElement := currentElement
	pointerDataElement := currentElement
	for dataElement.Type().Kind() == reflect.Ptr {
		if dataElement.IsZero() {
			dataElement.Set(reflect.New(dataElement.Type().Elem()))
		}
		pointerDataElement = dataElement
		dataElement = dataElement.Elem()
	}

	switch rowVal.Kind() {
	case reflect.Map:
		for _, columnNameVal := range rowVal.MapKeys() {
			columnName := columnNameVal.Interface().(string)
			myVal := rowVal.MapIndex(columnNameVal).Interface()
			if myVal == nil {
				continue
			}
			switch dataElement.Kind() {
			case reflect.Struct:
				if err := doStructColumnProperty(columnName, dataElement, myVal); err != nil {
					return err
				}
			default:
				fieldStructPropertyName := getStructPropertyName(columnName)
				fieldVal := dataElement.FieldByName(fieldStructPropertyName)
				if !fieldVal.IsValid() {
					return errors.New("internal error: couldn't get field from a struct")
				}
				fieldVal.Set(reflect.ValueOf(myVal).Convert(fieldVal.Type()))
			}

		}
	default:
		dataElement.Set(rowVal.Convert(dataElement.Type()))
	}
	if isSlice {
		if element.Type().Elem().Kind() == reflect.Ptr {
			element.Set(reflect.Append(element, pointerDataElement))
		} else {
			element.Set(reflect.Append(element, dataElement))
		}
	} else {
		if element.Type().Kind() == reflect.Ptr {
			element.Set(pointerDataElement)
		} else {
			element.Set(dataElement)
		}

	}
	return nil
}

func doSliceProperty(sliceVal reflect.Value, val interface{}) error {
	if reflect.TypeOf(val).Kind() != reflect.Slice {
		return errors.New("doSliceProperty got an element which is not a slice")
	}
	rows := val.([]interface{})
	for _, row := range rows {
		if err := doSingleRowProperty(true, sliceVal, row); err != nil {
			return err
		}
	}
	return nil
}

func BytesToString(b []byte) string {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{bh.Data, bh.Len}
	return *(*string)(unsafe.Pointer(&sh))
}

type MyQueryScanRet struct {
	Rows pgx.Rows
}

func (m *MyQueryScanRet) Scan(dest ...interface{}) error {
	return m.Rows.Scan(dest...)
}

func MyQueryScan(ctx context.Context, conn dbconn, sql string, args ...interface{}) (*MyQueryScanRet, bool, error) {
	if rows, err := conn.Query(ctx, sql, args...); err != nil {
		return nil, true, errors.Errorf("could not select from db: %v", err)
	} else {
		defer rows.Close()
		rowNumber := 0
		for rows.Next() {
			rowNumber++
			if rowNumber > 1 {
				return nil, true, errors.New("current version does not support result with more then one line")
			}
			ret := MyQueryScanRet{
				Rows: rows,
			}
			return &ret, false, nil
		}
		return nil, true, nil
	}
}

func MyQuery(ctx context.Context, conn dbconn, dstAddr interface{}, sql string, args ...interface{}) (bool, error) {
	barAddrVal := reflect.ValueOf(dstAddr)
	if rows, err := conn.Query(ctx, sql, args...); err != nil {
		return true, errors.Errorf("could not select from db: %v", err)
	} else {
		defer rows.Close()
		currentElement := barAddrVal.Elem()
		rowNumber := 0
		for rows.Next() {
			rowNumber++
			//		log.Printf("working on row %v",rowNumber)
			if barAddrVal.Elem().Kind() == reflect.Slice {
				sliceElm := barAddrVal.Elem()
				for sliceElm.Len() < rowNumber {
					newItem := reflect.New(sliceElm.Type().Elem())
					sliceElm.Set(reflect.Append(sliceElm, newItem.Elem()))
				}
				currentElement = barAddrVal.Elem().Index(rowNumber - 1)
				if !currentElement.IsValid() {
					return true, errors.New("slice item source is not valid")
				}
			}
			if values, err := rows.Values(); err != nil {
				return true, errors.Errorf("could not fetch values from db: %v", err)
			} else {
				fields := rows.FieldDescriptions()
				for idx, column := range fields {
					val := values[idx]
					//					log.Printf("working on column %s value %v",column.Name,val)
					if val == nil {
						continue
					}
					switch currentElement.Kind() {
					case reflect.Struct:
						if err := doStructColumnProperty(string(column.Name), currentElement, val); err != nil {
							return true, err
						}
					default:
						myVal := reflect.ValueOf(val) // if reflect.Kind = reflect.Interface, to change it
						if currentElement.Kind() == reflect.Ptr {
							if currentElement.Type().Elem().Kind() == reflect.Struct {
								if currentElement.IsZero() {
									currentElement.Set(reflect.New(currentElement.Type().Elem()))
								}
								if err := doStructColumnProperty(string(column.Name), currentElement.Elem(), val); err != nil {
									return true, err
								}
							} else {
								valIntPtr := reflect.New(currentElement.Type().Elem())
								if myVal.Type().String() == "pgtype.Numeric" {
									var num = myVal.Interface().(pgtype.Numeric)
									var res float64
									if err := num.AssignTo(&res); err != nil {
										return false, err
									}
									resF := reflect.ValueOf(res)
									valIntPtr.Elem().Set(resF.Convert(valIntPtr.Elem().Type()))
								} else {
									valIntPtr.Elem().Set(myVal.Convert(currentElement.Type().Elem()))
								}
								currentElement.Set(valIntPtr)
							}
						} else {
							if currentElement.Kind() == reflect.String && myVal.Kind() == reflect.Array { //UUID
								b := myVal.Interface()
								a := fmt.Sprintf("%x", b)
								currentElement.Set(reflect.ValueOf(a))
							} else {
								if myVal.Type().String() == "pgtype.Numeric" {
									var num = myVal.Interface().(pgtype.Numeric)
									var res float64
									if err := num.AssignTo(&res); err != nil {
										return false, err
									}
									resF := reflect.ValueOf(res)
									currentElement.Set(resF.Convert(currentElement.Type()))
								} else {
									currentElement.Set(myVal.Convert(currentElement.Type()))
								}
							}
						}
					}

				}
			}
		}
		return rowNumber == 0, rows.Err()
	}
}
