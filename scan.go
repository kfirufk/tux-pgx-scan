package tux_pgx_scan

import (
	"context"
	"github.com/iancoleman/strcase"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"reflect"
)

func doSliceProperty(originalColumnName string, currentSliceElement reflect.Value, val interface{}) error {
	return nil
}

func getStructProperty(columnName string, structElement reflect.Value) (reflect.Value, error) {
	columnNameParsed := strcase.ToCamel(columnName)
	structColumn := structElement.FieldByName(columnNameParsed)
	if !structColumn.IsValid() {
		return reflect.Value{}, errors.Errorf("row returned column name %v which was not found in the destination address", columnName)
	} else {
		return structColumn, nil
	}
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
	switch val.(type) {
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
							structColumn.Index(idx).Set(reflect.ValueOf(arr[idx]))
						default:
							return errors.New("unknown type when appending to slice")
						}
					}
				}
			default:
				structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
			}
		}

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
		structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
	}
	return nil
}

func MyQuery(ctx context.Context, conn *pgxpool.Pool, dstAddr interface{}, sql string, args ...interface{}) error {
	barAddrVal := reflect.ValueOf(dstAddr)
	if rows, err := conn.Query(ctx, sql, args...); err != nil {
		return errors.Errorf("could not select from db: %v", err)
	} else {
		currentElement := barAddrVal.Elem()
		rowNumber := 0
		for rows.Next() {
			rowNumber++
			if barAddrVal.Elem().Kind() == reflect.Slice {
				sliceElm := barAddrVal.Elem()
				for sliceElm.Len() < rowNumber {
					newItem := reflect.New(sliceElm.Type().Elem())
					sliceElm.Set(reflect.Append(sliceElm, newItem.Elem()))
				}
				currentElement = barAddrVal.Elem().Index(rowNumber - 1)
				if !currentElement.IsValid() {
					return errors.New("slice item source is not valid")
				}
			}
			if values, err := rows.Values(); err != nil {
				return errors.Errorf("could not fetch values from db: %v", err)
			} else {
				fields := rows.FieldDescriptions()
				for idx, column := range fields {
					val := values[idx]
					switch currentElement.Kind() {
					case reflect.Struct:
						if err := doStructColumnProperty(string(column.Name), currentElement, val); err != nil {
							return err
						}
					case reflect.Slice:
						if err := doSliceProperty(string(column.Name), currentElement, val); err != nil {
							return err
						}
					default:
						f := currentElement
						for f.Type().Kind() == reflect.Ptr && f.Elem().Kind() == reflect.Ptr {
							if f.Elem().IsZero() {
								f.Elem().Set(reflect.New(f.Type().Elem().Elem()))
							}
							f = f.Elem()
						}
						if f.Kind() == reflect.Ptr {
							if f.IsZero() {
								f.Set(reflect.New(f.Type().Elem()))
							}
							f.Elem().Set(reflect.ValueOf(val).Convert(f.Type().Elem()))
						} else {
							f.Set(reflect.ValueOf(val).Convert(f.Type()))
						}
					}

				}
			}
		}
		return nil
	}
}
