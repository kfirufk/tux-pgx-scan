package main

import (
	"context"
	"github.com/iancoleman/strcase"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"reflect"
)

func MyQuery(ctx context.Context, conn *pgxpool.Pool, dstAddr interface{}, sql string, args ...interface{}) error {
	barAddrVal := reflect.ValueOf(dstAddr)
	if rows, err := conn.Query(ctx, sql, args...); err != nil {
		return errors.Errorf("could not select from db: %v", err)
	} else {
		for rows.Next() {
			if values, err := rows.Values(); err != nil {
				return errors.Errorf("could not fetch values from db: %v", err)
			} else {
				fields := rows.FieldDescriptions()
				for idx, column := range fields {
					val := values[idx]
					columnName := strcase.ToCamel(string(column.Name))
					structColumn := barAddrVal.Elem().FieldByName(columnName)
					if !structColumn.IsValid() {
						return errors.Errorf("row returned column name %v which was not found in the destination address", string(column.Name))
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
					default:
						structColumn.Set(reflect.ValueOf(val).Convert(structColumnType))
					}
				}
			}
		}
		return nil
	}
}
