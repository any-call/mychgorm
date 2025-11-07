package mychgorm

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"reflect"
	"strings"
)

func CreateTable(db clickhouse.Conn, model CHModel, forceRecreate bool) error {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var columns []string
	var orderBy string

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		// ---- 解析 ch tag ----
		chTag := f.Tag.Get("ch")
		var col string
		var defaultExpr string
		var isOrder bool

		if chTag != "" {
			parts := strings.Split(chTag, ";")
			col = strings.TrimSpace(parts[0])
			for _, part := range parts[1:] {
				part = strings.TrimSpace(part)
				if part == "order" {
					isOrder = true
				} else if strings.HasPrefix(part, "default:") {
					defaultExpr = strings.TrimPrefix(part, "default:")
				}
			}
		}

		if col == "" {
			col = strings.ToLower(f.Name)
		}

		// ---- 映射 Go 类型到 ClickHouse 类型 ----
		sqlType := mapGoTypeToCHType(f)

		columnDef := fmt.Sprintf("%s %s", col, sqlType)
		if defaultExpr != "" {
			columnDef += fmt.Sprintf(" DEFAULT %s", defaultExpr)
		}

		columns = append(columns, columnDef)

		// ---- 判断排序键 ----
		if orderBy == "" && isOrder {
			orderBy = col
		}
	}

	// ---- 如果没有 order 字段，默认使用第一个字段 ----
	if orderBy == "" {
		if len(columns) > 0 {
			orderBy = strings.Fields(columns[0])[0]
		} else {
			orderBy = "tuple()" // 无字段 fallback
		}
	}

	// ---- 生成 SQL ----
	var createSQL string
	tableName := model.TableName()
	if forceRecreate {
		// 先 DROP
		err := db.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		if err != nil {
			return fmt.Errorf("drop table failed: %w", err)
		}

		createSQL = fmt.Sprintf(`
CREATE TABLE %s (
	%s
) ENGINE = MergeTree()
ORDER BY (%s);`, tableName, strings.Join(columns, ",\n\t"), orderBy)
	} else {
		createSQL = fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	%s
) ENGINE = MergeTree()
ORDER BY (%s);`, model.TableName(), strings.Join(columns, ",\n\t"), orderBy)
	}

	return db.Exec(context.Background(), createSQL)
}

func mapGoTypeToCHType(field reflect.StructField) string {
	switch field.Type.Kind() {
	case reflect.String:
		return "String"
	case reflect.Int8:
		return "Int8"
	case reflect.Int16:
		return "Int16"
	case reflect.Int32:
		return "Int32"
	case reflect.Int64, reflect.Int:
		return "Int64"
	case reflect.Uint, reflect.Uint64:
		return "UInt64"
	case reflect.Uint8:
		return "UInt8"
	case reflect.Uint16:
		return "UInt16"
	case reflect.Uint32:
		return "UInt32"
	case reflect.Float32:
		return "Float32"
	case reflect.Float64:
		return "Float64"
	case reflect.Bool:
		return "UInt8"
	case reflect.Struct:
		if field.Type.PkgPath() == "time" && field.Type.Name() == "Time" {
			return "DateTime"
		}
	default:
		return "String"
	}

	return "String"
}
