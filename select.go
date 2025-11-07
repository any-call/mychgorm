package mychgorm

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"reflect"
	"strings"
)

func ScanList(c clickhouse.Conn, dst any, queryStr string, args ...any) error {
	if c == nil {
		return fmt.Errorf("null click house conn")
	}

	// 检查 dst 类型：必须是 *[]T
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("dst must be a pointer to a slice, got %T", dst)
	}

	sliceVal := rv.Elem()
	elemType := sliceVal.Type().Elem() // 取 T 的类型

	rows, err := c.Query(context.Background(), queryStr, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols := rows.Columns()
	for rows.Next() {
		// 新建一个元素指针 *T
		elemPtr := reflect.New(elemType)
		elemVal := elemPtr.Elem()
		if elemType.Kind() == reflect.Struct {
			// 生成每列对应字段的地址
			addrs := make([]any, len(cols))
			for i, col := range cols {
				f := findStructFieldByCol(elemVal, col)
				if f.IsValid() && f.CanAddr() {
					addrs[i] = f.Addr().Interface()
				} else {
					// 占位，避免扫描报错
					var discard any
					addrs[i] = &discard
				}
			}

			if err := rows.Scan(addrs...); err != nil {
				return fmt.Errorf("rows.Scan failed: %w", err)
			}

			sliceVal = reflect.Append(sliceVal, elemVal)
		} else {
			// 非结构体（基础类型），期望只有一列
			if cols != nil && len(cols) != 1 {
				return fmt.Errorf("expected single column for slice of %s, got %d columns", elemType.Kind().String(), len(cols))
			}
			fieldPtr := reflect.New(elemType)
			if err := rows.Scan(fieldPtr.Interface()); err != nil {
				return fmt.Errorf("rows.Scan single-col failed: %w", err)
			}

			sliceVal = reflect.Append(sliceVal, fieldPtr.Elem())
		}

	}

	rv.Elem().Set(sliceVal)
	return nil
}

// findStructFieldByCol 根据列名匹配结构体字段
func findStructFieldByCol(elem reflect.Value, col string) reflect.Value {
	colLower := strings.ToLower(col)
	t := elem.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		if strings.ToLower(f.Name) == colLower {
			return elem.Field(i)
		}
		if tag := f.Tag.Get("ch"); strings.ToLower(tag) == colLower {
			return elem.Field(i)
		}
		if tag := f.Tag.Get("json"); strings.ToLower(strings.Split(tag, ",")[0]) == colLower {
			return elem.Field(i)
		}
	}
	return reflect.Value{} // 无匹配
}
