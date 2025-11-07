package mychgorm

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"strings"
	"time"
)

type (
	CHModel interface {
		TableName() string
	}

	PageReq struct {
		Limit int `form:"limit" json:"limit" validate:"min(1,入参limit不能为0)"`
		Page  int `form:"page" json:"page" validate:"min(1,入参page不能为0)"`
	}
	PageResp[T any] struct {
		Total int64 `json:"total"`
		Page  int   `json:"page"`
		Limit int   `json:"limit"`
		List  []T   `json:"list"`
	}

	InsertSQL interface {
		CHModel
		InsertSQLFields() []string
		InsertSQLValues() []any
	}

	BatchWriterConfig struct {
		MaxInsertCacheNum int
		MaxInsertDuration time.Duration
	}
)

//初始化模块
//func init() {
//	conn, err := clickhouse.Open(&clickhouse.Options{
//		Addr: []string{envs.ClickHouseAddr},
//		Auth: clickhouse.Auth{Database: envs.ClickHouseDatabase, Username: envs.ClickHouseUser, Password: envs.ClickHousePasswd},
//		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
//			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
//		},
//		Debug:                envs.ClickHouseDebug,
//		Debugf:               func(format string, v ...any) { fmt.Printf(format, v) },
//		Settings:             clickhouse.Settings{"max_execution_time": 60},
//		Compression:          &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
//		DialTimeout:          time.Second * 30,
//		MaxOpenConns:         10,
//		MaxIdleConns:         5,
//		ConnMaxLifetime:      time.Duration(10) * time.Minute,
//		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
//		BlockBufferSize:      10,
//		MaxCompressionBuffer: 10240,
//	})
//	if err != nil {
//		panic(err)
//	}
//	if err = conn.Ping(context.Background()); err != nil {
//		panic(err)
//	}
//	chConn = conn
//}

func Exec(c clickhouse.Conn, queryStr string, args ...any) error {
	if c != nil {
		return c.Exec(context.Background(), queryStr, args...)
	}

	return fmt.Errorf("null click house conn")
}

func Select(c clickhouse.Conn, dst any, queryStr string, args ...any) error {
	if c != nil {
		return c.Select(context.Background(), dst, queryStr, args...)
	}
	return fmt.Errorf("null click house conn")
}

func ScanStruct(c clickhouse.Conn, dst any, queryStr string, args ...any) error {
	if c != nil {
		return c.QueryRow(context.Background(), queryStr, args...).ScanStruct(dst)
	}
	return fmt.Errorf("null click house conn")
}

func AsyncInsert(c clickhouse.Conn, queryStr string, args ...any) error {
	if c != nil {
		return c.AsyncInsert(context.Background(), queryStr, false, args...)
	}

	return fmt.Errorf("null click house conn")
}

func Pagination[T any](db clickhouse.Conn, sql string, req PageReq, resp *PageResp[T]) (err error) {
	if db == nil {
		return fmt.Errorf("clickhouse connection is nil")
	}
	if req.Page < 1 {
		req.Page = 1
	}
	if req.Limit < 1 {
		req.Limit = 10
	}

	ctx := context.Background()
	// --------- Step 1: 统计总数 ----------
	countSQL := fmt.Sprintf("SELECT count() FROM (%s)", sql)
	var total uint64
	if err = db.QueryRow(ctx, countSQL).Scan(&total); err != nil {
		return fmt.Errorf("count query failed: %w", err)
	}

	resp.Total = int64(total)
	resp.Page = req.Page
	resp.Limit = req.Limit

	// 如果没有数据，提前返回空列表
	if total == 0 {
		resp.List = []T{}
		return nil
	}

	// --------- Step 2: 添加分页SQL ----------
	offset := (req.Page - 1) * req.Limit
	pageSQL := fmt.Sprintf("%s LIMIT %d OFFSET %d", strings.TrimSpace(sql), req.Limit, offset)

	// --------- Step 3: 用 ScanList 扫描分页数据 ----------
	if err := ScanList(db, &resp.List, pageSQL); err != nil {
		return fmt.Errorf("scan list failed: %w", err)
	}

	return nil
}

// SetTableTTL 设置 ClickHouse 表的 TTL 自动过期规则。
// conn:  clickhouse.Conn
// table: 表名，例如 "logs"
// field: 用于过期的字段，例如 "timestamp"
// days:  保留天数，例如 7
func SetTableTTL(conn clickhouse.Conn, table, field string, days int) error {
	if conn == nil {
		return fmt.Errorf("invalid ClickHouse connection")
	}
	if table == "" || field == "" || days <= 0 {
		return fmt.Errorf("invalid parameters: table=%q, field=%q, days=%d", table, field, days)
	}

	ctx := context.Background()

	// 查询字段类型：使用 system.columns 保证跨版本通用
	var dataType string
	query := `
		SELECT name, type 
		FROM system.columns 
		WHERE table = ? AND database = currentDatabase()
	`
	rows, err := conn.Query(ctx, query, table)
	if err != nil {
		return fmt.Errorf("failed to query system.columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return fmt.Errorf("failed to scan column info: %w", err)
		}
		if name == field {
			dataType = typ
			break
		}
	}

	if dataType == "" {
		return fmt.Errorf("field %q not found in table %q", field, table)
	}

	// 判断字段类型（DateTime 不转换，其他转为 toDateTime）
	fieldExpr := field
	if !strings.HasPrefix(strings.ToLower(dataType), "datetime") {
		fieldExpr = fmt.Sprintf("toDateTime(%s)", field)
	}

	// 设置 TTL
	queryTTL := fmt.Sprintf(`
		ALTER TABLE %s
		MODIFY TTL %s + INTERVAL %d DAY;
	`, table, fieldExpr, days)
	if err := conn.Exec(ctx, queryTTL); err != nil {
		return fmt.Errorf("failed to set TTL: %w", err)
	}

	// 可选：立即触发 TTL 清理，方便测试
	if err := conn.Exec(ctx, fmt.Sprintf("OPTIMIZE TABLE %s FINAL;", table)); err != nil {
		return fmt.Errorf("failed to optimize table after TTL change: %w", err)
	}

	return nil
}
