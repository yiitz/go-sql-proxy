package proxy

import (
	"database/sql/driver"
	"fmt"
	"log"
)

// NewTraceProxy generates a proxy that logs queries.
func NewTraceProxy(d driver.Driver, logger *log.Logger) *Proxy {
	return &Proxy{
		Driver: d,
		Hooks: &Hooks{
			Open: func(conn *Conn) error {
				logger.Output(7, "Open")
				return nil
			},
			Exec: func(stmt *Stmt, args []driver.Value, result driver.Result) error {
				logger.Output(7, fmt.Sprintf("Exec: %s; args = %v", stmt.QueryString, args))
				return nil
			},
			Query: func(stmt *Stmt, args []driver.Value, rows driver.Rows) error {
				logger.Output(9, fmt.Sprintf("Query: %s; args = %v", stmt.QueryString, args))
				return nil
			},
			Begin: func(conn *Conn) error {
				logger.Output(7, "Begin")
				return nil
			},
			Commit: func(tx *Tx) error {
				logger.Output(7, "Commit")
				return nil
			},
			Rollback: func(tx *Tx) error {
				logger.Output(9, "Rollback")
				return nil
			},
		},
	}
}
