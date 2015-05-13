// a proxy package is a proxy driver for dabase/sql.

package proxy

import "database/sql/driver"

type Proxy struct {
	Driver driver.Driver
	Hooks  HooksInterface
}

type HooksInterface interface {
	OpenFunc(conn *Conn) error
	ExecFunc(stmt *Stmt, args []driver.Value, result driver.Result) error
	QueryFunc(stmt *Stmt, args []driver.Value, rows driver.Rows) error
	BeginFunc(conn *Conn) error
	CommitFunc(tx *Tx) error
	RollbackFunc(tx *Tx) error
}

type Hooks struct {
	Open     func(conn *Conn) error
	Exec     func(stmt *Stmt, args []driver.Value, result driver.Result) error
	Query    func(stmt *Stmt, args []driver.Value, rows driver.Rows) error
	Begin    func(conn *Conn) error
	Commit   func(tx *Tx) error
	Rollback func(tx *Tx) error
}

func (h *Hooks) OpenFunc(conn *Conn) error {
	if h.Open == nil {
		return nil
	}
	return h.Open(conn)
}

func (h *Hooks) ExecFunc(stmt *Stmt, args []driver.Value, result driver.Result) error {
	if h.Exec == nil {
		return nil
	}
	return h.Exec(stmt, args, result)
}

func (h *Hooks) QueryFunc(stmt *Stmt, args []driver.Value, rows driver.Rows) error {
	if h.Query == nil {
		return nil
	}
	return h.Query(stmt, args, rows)
}

func (h *Hooks) BeginFunc(conn *Conn) error {
	if h.Begin == nil {
		return nil
	}
	return h.Begin(conn)
}

func (h *Hooks) CommitFunc(tx *Tx) error {
	if h.Commit == nil {
		return nil
	}
	return h.Commit(tx)
}

func (h *Hooks) RollbackFunc(tx *Tx) error {
	if h.Rollback == nil {
		return nil
	}
	return h.Rollback(tx)
}

func NewProxy(driver driver.Driver, hooks HooksInterface) *Proxy {
	if hooks == nil {
		hooks = &Hooks{}
	}
	return &Proxy{
		Driver: driver,
		Hooks:  hooks,
	}
}

func (p *Proxy) Open(name string) (driver.Conn, error) {
	conn, err := p.Driver.Open(name)
	if err != nil {
		return nil, err
	}

	proxyConn := &Conn{
		Conn:  conn,
		Proxy: p,
	}
	if err := p.Hooks.OpenFunc(proxyConn); err != nil {
		conn.Close()
		return nil, err
	}

	return proxyConn, nil
}
