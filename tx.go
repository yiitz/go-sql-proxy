package proxy

import "database/sql/driver"

type Tx struct {
	Tx    driver.Tx
	Proxy *Proxy
}

func (tx *Tx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}

	return tx.Proxy.Hooks.CommitFunc(tx)
}

func (tx *Tx) Rollback() error {
	if err := tx.Tx.Rollback(); err != nil {
		return err
	}

	return tx.Proxy.Hooks.RollbackFunc(tx)
}
