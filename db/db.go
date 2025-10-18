package db

import (
	bolt "go.etcd.io/bbolt"
)

var (
	balancesBucket     = []byte("balances")
	transactionsBucket = []byte("transactions")
	walBucket          = []byte("wal")
	locksBucket        = []byte("locks")
)

type Database struct {
	db *bolt.DB
}

func InitDatabase(dbPath string) (db *Database, closeFunc func() error, err error) {
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{
		db: boltDB,
	}

	if err := db.createBuckets(); err != nil {
		boltDB.Close()
		return nil, nil, err
	}

	return db, boltDB.Close, nil
}

func (d *Database) createBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(balancesBucket); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(transactionsBucket); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(walBucket); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(locksBucket); err != nil {
			return err
		}
		return nil
	})
}