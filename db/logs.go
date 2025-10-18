package db

import (
	"2pcbyz-prajwal/pbft"
	"encoding/binary"
	"encoding/json"
	"log"

	bolt "go.etcd.io/bbolt"
)

type DBUnit struct {
	Seq         int64
	Transaction *pbft.Transaction
	Status      string
}

func serializeDBUnit(seq int64, transaction *pbft.Transaction, status string) ([]byte, error) {
	bytes, err := json.Marshal(DBUnit{
		Seq:         seq,
		Transaction: transaction,
		Status:      status,
	})
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func deserializeDBUnit(bytes []byte) (*DBUnit, error) {
	dbUnit := &DBUnit{}
	err := json.Unmarshal(bytes, &dbUnit)
	return dbUnit, err
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func Btoi(v []byte) uint64 {
	return binary.BigEndian.Uint64(v)
}

func (d *Database) SaveTransactionToDB(seq int64, transaction *pbft.Transaction, status string) ([]byte, error) {
	bytes, err := serializeDBUnit(seq, transaction, status)
	if err != nil {
		return nil, err
	}

	err = d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(transactionsBucket)
		id, _ := b.NextSequence()
		return b.Put(itob(id), bytes)
	})
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (d *Database) SaveTransactionToDBBytes(transactionBytes []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(transactionsBucket)
		id, _ := b.NextSequence()
		return b.Put(itob(id), transactionBytes)
	})
}

func (d *Database) GetAllTransactionBlocks() []*DBUnit {
	var blockList []*DBUnit
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(transactionsBucket)

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			dbUnit, err := deserializeDBUnit(v)
			if err != nil {
				log.Printf("Error deserializing DB Unit: %v\n", err)
				continue
			}

			blockList = append(blockList, dbUnit)
		}
		return nil
	})
	return blockList
}
