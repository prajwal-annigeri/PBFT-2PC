package db

import (
	"2pcbyz-prajwal/pbft"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

type WALLog struct {
	Operation   string
	Subject     string
	Transaction *pbft.Transaction
}

type WALEntry struct {
	Logs []WALLog
}

func (d *Database) AppendToWALEntry(clientSeq int64, newLogs []WALLog) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(walBucket)

		var currentEntry WALEntry
		data := bucket.Get([]byte(strconv.FormatInt(clientSeq, 10)))
		if data != nil {
			err := json.Unmarshal(data, &currentEntry)
			if err != nil {
				return fmt.Errorf("failed to deserialize existing WALEntry: %v", err)
			}
		}

		currentEntry.Logs = append(currentEntry.Logs, newLogs...)

		updatedData, err := json.Marshal(currentEntry)
		if err != nil {
			return fmt.Errorf("failed to serialize updated WALEntry: %v", err)
		}

		return bucket.Put([]byte(strconv.FormatInt(clientSeq, 10)), updatedData)
	})
}

func (d *Database) ReadWALEntry(clientSeq int64) (WALEntry, error) {
	var entry WALEntry

	err := d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(walBucket)

		data := bucket.Get([]byte(strconv.FormatInt(clientSeq, 10)))
		if data == nil {
			return fmt.Errorf("no wal for %d", clientSeq)
		}

		return json.Unmarshal(data, &entry)
	})
	if err != nil {
		return WALEntry{}, err
	}

	return entry, nil
}

func (d *Database) PrintAllWAL() {
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(walBucket)

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry WALEntry
			err := json.Unmarshal(v, &entry)
			if err != nil {
				log.Printf("Error deserializing DB Unit: %v\n", err)
				continue
			}

			fmt.Printf("%s: %v", string(k), entry)
		}
		return nil
	})
}

func (d *Database) GetWalCopy() map[string]string {
	wals := make(map[string]string)
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(walBucket)

		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry WALEntry
			err := json.Unmarshal(v, &entry)
			if err != nil {
				log.Printf("Error deserializing DB Unit: %v\n", err)
				continue
			}
			wals[string(k)] = string(v)
		}
		return nil
	})
	return wals
}

func (d *Database) EraseWal(clientSeq int64) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(walBucket)
		return bucket.Delete([]byte(strconv.FormatInt(clientSeq, 10)))
	})
}
