package db

import (
	"errors"
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
)

func (d *Database) ReleaseLocksDB(clients []string) error {
	log.Printf("Releasing locks %v\n", clients)
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(locksBucket)

		for _, client := range clients {
			bucket.Delete([]byte(client))
		}
		return nil
	})
}

func (d *Database) GetLocksDB(clients []string) error {
	log.Printf("Locking %v\n", clients)
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(locksBucket)

		for _, client := range clients {
			bucket.Put([]byte(client), []byte("true"))
		}
		return nil
	})
}

func (d *Database) CheckAndGetLocks(clients []string, byz bool, seq int64) error {
	err := d.CheckLocksDB(clients, seq)
	if err != nil {
		return errors.New("already locked")
	}
	if !byz {
		err = d.GetLocksDB(clients)
		if err != nil {
			return errors.New("could not get locks")
		}
	}	

	return nil
}

func (d *Database) CheckLocksDB(clients []string, seq int64) error {
	// log.Printf("Checking locks for %v seq %d\n", clients, seq)
	return d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(locksBucket)

		for _, client := range clients {
			locked := bucket.Get([]byte(client))
			if locked != nil {
				// log.Printf("%s already locked\n", client)
				return fmt.Errorf("%s already locked", client)
			}
		}
		return nil
	})
}

func (d *Database) CatchUpLockMap(lockMap map[string]bool) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(locksBucket)
		if err != nil {
			return fmt.Errorf("error deleting locks bucket: %v", err)
		}
		b, err := tx.CreateBucket(locksBucket)
		if err != nil {
			return fmt.Errorf("error recreating locks bucket: %v", err)
		}

		for client := range lockMap {
			err := b.Put([]byte(client), []byte("true"))
			if err != nil {
				log.Printf("Error putting lock %s during catch up\n", client)
			}
		}
		return nil
	})
}

func (d *Database) GetAllLocks() []string {
	var locks []string
	d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(locksBucket)

		c := b.Cursor()

		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			locks = append(locks, string(k))
		}
		return nil
	})
	return locks
}