package util

import (
	"fmt"
	"github.com/cockroachdb/pebble"
)

func DBGet(db *pebble.DB, key string) ([]byte, error) {
	value, closer, err := db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	
	defer func() {
		err := closer.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()

	return value, nil
}
