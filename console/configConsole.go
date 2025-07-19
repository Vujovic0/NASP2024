package console

import (
	"fmt"
	"time"

	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/lruCache"
	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/tokenBucket"
	"github.com/Vujovic0/NASP2024/wal"
)

func ChangeConfigurationMenu(walFactory *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache, tokenBucket *tokenBucket.TokenBucket) {
	for {
		fmt.Println("--CONFIGURATION MENU--")
		fmt.Println("Choose a configuration to change:")
		fmt.Println("1. TokenBucket")
		fmt.Println("2. ...")
		fmt.Println("3. ....")
		fmt.Println("0. Back")
		var choice int
		fmt.Scan(&choice)
		switch choice {
		case 1:
			ChangeTokenBucketConfig(tokenBucket)
		case 2:
			//
		case 0:
			return
		default:
			fmt.Println("Invalid choice, please try again.")
		}
	}
}

func ChangeTokenBucketConfig(tokenBucket *tokenBucket.TokenBucket) {
	capacity, interval := tokenBucket.UserConfig()
	config.TokensNum = capacity
	config.ResetingIntervalMs = int(interval.Milliseconds())
	tokenBucket.Capacity = capacity
	tokenBucket.Tokens = capacity
	tokenBucket.TimeInterval = interval
	tokenBucket.LastRefillTime = time.Now()
	fmt.Println("TokenBucket configuration updated successfully.")
}
