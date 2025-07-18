package tokenBucket

import (
	"fmt"
	"time"

	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/lruCache"
	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/systemOps"
	"github.com/Vujovic0/NASP2024/wal"
)

type TokenBucket struct {
	capacity       int
	tokens         int
	timeInterval   time.Duration
	lastRefillTime time.Time // Time of the last token refill
}

func NewTokenBucket(capacity int, timeInterval time.Duration) *TokenBucket {
	return &TokenBucket{
		capacity:       capacity,
		tokens:         capacity,
		timeInterval:   timeInterval,
		lastRefillTime: time.Now(),
	}
}

func initializeTokenBucket() *TokenBucket {
	// Učitaj konfiguraciju iz globalnog config-a
	capacity := config.TokensNum
	interval := time.Duration(config.ResetingIntervalMs) * time.Millisecond

	tokenBucket := NewTokenBucket(capacity, interval)
	return tokenBucket
}

// StartRefill starts refilling the token bucket to its capacity every timeInterval.
func (tb *TokenBucket) StartRefill() {
	go func() {
		ticker := time.NewTicker(tb.timeInterval)
		for range ticker.C {
			tb.tokens = tb.capacity // Reset tokens to maximum capacity
			fmt.Println("Token bucket refilled to capacity:", tb.capacity)
		}
	}()
}

func (tb *TokenBucket) Consume(walFactory *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) bool {
	// Check if the time interval has passed since the last refill
	if time.Since(tb.lastRefillTime) >= tb.timeInterval {
		// Refill the token bucket to its maximum capacity
		tb.tokens = tb.capacity
		tb.lastRefillTime = time.Now()
		fmt.Println("Token bucket reffilled to capacity: ", tb.capacity)

	}

	// Check if there are tokens available
	if tb.tokens > 0 {
		// Consume one token
		tb.tokens--
		fmt.Println("Token consumed!")
		return true
	}
	fmt.Println("No tokens available.")

	value := fmt.Sprintf("%d;%s", tb.tokens, tb.lastRefillTime.Format(time.RFC3339))
	systemOps.SystemPut(walFactory, memtable, lruCache, config.TokenBucketStateKey, value)
	return false
}

func userConfig(defaultCapacity int, defaultInterval time.Duration) (int, time.Duration) {
	var answer string
	fmt.Printf("Podrazumevani broj tokena je %d, a podrazumevani interval resetovanja je %d ms.\n", defaultCapacity, defaultInterval.Milliseconds())
	for {
		fmt.Println("Da li zelite da promenite ove vrednosti?: ")
		fmt.Println("[1] DA ")
		fmt.Println("[2] NE ")
		fmt.Scanln(&answer)

		if answer == "1" {
			var capacity int
			var intervalMS int64

			// Unos broja tokena
			for {
				fmt.Print("Unesite novi broj tokena: ")
				_, err := fmt.Scanln(&capacity)
				if err == nil && capacity > 0 {
					break
				}
				fmt.Println("Pogresan unos! Molimo unesite ceo broj veci od 0.")
				// Cistimo buffer
				var dump string
				fmt.Scanln(&dump)
			}

			// Unos intervala resetovanja
			for {
				fmt.Print("Unesite novi interval resetovanja u milisekundama: ")
				_, err := fmt.Scanln(&intervalMS)
				if err == nil && intervalMS > 0 {
					break
				}
				fmt.Println("Pogresan unos! Molimo unesite ceo broj veci od 0.")
				var dump string
				fmt.Scanln(&dump)
			}

			interval := time.Duration(intervalMS) * time.Millisecond
			return capacity, interval
		} else if answer == "2" {
			return defaultCapacity, defaultInterval
		} else {
			fmt.Println("Pogresan unos, molimo vas da unesete 1 ili 2.")
		}
	}
}

// func main() {
// 	_, err := config.LoadConfig()
// 	if err != nil {
// 		fmt.Println("Greska pri ucitavanju konfiguracije:", err)
// 	}

// 	tokenBucket := initializeTokenBucket()
// 	fmt.Println(tokenBucket)

// 	capacity, interval := userConfig(tokenBucket.capacity, tokenBucket.timeInterval)
// 	tokenBucket = NewTokenBucket(capacity, interval)

// 	// Load state from file
// 	err = tokenBucket.LoadState("token_bucket_state.json")
// 	if err != nil {
// 		fmt.Println("No previous state found, starting fresh.")
// 	}

// 	// Start token refill
// 	// tokenBucket.StartRefill()

// 	// Simulate token consumption
// 	for i := 0; i < 7; i++ {
// 		if tokenBucket.Consume() {
// 			// fmt.Println("Token consumed!")
// 		} else {
// 			// fmt.Println("No tokens available.")
// 		}
// 		time.Sleep(1 * time.Second)
// 	}

// 	err = tokenBucket.SaveState("token_bucket_state.json")
// 	if err != nil {
// 		fmt.Println("Error saving state:", err)
// 	}

// }
