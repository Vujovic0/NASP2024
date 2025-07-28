package tokenBucket

import (
	"fmt"
	"strings"
	"time"

	"github.com/Vujovic0/NASP2024/config"
	"github.com/Vujovic0/NASP2024/lruCache"
	"github.com/Vujovic0/NASP2024/memtableStructures"
	"github.com/Vujovic0/NASP2024/systemOps"
	"github.com/Vujovic0/NASP2024/wal"
)

type TokenBucket struct {
	Capacity       int
	Tokens         int
	TimeInterval   time.Duration
	LastRefillTime time.Time // Time of the last token refill
}

func NewTokenBucket(capacity int, timeInterval time.Duration) *TokenBucket {
	return &TokenBucket{
		Capacity:       capacity,
		Tokens:         capacity,
		TimeInterval:   timeInterval,
		LastRefillTime: time.Now(),
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
		ticker := time.NewTicker(tb.TimeInterval)
		for range ticker.C {
			tb.Tokens = tb.Capacity // Reset tokens to maximum capacity
			fmt.Println("Token bucket refilled to capacity:", tb.Capacity)
		}
	}()
}

func (tb *TokenBucket) Consume(walFactory *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) bool {
	// Check if the time interval has passed since the last refill
	if time.Since(tb.LastRefillTime) >= tb.TimeInterval {
		// Refill the token bucket to its maximum capacity
		tb.Tokens = tb.Capacity
		tb.LastRefillTime = time.Now()
		fmt.Println("Token bucket reffilled to capacity: ", tb.Capacity)

	}

	// Check if there are tokens available
	if tb.Tokens > 0 {
		// Consume one token
		tb.Tokens--
		//fmt.Println("Token consumed!")
		return true
	}
	fmt.Println("No tokens available.")

	value := fmt.Sprintf("%d;%s", tb.Tokens, tb.LastRefillTime.Format(time.RFC3339))
	systemOps.SystemPut(walFactory, memtable, lruCache, config.TokenBucketStateKey, value)
	return false
}

// Funkcija ucitava token bucket iz sistema ili kreira novi ako ne postoji
func LoadOrCreateTokenBucket(walFactory *wal.WAL, memtable *memtableStructures.MemTableManager, lruCache *lruCache.LRUCache) *TokenBucket {
	tbState, found := systemOps.SystemGet(lruCache, memtable, config.TokenBucketStateKey)
	if found {
		parts := strings.Split(tbState, ";")
		if len(parts) == 2 {
			var tokens int
			// Parsiranje tokena i vremena poslednjeg punjenja
			fmt.Sscanf(parts[0], "%d", &tokens)
			lastRefill, err := time.Parse(time.RFC3339, parts[1])
			if err == nil {
				return &TokenBucket{
					Capacity:       config.TokensNum,
					Tokens:         tokens,
					TimeInterval:   time.Duration(config.ResetingIntervalMs) * time.Millisecond,
					LastRefillTime: lastRefill,
				}
			}
		}
	}
	// Ako nije pronadjen, kreiramo novi i upisujemo u sistem
	tb := NewTokenBucket(config.TokensNum, time.Duration(config.ResetingIntervalMs)*time.Millisecond)
	value := fmt.Sprintf("%d;%s", tb.Tokens, tb.LastRefillTime.Format(time.RFC3339))
	systemOps.SystemPut(walFactory, memtable, lruCache, config.TokenBucketStateKey, value)
	return tb
}

func (tb *TokenBucket) UserConfig() (int, time.Duration) {
	var answer string
	fmt.Printf("Podrazumevani broj tokena je %d, a podrazumevani interval resetovanja je %d ms.\n", tb.Capacity, tb.TimeInterval.Milliseconds())
	for {
		fmt.Println("Da li zelite da promenite ove vrednosti?: ")
		fmt.Println("[1] DA ")
		fmt.Println("[2] NE ")
		fmt.Scanln(&answer)
		if answer == "" {
			fmt.Scanln(&answer)
		}

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
			return tb.Capacity, tb.TimeInterval
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
