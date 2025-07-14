package tokenBucket

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Vujovic0/NASP2024/config"
)

// Config represents the configuration structure for the token bucket.
// type Config struct {
// 	Capacity     int           `json:"capacity"`
// 	TimeInterval time.Duration `json:"timeInterval"`
// }

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

// func initializeTokenBucket() *TokenBucket {
// 	// Podrazumevane vrednosti ukoliko nema konfiguracije
// 	defaultCapacity := 10
// 	defaultInterval := 5000 * time.Millisecond

// 	// Postavljamo podrazumevane vrednosti
// 	config := Config{
// 		Capacity:     defaultCapacity,
// 		TimeInterval: defaultInterval,
// 	}

// 	// var config Config
// 	configData, err := os.ReadFile("config tokenBucket.json")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	err = json.Unmarshal(configData, &config)
// 	if err != nil {
// 		fmt.Println("Konfiguracioni fajl nije pronadjen, koriste se podrazumevane vrednosti.")
// 	}
// 	// fmt.Println(config)

// 	// Provera validnosti konfiguracije
// 	if config.Capacity <= 0 {
// 		config.Capacity = defaultCapacity
// 	}
// 	if config.TimeInterval <= 0 {
// 		config.TimeInterval = defaultInterval
// 	}

// 	tokenBucket := NewTokenBucket(config.Capacity, config.TimeInterval)

// 	return tokenBucket
// }

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

func (tb *TokenBucket) Consume() bool {
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
	return false
}

// Save state to file
func (tb *TokenBucket) SaveState(filename string) error {
	state := map[string]interface{}{
		"token_bucket_user": map[string]interface{}{
			"tokens":           tb.tokens,
			"last_refill_time": tb.lastRefillTime.Format(time.RFC3339),
		},
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

func (tb *TokenBucket) LoadState(filename string) error {
	// Load data from file
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// Parse JSON data into a map
	state := map[string]interface{}{}
	err = json.Unmarshal(data, &state)
	if err != nil {
		return err
	}

	// Retrieve data for "token_bucket_user"
	userState, ok := state["token_bucket_user"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid state format: missing token_bucket_user")
	}

	// Set values in TokenBucket
	tb.tokens = int(userState["tokens"].(float64))
	lastRefillTime, err := time.Parse(time.RFC3339, userState["last_refill_time"].(string))
	if err != nil {
		return err
	}
	tb.lastRefillTime = lastRefillTime

	return nil
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
