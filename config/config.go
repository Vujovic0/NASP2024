package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// PREFIXES FOR PROBABILISTIC STRUCTURES
var BloomFilterPrefix string = "bf_"
var CountMinSketchPrefix string = "cms_"
var HyperLogLogPrefix string = "hpp_"
var SimHashPrefix string = "sh_"

// WAL
var BlocksInSegment int = 5

// MEMTABLE
var SingleTableSize int = 10
var TablesCount int = 3
var ImplementationType string = "skiplist"

// SSTABLE
var SummarySparsity int = 5
var IndexSparsity int = 5
var SeparateFiles bool = false
var Compression bool = false
var VariableHeader bool = false

// LSM TREE
var MaxLevels int = 5
var CompactionType string = "tiered"

// ELEMENT CACHE
var CacheSize int = 5

// BLOCK MANAGER
var GlobalBlockSize int = 4096
var BlockManagerCacheSize int = 5

// TOKEN BUCKET
var TokensNum int = 10
var ResetingIntervalMs int = 1000

const TokenBucketStateKey = "token_bucket_state"

type Config struct {
	WAL               wALConfig               `json:"wal"`
	Memtable          memtableConfig          `json:"memtable"`
	SSTable           sSTableConfig           `json:"sstable"`
	LSMTree           lSMTreeConfig           `json:"lsm_tree"`
	Cache             cacheConfig             `json:"cache"`
	BlockManagerCache blockManagerCacheConfig `json:"block_manager_and_cache"`
	TokenBucket       tokenBucketConfig       `json:"token_bucket"`
}

type wALConfig struct {
	BlocksInSegment int `json:"blocks_in_segment"`
}

type memtableConfig struct {
	SingleTableSize    int    `json:"elements_in_single_table"`
	TablesCount        int    `json:"tables_count"`
	ImplementationType string `json:"implementation_type"`
	// IMPLEMENTATION CAN BE:
	// btree; skiplist; hashMap
}

type sSTableConfig struct {
	SummarySparsity int  `json:"summary_sparsity"`
	IndexSparsity   int  `json:"index_sparsity"`
	SeparateFiles   bool `json:"separate_files"`
	Compression     bool `json:"compression"`
	VariableHeader  bool `json:"variable_header"`
}

type lSMTreeConfig struct {
	MaxLevels      int    `json:"max_levels"`
	CompactionType string `json:"compaction_type"`
}

type cacheConfig struct {
	CacheSize int `json:"size"`
}

type blockManagerCacheConfig struct {
	BlockSizeB int `json:"block_size_in_b"`
	CacheSize  int `json:"cache_size"`
}

type tokenBucketConfig struct {
	TokensNum          int `json:"number_of_tokens"`
	ResetingIntervalMs int `json:"interval_of_reseting_in_ms"`
}

func SaveConfig(config *Config) error {
	filepath := "./config/previous_config.json"
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	err = os.WriteFile(filepath, data, 0644)
	if err != nil {
		return err
	}

	return nil
}

func LoadConfig() (*Config, error) {
	changableJson := "./configuration.json"
	changableData, err := os.ReadFile(changableJson)
	loadingError := false
	if err != nil {
		loadingError = true
	}

	var newConfig Config
	err = json.Unmarshal(changableData, &newConfig)
	if err != nil {
		loadingError = true
	}

	if loadingError {
		fmt.Println("Error has occured while loading configuration!")
		fmt.Println(err)
		for {
			fmt.Println("Do you want to load previous configuration? (y/n)")
			var input string
			_, _ = fmt.Scan(&input)
			input = strings.TrimSpace(input)
			if input == "y" {
				previousConfig := LoadPreviousConfig()
				SetConfigValues(previousConfig)
				return previousConfig, nil
			}
			if input == "n" {
				fmt.Println("Stopping the program.")
				os.Exit(0)
				break
			}
			fmt.Println("You need to input valid option! (y/n)")
		}
	}
	// DODAJ DA SE UPOREĐUJE STARI I NOVI CONFIG
	comparePreviousToNewConfig(&newConfig)
	SetConfigValues(&newConfig)
	SaveConfig(&newConfig)
	return &newConfig, nil
}

func comparePreviousToNewConfig(newConfig *Config) {
	previousJson := "./config/previous_config.json"
	previousData, err := os.ReadFile(previousJson)
	loadingError := false
	if err != nil {
		loadingError = true
	}
	var previousConfig Config
	err = json.Unmarshal(previousData, &previousConfig)
	if err != nil {
		loadingError = true
	}
	if loadingError {
		fmt.Println("Error has occured while loading previous configuration!")
		fmt.Println("Because of potential change of block size")
		fmt.Println("All the old data needs to be deleted to continue.")
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Println("Do you want to continue? (y/n)")

			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading input:", err)
				continue
			}

			input = strings.TrimSpace(input)

			if input == "y" {
				err := DeleteOldData()
				if err != nil {
					fmt.Println("Stopping the program.")
					os.Exit(0)
				}
				break
			}

			if input == "n" {
				fmt.Println("Stopping the program.")
				os.Exit(0)
				break
			}

			fmt.Println("Please enter 'y' or 'n'.")
		}
	}
	blockSize := newConfig.BlockManagerCache.BlockSizeB != previousConfig.BlockManagerCache.BlockSizeB
	variableHeader := newConfig.SSTable.VariableHeader != previousConfig.SSTable.VariableHeader
	if blockSize || variableHeader {
		fmt.Println("There is change in data writing parameter!")
		fmt.Println("(Block size or variable header setting...)")
		fmt.Println("All the old data needs to be deleted to continue.")
		//fmt.Println("all old data needs to be deleted.")
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Println("Do you want to continue? (y/n)")

			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading input:", err)
				continue
			}

			input = strings.TrimSpace(input)

			if input == "y" {
				err := DeleteOldData()
				if err != nil {
					fmt.Println("Stopping the program.")
					os.Exit(0)
				}
				break
			}

			if input == "n" {
				fmt.Println("Stopping the program.")
				os.Exit(0)
				break
			}

			fmt.Println("Please enter 'y' or 'n'.")
		}
	}
}

func LoadPreviousConfig() *Config {
	previousJson := "./config/previous_config.json"
	previousData, err := os.ReadFile(previousJson)
	loadingError := false
	if err != nil {
		loadingError = true
	}
	var previousConfig Config
	err = json.Unmarshal(previousData, &previousConfig)
	if err != nil {
		loadingError = true
	}
	if loadingError {
		fmt.Println("Error has occured while loading previous configuration!")
		fmt.Println("This will result in deletion of old data,")
		fmt.Println("because of potential change of block size.")
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Println("Do you want to load default configuration? (y/n)")

			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading input:", err)
				continue
			}

			input = strings.TrimSpace(input)

			if input == "y" {
				err := DeleteOldData()
				if err != nil {
					fmt.Println("Stopping the program.")
					os.Exit(0)
				}
				break
			}

			if input == "n" {
				fmt.Println("Stopping the program.")
				os.Exit(0)
				break
			}

			fmt.Println("Please enter 'y' or 'n'.")
		}
	}
	return &previousConfig
}

func DeleteFolderData(folderPath string) error {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.Type().IsRegular() || entry.Type()&fs.ModeSymlink != 0 {
			err := os.Remove(filepath.Join(folderPath, entry.Name()))
			if err != nil {
				return fmt.Errorf("failed to delete file %s: %w", entry.Name(), err)
			}
		}
	}

	return nil
}

func DeleteOldData() error {
	ssTableDataFolderpath := "./data"
	walFolderpath := "./wal/wals"
	walOffsetFilepath := "./wal/offset.bin"

	// DELETE SSTABLE DATA
	err := DeleteFolderData(ssTableDataFolderpath)
	if err != nil {
		return fmt.Errorf("Failed to delete SSTable data: %w", err)
	}

	// DELETE WAL OFFSET FILE
	err = os.Remove(walOffsetFilepath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Failed to delete offset.bin: %w", err)
	}

	// DELETE WAL DATA
	err = DeleteFolderData(walFolderpath)
	if err != nil {
		return fmt.Errorf("Failed to delete WAL data: %w", err)
	}

	return nil
}

func SetConfigValues(cfg *Config) {
	// WAL
	BlocksInSegment = cfg.WAL.BlocksInSegment

	// MEMTABLE
	SingleTableSize = cfg.Memtable.SingleTableSize
	TablesCount = cfg.Memtable.TablesCount
	ImplementationType = cfg.Memtable.ImplementationType

	// SSTABLE
	SummarySparsity = cfg.SSTable.SummarySparsity
	IndexSparsity = cfg.SSTable.IndexSparsity
	SeparateFiles = cfg.SSTable.SeparateFiles
	Compression = cfg.SSTable.Compression
	VariableHeader = cfg.SSTable.VariableHeader

	// LSM TREE
	MaxLevels = cfg.LSMTree.MaxLevels
	CompactionType = cfg.LSMTree.CompactionType

	// CACHE
	CacheSize = cfg.Cache.CacheSize

	// BLOCK MANAGER
	GlobalBlockSize = cfg.BlockManagerCache.BlockSizeB
	BlockManagerCacheSize = cfg.BlockManagerCache.CacheSize

	// TOKEN BUCKET
	TokensNum = cfg.TokenBucket.TokensNum
	ResetingIntervalMs = cfg.TokenBucket.ResetingIntervalMs
}
