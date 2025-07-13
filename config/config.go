package config

import (
	"encoding/json"
	"fmt"
	"os"
)

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
	SummarySparsity  int  `json:"summary_sparsity"`
	IndexSparsity    int  `json:"index_sparsity"`
	SeparateFiles    bool `json:"separate_files"`
	Compression      bool `json:"compression"`
	VariableEncoding bool `json:"variable_encoding"`
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

func LoadConfig() (*Config, error) {
	changableJson := "./configuration.json"
	changableData, err := os.ReadFile(changableJson)
	if err != nil {
		return nil, err
	}

	var newConfig Config
	err = json.Unmarshal(changableData, &newConfig)
	if err != nil {
		fmt.Println("Error has occured while loading configuration!")
		fmt.Println("Previous config loading...")
		previousConfig, err := LoadPreviousConfig()
		if err != nil {
			//
		}
		SetConfigValues(previousConfig)
		return previousConfig, nil
	}

	SetConfigValues(&newConfig)

	return &newConfig, nil
}

func LoadPreviousConfig() (*Config, error) {
	previousJson := "./config/previous_config.json"
	previousData, err := os.ReadFile(previousJson)
	if err != nil {
		return nil, err
	}
	var previousConfig Config
	err = json.Unmarshal(previousData, &previousConfig)
	if err != nil {
		return nil, err
	}
	return &previousConfig, nil
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
	VariableHeader = cfg.SSTable.VariableEncoding

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
