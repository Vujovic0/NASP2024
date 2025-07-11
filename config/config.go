package config

var pageSize int = 4096
var GlobalBlockSize int = pageSize * 1
var MaxLSMLevel byte = 3
var IndexSparsity byte = 5
var SummarySparsity byte = 5
var VariableEncoding bool = true
var UseCompactSSTable bool = true
var UseCompression bool = false
