module github.com/Vujovic0/NASP2024

go 1.23.2

replace github.com/Vujovic0/NASP2024/probabilisticDataStructures/bloomFilter => ./probabilisticDataStructures/bloomFilter

replace github.com/Vujovic0/NASP2024/probabilisticDataStructures/cms => ./probabilisticDataStructures/cms

replace github.com/Vujovic0/NASP2024/probabilisticDataStructures/hyperloglog => ./probabilisticDataStructures/hyperloglog

replace github.com/Vujovic0/NASP2024/ssTable => ./ssTable

replace github.com/Vujovic0/NASP2024/wal => ./wal

require (
	github.com/Vujovic0/NASP2024/probabilisticDataStructures/cms v0.0.0-00010101000000-000000000000 // indirect
	github.com/Vujovic0/NASP2024/probabilisticDataStructures/hyperloglog v0.0.0-00010101000000-000000000000 // indirect
)
