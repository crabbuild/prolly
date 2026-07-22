package main

type protocolRow struct {
	ContractVersion     string  `json:"contract_version"`
	Kind                string  `json:"kind"`
	Implementation      string  `json:"implementation"`
	Revision            string  `json:"revision"`
	Records             int     `json:"records"`
	Repetition          int     `json:"repetition"`
	Operation           string  `json:"operation"`
	Pattern             string  `json:"pattern"`
	CacheState          string  `json:"cache_state"`
	LogicalOperations   int     `json:"logical_operations"`
	ObservedItems       int     `json:"observed_items"`
	TotalNS             uint64  `json:"total_ns"`
	NSPerOperation      float64 `json:"ns_per_operation"`
	OperationsPerSecond float64 `json:"operations_per_second"`
	P50NS               *uint64 `json:"p50_ns"`
	P95NS               *uint64 `json:"p95_ns"`
	P99NS               *uint64 `json:"p99_ns"`
	MaxNS               *uint64 `json:"max_ns"`
	ChunkReads          *uint64 `json:"chunk_reads"`
	ChunkWrites         *uint64 `json:"chunk_writes"`
	BytesRead           *uint64 `json:"bytes_read"`
	BytesWritten        *uint64 `json:"bytes_written"`
	ResultEntries       int     `json:"result_entries"`
	DBBytes             uint64  `json:"db_bytes"`
	WALBytes            uint64  `json:"wal_bytes"`
	SHMBytes            uint64  `json:"shm_bytes"`
	TotalDatabaseBytes  uint64  `json:"total_database_bytes"`
	ExpectedEntries     int     `json:"expected_entries"`
	ObservedEntries     int     `json:"observed_entries"`
	QueryStrategy       *string `json:"query_strategy"`
	Validated           bool    `json:"validated"`
	Error               string  `json:"error"`
}

func pointer(value uint64) *uint64 { return &value }

func rate(operations int, totalNS uint64) float64 {
	if totalNS == 0 {
		return 0
	}
	return float64(operations) / (float64(totalNS) / 1_000_000_000)
}

func nearestRank(values []uint64, quantile float64) *uint64 {
	if len(values) == 0 {
		return nil
	}
	sorted := append([]uint64(nil), values...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	rank := int(quantile*float64(len(sorted)) + 0.999999999)
	if rank < 1 {
		rank = 1
	}
	return pointer(sorted[rank-1])
}
