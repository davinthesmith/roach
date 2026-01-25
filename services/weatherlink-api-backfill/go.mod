module weatherlink-api-backfill

go 1.22

require github.com/roach/weatherlink-lib v0.0.0

require github.com/confluentinc/confluent-kafka-go/v2 v2.3.0 // indirect

replace github.com/roach/weatherlink-lib => ../weatherlink-lib
