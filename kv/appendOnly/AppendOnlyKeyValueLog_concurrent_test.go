package appendOnly_test

import (
	"bytes"
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"sort"
	"strconv"
	"sync"
	"testing"
)

func TestConcurrentPutInKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	execution := concurrentExecution{
		log:         appendOnly.NewKeyValueLog(fileName),
		keyPrefix:   "Key",
		valuePrefix: "Value",
	}

	numberOfGoroutines := 30
	execution.runPutWithIndexedKeyValue(numberOfGoroutines)
	execution.wg.Wait()

	ensureAllKeyValuePairsAreLogged := func() {
		loggedKeyValuePairs := execution.loggedKeyValuePairs()
		sourceKeyValuePairs := execution.sourceKeyValuePairs

		sort.Slice(loggedKeyValuePairs, func(i, j int) bool {
			return bytes.Compare(loggedKeyValuePairs[i].Key, loggedKeyValuePairs[j].Key) < 0
		})
		sort.Slice(sourceKeyValuePairs, func(i, j int) bool {
			return bytes.Compare(sourceKeyValuePairs[i].Key, sourceKeyValuePairs[j].Key) < 0
		})
		for index := range sourceKeyValuePairs {
			if bytes.Compare(sourceKeyValuePairs[index].Key, loggedKeyValuePairs[index].Key) != 0 ||
				bytes.Compare(sourceKeyValuePairs[index].Value, loggedKeyValuePairs[index].Value) != 0 {
				t.Fatalf("sourceKeyValuePair's key is %v, value is %v, loggedKeyValuePair's key is %v, value is %v",
					string(sourceKeyValuePairs[index].Key),
					string(sourceKeyValuePairs[index].Value),
					string(loggedKeyValuePairs[index].Key),
					string(loggedKeyValuePairs[index].Value),
				)
			}
		}
	}
	ensureAllKeyValuePairsAreLogged()
}

type concurrentExecution struct {
	offsetResult        offsetResult
	sourceKeyValuePairs []appendOnly.KeyValuePair
	wg                  sync.WaitGroup
	log                 appendOnly.KeyValueLog
	keyPrefix           string
	valuePrefix         string
}

type offsetResult struct {
	offsets []appendOnly.Offset
	mutex   sync.Mutex
}

func (execution *concurrentExecution) put(pair appendOnly.KeyValuePair) {
	defer execution.offsetResult.mutex.Unlock()
	defer execution.wg.Done()

	execution.offsetResult.mutex.Lock()
	execution.offsetResult.offsets = append(execution.offsetResult.offsets, execution.log.Put(pair))
}

func (execution *concurrentExecution) runPutWithIndexedKeyValue(numberOfGoroutines int) {
	execution.wg.Add(numberOfGoroutines)
	for index := 1; index <= numberOfGoroutines; index++ {
		pair := appendOnly.KeyValuePair{
			Key:   []byte(execution.keyPrefix + strconv.Itoa(index)),
			Value: []byte(execution.valuePrefix + strconv.Itoa(index)),
		}
		execution.sourceKeyValuePairs = append(execution.sourceKeyValuePairs, pair)
		go execution.put(pair)
	}
}

func (execution *concurrentExecution) loggedKeyValuePairs() []appendOnly.KeyValuePair {
	var pairs []appendOnly.KeyValuePair
	for _, offset := range execution.offsetResult.offsets {
		pairs = append(pairs, appendOnly.KeyValuePair{
			Key:   execution.log.GetAtStartingOffset(offset).Key,
			Value: execution.log.GetAtStartingOffset(offset).Value,
		})
	}
	return pairs
}
