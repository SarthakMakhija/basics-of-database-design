package appendOnly

type InMemoryKeyValueOffsetTable struct {
	offsetByKey map[string]Offset
	keyValueLog *KeyValueLog
}

func NewInMemoryKeyValueOffsetTable(keyValueLog *KeyValueLog) InMemoryKeyValueOffsetTable {
	return InMemoryKeyValueOffsetTable{
		offsetByKey: make(map[string]Offset),
		keyValueLog: keyValueLog,
	}
}

func (table InMemoryKeyValueOffsetTable) Put(key []byte, value []byte) {
	startingOffset := table.keyValueLog.Put(KeyValuePair{Key: key, Value: value})
	table.offsetByKey[string(key)] = startingOffset
}

func (table InMemoryKeyValueOffsetTable) Get(key []byte) []byte {
	offset, exists := table.offsetByKey[string(key)]
	if exists {
		return table.keyValueLog.GetAtStartingOffset(offset).Value
	}
	return []byte{}
}
