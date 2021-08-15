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

func (table InMemoryKeyValueOffsetTable) Put(key string, value string) {
	startingOffset := table.keyValueLog.Put(KeyValuePair{Key: key, Value: value})
	table.offsetByKey[key] = startingOffset
}

func (table InMemoryKeyValueOffsetTable) Get(key string) string {
	offset, exists := table.offsetByKey[key]
	if exists {
		return table.keyValueLog.GetAtStartingOffset(offset).Value
	}
	return ""
}
