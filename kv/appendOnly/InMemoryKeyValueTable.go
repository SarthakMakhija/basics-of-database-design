package appendOnly

type InMemoryKeyValueTable struct {
	valueByKey  map[string]Offset
	keyValueLog *KeyValueLog
}

func NewInMemoryKeyValueTable(keyValueLog *KeyValueLog) InMemoryKeyValueTable {
	return InMemoryKeyValueTable{
		valueByKey:  make(map[string]Offset),
		keyValueLog: keyValueLog,
	}
}

func (table InMemoryKeyValueTable) Put(key string, value string) {
	startingOffset := table.keyValueLog.Put(KeyValuePair{Key: key, Value: value})
	table.valueByKey[key] = startingOffset
}

func (table InMemoryKeyValueTable) Get(key string) string {
	offset, exists := table.valueByKey[key]
	if exists {
		return table.keyValueLog.GetAtStartingOffset(offset).Value
	}
	return ""
}
