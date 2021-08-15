package appendOnly

type Store struct {
	inMemoryKeyValueTable *InMemoryKeyValueOffsetTable
}

func Open(fileName string) Store {
	keyValueLog := NewKeyValueLog(fileName)
	inMemoryKeyValueTable := createOrLoadInMemoryKeyValueOffsetTable(keyValueLog)

	return Store{
		inMemoryKeyValueTable: &inMemoryKeyValueTable,
	}
}

func (store Store) Put(key string, value string) {
	store.inMemoryKeyValueTable.Put(key, value)
}

func (store Store) Get(key string) string {
	return store.inMemoryKeyValueTable.Get(key)
}

func createOrLoadInMemoryKeyValueOffsetTable(keyValueLog KeyValueLog) InMemoryKeyValueOffsetTable {
	var inMemoryKeyValueTable InMemoryKeyValueOffsetTable
	if keyValueLog.IsANewlyCreatedKeyValueLog() {
		inMemoryKeyValueTable = NewInMemoryKeyValueOffsetTable(&keyValueLog)
	}
	return inMemoryKeyValueTable
}
