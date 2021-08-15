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

func (store Store) Close() {
	store.inMemoryKeyValueTable.Close()
}

func (store Store) Put(key []byte, value []byte) {
	store.inMemoryKeyValueTable.Put(key, value)
}

func (store Store) Get(key []byte) []byte {
	return store.inMemoryKeyValueTable.Get(key)
}

func createOrLoadInMemoryKeyValueOffsetTable(keyValueLog KeyValueLog) InMemoryKeyValueOffsetTable {
	if keyValueLog.IsANewlyCreatedKeyValueLog() {
		return NewInMemoryKeyValueOffsetTable(&keyValueLog)
	} else {
		return ReloadFrom(&keyValueLog)
	}
}
