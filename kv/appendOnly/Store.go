package appendOnly

type Store struct {
	inMemoryKeyValueTable *InMemoryKeyValueOffsetTable
}

func Open(fileName string) Store {
	keyValueLog := NewKeyValueLog(fileName)
	inMemoryKeyValueTable := NewInMemoryKeyValueOffsetTable(&keyValueLog)

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
