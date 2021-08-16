package appendOnly

type Store struct {
	inMemoryKeyValueTable *InMemoryKeyValueOffsetTable
	fileLock              FileLock
}

func Open(fileName string) Store {
	acquireLockOrFail := func() FileLock {
		fileLock := AcquireExclusiveLock(fileName)
		if fileLock.Err != nil {
			panic(fileLock.Err)
		}
		return fileLock
	}

	keyValueLog := NewKeyValueLog(fileName)
	fileLock := acquireLockOrFail()
	inMemoryKeyValueTable := createOrLoadInMemoryKeyValueOffsetTable(keyValueLog)

	return Store{
		inMemoryKeyValueTable: &inMemoryKeyValueTable,
		fileLock:              fileLock,
	}
}

func (store Store) Put(key []byte, value []byte) {
	store.inMemoryKeyValueTable.Put(key, value)
}

func (store Store) Get(key []byte) []byte {
	return store.inMemoryKeyValueTable.Get(key)
}

func (store Store) Close() {
	releaseLockOrFail := func(fileLock FileLock) {
		if err := fileLock.Release(); err != nil {
			panic(err)
		}
	}
	store.inMemoryKeyValueTable.Close()
	releaseLockOrFail(store.fileLock)
}

func createOrLoadInMemoryKeyValueOffsetTable(keyValueLog KeyValueLog) InMemoryKeyValueOffsetTable {
	if keyValueLog.IsANewlyCreatedKeyValueLog() {
		return NewInMemoryKeyValueOffsetTable(&keyValueLog)
	} else {
		return ReloadFrom(&keyValueLog)
	}
}
