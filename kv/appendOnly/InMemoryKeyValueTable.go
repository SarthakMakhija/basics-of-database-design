package appendOnly

type InMemoryKeyValueTable struct {
	valueByKey map[string]string
}

func NewInMemoryKeyValueTable() InMemoryKeyValueTable {
	return InMemoryKeyValueTable{
		valueByKey: make(map[string]string),
	}
}

func (table InMemoryKeyValueTable) Put(key string, value string) {
	table.valueByKey[key] = value
}

func (table InMemoryKeyValueTable) Get(key string) string {
	value, _ := table.valueByKey[key]
	return value
}
