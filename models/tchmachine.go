package models

type TchMachine struct {
	ID  int
	GID int
	UID int
}

//索引列的值暂不支持修改，如需修改，可以先删除记录，再插入新记录
func (t *TchMachine) Index() [][]string {
	return [][]string{
		{"GID", "UID"},
		{"GID"},
		{"UID"},
	}

}

func (t *TchMachine) GetUID() int {
	return t.ID
}
