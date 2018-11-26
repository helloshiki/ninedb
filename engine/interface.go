package engine

type Row interface {
	GetUID() int       //主键
	Index() [][]string //索引
}
