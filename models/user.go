package models

type User struct {
	UID int
	GID int
	TCC int
}

func (u *User) GetID() int {
	return u.UID
}

func (u *User) Index() [][]string {
	return nil
}
