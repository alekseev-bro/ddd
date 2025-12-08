package natsstore

import (
	"reflect"
	"strings"
)

func MetaFromType[T any]() (aname string, bctx string) {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	aname = t.Name()
	sep := strings.Split(t.PkgPath(), "/")
	bctx = sep[len(sep)-1]
	return
}
