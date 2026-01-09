package typereg

import (
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"

	"github.com/alekseev-bro/ddd/internal/serde"
)

type ctor func(payload []byte) any

var ermu sync.RWMutex
var items map[string]ctor = make(map[string]ctor)

func Register(item any) {
	t := reflect.TypeOf(item)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct && t.Kind() != reflect.Interface {
		panic("register: registered type must be struct or interface")
	}
	ctor := func(payload []byte) any {

		vt := reflect.New(t).Interface()
		if err := serde.Deserialize(payload, vt); err != nil {
			slog.Error("registry: failed to deserialize", "type", t.Name(), "error", err)
			panic(err)
		}

		return reflect.ValueOf(vt).Elem().Interface()
		//	var val V

	}
	tn := TypeNameFrom(item)

	ermu.Lock()
	items[tn] = ctor
	ermu.Unlock()
	slog.Info("event registered", "type", tn)
}

func TypeNameFor[T any](opts ...typeNameFromOption) string {
	var zero T
	return TypeNameFrom(zero, opts...)
}

type typeNameFromOption string

func WithDelimiter(delimiter string) typeNameFromOption {
	return typeNameFromOption(delimiter)
}

func TypeNameFrom(e any, opts ...typeNameFromOption) string {
	if strev, ok := e.(fmt.Stringer); ok {

		return strev.String()
	}
	delim := "::"
	for _, opt := range opts {
		delim = string(opt)
	}
	t := reflect.TypeOf(e)
	sep := strings.Split(t.PkgPath(), "/")
	bctx := sep[len(sep)-1]
	switch t.Kind() {

	case reflect.Struct:
		return fmt.Sprintf("%s%s%s", bctx, delim, t.Name())
	case reflect.Pointer:
		return fmt.Sprintf("%s%s%s", bctx, delim, t.Elem().Name())
	default:

		panic("unsupported type")

		//	json.Marshal()
	}

}

func GetKind(t any) string {
	tname := TypeNameFrom(t)
	ermu.RLock()
	defer ermu.RUnlock()
	if _, ok := items[tname]; ok {
		return tname
	}
	slog.Error("guard: no type found, register it first", "type", tname)
	panic("unrecovered")
}

func GetType(tname string, b []byte) any {
	ermu.RLock()
	defer ermu.RUnlock()

	if ct, ok := items[tname]; ok {

		return ct(b)
	}
	slog.Error("get type: no type found", "type", tname)
	panic("unrecovered")
}
