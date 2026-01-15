package typereg

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
)

type ctor func(payload []byte) any

var ermu sync.RWMutex
var items map[string]ctor = make(map[string]ctor)

var rtmu sync.RWMutex
var types map[reflect.Type]string = make(map[reflect.Type]string)

func Register[T any](tname string, item ctor) {
	t := reflect.TypeFor[T]()

	// if t.Kind() == reflect.Pointer {
	// 	t = t.Elem()
	// }

	rtmu.Lock()
	types[t] = tname
	rtmu.Unlock()

	// if t.Kind() != reflect.Struct && t.Kind() != reflect.Interface {
	// 	panic("register: registered type must be struct or interface")
	// }
	// ctor := func(payload []byte) any {

	// 	//	vt := reflect.New(t).Interface()
	// 	var vt any
	// 	if err := serde.Deserialize(payload, &vt); err != nil {
	// 		slog.Error("registry: failed to deserialize", "type", t.Name(), "error", err)
	// 		panic(err)
	// 	}

	// 	//return reflect.ValueOf(vt).Elem().Interface()
	// 	return vt
	// 	//	var val V
	// }
	// if tname == "" {
	// 	tname = TypeNameFrom(item)
	// }

	ermu.Lock()
	items[tname] = item
	ermu.Unlock()
	slog.Info("event registered", "type", tname)
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
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	//	sep := strings.Split(t.PkgPath(), "/")
	sha := sha1.New()
	sha.Write([]byte(t.PkgPath()))
	bctx := base64.RawURLEncoding.EncodeToString(sha.Sum(nil))
	//bctx := sep[len(sep)-1]
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

func GetKind(in any) string {
	t := reflect.TypeOf(in)
	ermu.RLock()
	defer ermu.RUnlock()
	if tname, ok := types[t]; ok {
		return tname
	}
	slog.Error("guard: no type found, register it first", "type", t.Name())
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
