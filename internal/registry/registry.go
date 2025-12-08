package registry

import (
	"fmt"
	"log/slog"
	"reflect"
	"sync"

	"github.com/alekseev-bro/ddd/internal/serde"
)

type ctor func(payload []byte) any

type Type struct {
	serde.Serder
	ermu  sync.RWMutex
	items map[string]ctor
}

func New(s serde.Serder) *Type {
	return &Type{
		Serder: s,
		items:  make(map[string]ctor),
	}
}

func (r *Type) Register(item any) {
	t := reflect.TypeOf(item)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	slog.Info("event registered", "type", t.Name())

	if t.Kind() != reflect.Struct && t.Kind() != reflect.Interface {
		panic("register: registered type must be struct or interface")
	}
	ctor := func(payload []byte) any {

		vt := reflect.New(t).Interface()
		if err := r.Serder.Deserialize(payload, vt); err != nil {
			slog.Error("registry: failed to deserialize", "type", t.Name(), "error", err)
			panic(err)
		}
		return vt
		//	var val V

	}
	r.ermu.Lock()
	r.items[TypeNameFrom(item)] = ctor
	r.ermu.Unlock()

}

func TypeNameFrom(e any) string {
	if strev, ok := e.(fmt.Stringer); ok {
		return strev.String()
	}

	t := reflect.TypeOf(e)
	switch t.Kind() {
	case reflect.Struct:
		return t.Name()
	case reflect.Pointer:
		return t.Elem().Name()
	default:
		panic("unsupported type")

		//	json.Marshal()
	}
}

func (r *Type) GuardType(t any) string {
	tname := TypeNameFrom(t)
	r.ermu.RLock()
	defer r.ermu.RUnlock()
	if _, ok := r.items[tname]; ok {
		return tname
	}
	slog.Error("guard: no type found", "type", tname)
	panic("unrecovered")
}

func (r *Type) GetType(tname string, b []byte) any {
	r.ermu.RLock()
	defer r.ermu.RUnlock()

	if ct, ok := r.items[tname]; ok {

		return ct(b)
	}
	slog.Error("get type: no type found", "type", tname)
	panic("unrecovered")
}
