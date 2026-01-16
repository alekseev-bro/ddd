package typereg

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
)

type registry struct {
	mu    sync.RWMutex
	ctors map[string]ctor
	types map[reflect.Type]string
}

func New() *registry {
	return &registry{
		ctors: make(map[string]ctor),
		types: make(map[reflect.Type]string),
	}
}

type ctor = func() any

func (r *registry) Register(tname string, c ctor) {

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.types[reflect.TypeOf(c())]; ok {
		panic(fmt.Sprintf("type %v is already registered", reflect.TypeOf(c())))
	}
	if _, ok := r.ctors[tname]; ok {
		panic(fmt.Sprintf("type %q is already registered", tname))
	}
	r.types[reflect.TypeOf(c())] = tname
	r.ctors[tname] = c
	slog.Info("event registered", "type", tname)
}

func (r *registry) Create(name string) (any, error) {
	r.mu.RLock()
	ct, ok := r.ctors[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("registry: unknown type name %q", name)
	}
	return ct(), nil
}

func (r *registry) NameFor(in any) (string, error) {
	if in == nil {
		return "", errors.New("registry: cannot get name for nil")
	}

	t := reflect.TypeOf(in)

	r.mu.RLock()
	name, ok := r.types[t]
	r.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("registry: type %v is not registered", t)
	}
	return name, nil
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
		return fmt.Sprintf("%s%s%s", t.Name(), delim, bctx)
	case reflect.Pointer:
		return fmt.Sprintf("%s%s%s", t.Elem().Name(), delim, bctx)
	default:

		panic("unsupported type")

		//	json.Marshal()
	}

}
