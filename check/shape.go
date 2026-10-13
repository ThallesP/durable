package check

import (
	"fmt"
	"go/types"
	"reflect"
	"slices"
	"strings"
)

type Shape struct {
	Kind   string            `json:"kind"`
	Name   string            `json:"name,omitempty"`
	Elem   *Shape            `json:"elem,omitempty"`
	Key    *Shape            `json:"key,omitempty"`
	Fields map[string]*Shape `json:"fields,omitempty"`
}

func ShapeOf(t types.Type) *Shape {
	return shapeOf(t, map[string]bool{})
}

func shapeOf(t types.Type, seen map[string]bool) *Shape {
	t = types.Unalias(t)
	if ptr, ok := t.(*types.Pointer); ok {
		return shapeOf(ptr.Elem(), seen)
	}
	name := types.TypeString(t, nil)
	if decodesItself(t) {
		return &Shape{Kind: "custom", Name: name}
	}
	if _, ok := t.(*types.Named); ok {
		if seen[name] {
			return &Shape{Kind: "ref", Name: name}
		}
		seen[name] = true
		defer delete(seen, name)
	}

	switch u := t.Underlying().(type) {
	case *types.Basic:
		return &Shape{Kind: basicKind(u)}
	case *types.Slice:
		if elem, ok := types.Unalias(u.Elem()).Underlying().(*types.Basic); ok && elem.Kind() == types.Byte {
			return &Shape{Kind: "bytes"}
		}
		return &Shape{Kind: "list", Elem: shapeOf(u.Elem(), seen)}
	case *types.Array:
		return &Shape{Kind: "list", Elem: shapeOf(u.Elem(), seen)}
	case *types.Map:
		return &Shape{Kind: "map", Key: shapeOf(u.Key(), seen), Elem: shapeOf(u.Elem(), seen)}
	case *types.Struct:
		return &Shape{Kind: "struct", Fields: structFields(u, seen)}
	case *types.Interface:
		return &Shape{Kind: "any"}
	}
	return &Shape{Kind: "unknown", Name: name}
}

func basicKind(b *types.Basic) string {
	info := b.Info()
	switch {
	case info&types.IsString != 0:
		return "string"
	case info&types.IsBoolean != 0:
		return "bool"
	case info&types.IsUnsigned != 0:
		return "uint"
	case info&types.IsInteger != 0:
		return "int"
	case info&types.IsFloat != 0:
		return "float"
	}
	return "unknown"
}

func structFields(st *types.Struct, seen map[string]bool) map[string]*Shape {
	fields := map[string]*Shape{}
	for i := range st.NumFields() {
		field := st.Field(i)
		name, opts, _ := strings.Cut(reflect.StructTag(st.Tag(i)).Get("json"), ",")
		if name == "-" && opts == "" {
			continue
		}
		if field.Embedded() && name == "" {
			embedded := shapeOf(field.Type(), seen)
			if embedded.Kind == "struct" {
				for k, v := range embedded.Fields {
					if _, ok := fields[k]; !ok {
						fields[k] = v
					}
				}
				continue
			}
		}
		if !field.Exported() {
			continue
		}
		if name == "" {
			name = field.Name()
		}
		shape := shapeOf(field.Type(), seen)
		if slices.Contains(strings.Split(opts, ","), "string") && shape.Kind != "struct" {
			shape = &Shape{Kind: "string"}
		}
		fields[name] = shape
	}
	return fields
}

func decodesItself(t types.Type) bool {
	methods := types.NewMethodSet(types.NewPointer(t))
	return methods.Lookup(nil, "UnmarshalJSON") != nil || methods.Lookup(nil, "UnmarshalText") != nil
}

func (s *Shape) String() string {
	switch s.Kind {
	case "custom", "ref":
		return s.Name
	case "list":
		return "[]" + s.Elem.String()
	case "map":
		return fmt.Sprintf("map[%s]%s", s.Key, s.Elem)
	}
	return s.Kind
}

var widenings = map[[2]string]bool{
	{"int", "float"}:  true,
	{"uint", "int"}:   true,
	{"uint", "float"}: true,
}

func Incompatible(saved, current *Shape) []string {
	return incompatible(saved, current, "")
}

func incompatible(saved, current *Shape, path string) []string {
	if saved == nil || current == nil || current.Kind == "any" || current.Kind == "unknown" || saved.Kind == "unknown" {
		return nil
	}
	if saved.Kind == "ref" || current.Kind == "ref" {
		return nil
	}
	at := path
	if at == "" {
		at = "the value"
	}
	if saved.Kind == "custom" || current.Kind == "custom" {
		if saved.Kind == current.Kind && saved.Name == current.Name {
			return nil
		}
		return []string{fmt.Sprintf("%s was %s, now %s", at, saved, current)}
	}
	if saved.Kind != current.Kind {
		if widenings[[2]string{saved.Kind, current.Kind}] {
			return nil
		}
		return []string{fmt.Sprintf("%s was %s, now %s", at, saved, current)}
	}

	switch saved.Kind {
	case "list":
		return incompatible(saved.Elem, current.Elem, path+"[]")
	case "map":
		if saved.Key.Kind != current.Key.Kind {
			return []string{fmt.Sprintf("%s keys were %s, now %s", at, saved.Key, current.Key)}
		}
		return incompatible(saved.Elem, current.Elem, path+"[]")
	case "struct":
		problems := []string{}
		for _, name := range sortedKeys(saved.Fields) {
			field := fieldFold(current.Fields, name)
			fieldPath := strings.TrimPrefix(path+"."+name, ".")
			if field == nil {
				problems = append(problems, fmt.Sprintf("field %s was removed or renamed; saved data still has it and strict decoding rejects it", fieldPath))
				continue
			}
			problems = append(problems, incompatible(saved.Fields[name], field, fieldPath)...)
		}
		return problems
	}
	return nil
}

func Describe(saved, current *Shape) string {
	added := addedFields(saved, current, "")
	if len(added) == 0 {
		return fmt.Sprintf("%s, now %s", saved, current)
	}
	return "added " + strings.Join(added, ", ")
}

func addedFields(saved, current *Shape, path string) []string {
	if saved == nil || current == nil || saved.Kind != current.Kind {
		return nil
	}
	switch saved.Kind {
	case "list", "map":
		return addedFields(saved.Elem, current.Elem, path+"[]")
	case "struct":
		added := []string{}
		for _, name := range sortedKeys(current.Fields) {
			fieldPath := strings.TrimPrefix(path+"."+name, ".")
			old := fieldFold(saved.Fields, name)
			if old == nil {
				added = append(added, fieldPath)
				continue
			}
			added = append(added, addedFields(old, current.Fields[name], fieldPath)...)
		}
		return added
	}
	return nil
}

func fieldFold(fields map[string]*Shape, name string) *Shape {
	if field, ok := fields[name]; ok {
		return field
	}
	for k, v := range fields {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
