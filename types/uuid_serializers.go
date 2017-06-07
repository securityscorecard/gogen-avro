package types

import "github.com/alanctgardner/gogen-avro/generator"

func AddUUIDSerializerToPackage(pkg *generator.Package) {
	pkg.AddFile("uuid_serializers.go", uuidSerializersFileContent)
}

var allowedFieldTypes = map[string]bool{
	"string": true, "[]string": true,
	"bool": true, "[]bool": true,
	"byte": true, "[]byte": true,

	// int
	"int": true, "[]int": true,
	"int32": true, "[]int32": true,
	"int64": true, "[]int64": true,

	// float
	"float32": true, "[]float32": true,
	"float64": true, "[]float64": true,

	// ip
	"IPAddress": true,
}

var typeSerializerFuncs = map[string]string{
	"byte": "byteSerializer", "[]byte": "byteSliceSerializer",
	"bool": "boolSerializer", "[]bool": "boolSliceSerializer",
	"string": "stringSerializer", "[]string": "stringSliceSerializer",

	// int
	"int": "intSerializer", "[]int": "intSliceSerializer",
	"int32": "intSerializer", "[]int32": "intSliceSerializer",
	"int64": "intSerializer", "[]int64": "intSliceSerializer",

	// float
	"float32": "floatSerializer", "[]float32": "floatSliceSerializer",
	"float64": "floatSerializer", "[]float64": "floatSliceSerializer",

	// IP related
	"IPAddress": "ipSerializer",
}

var uuidSerializersFileContent = `
import (
	"fmt"
	"reflect"
)

type fieldTypeSerializer func(interface{}) string

var ipSerializer = fieldTypeSerializer(func(i interface{}) string {
	vs := reflect.ValueOf(i).Convert(reflect.TypeOf([16]byte{})).Interface().([16]byte)
	out := ""
	for _, v := range vs {
		out += fmt.Sprintf("%d", v)
	}
	return out
})

var byteSerializer = fieldTypeSerializer(func(i interface{}) string {
	v := i.(byte)
	return fmt.Sprintf("%d", v)
})

var byteSliceSerializer = fieldTypeSerializer(func(i interface{}) string {
	vs := i.([]byte)
	out := ""
	for _, v := range vs {
		out += fmt.Sprintf("%d", v)
	}
	return out
})

var stringSerializer = fieldTypeSerializer(func(i interface{}) string {
	v := i.(string)
	return v
})

var stringSliceSerializer = fieldTypeSerializer(func(i interface{}) string {
	vs := i.([]string)
	out := ""
	for _, v := range vs {
		out += v
	}
	return out
})

var boolSerializer = fieldTypeSerializer(func(i interface{}) string {
	return fmt.Sprintf("%v", i)
})

var boolSliceSerializer = fieldTypeSerializer(func(i interface{}) string {
	vs := i.([]bool)
	out := ""
	for _, v := range vs {
		out += fmt.Sprintf("%v", v)
	}
	return out
})

var intSerializer = fieldTypeSerializer(func(i interface{}) string {
	return fmt.Sprintf("%d", i)
})

var intSliceSerializer = fieldTypeSerializer(func(i interface{}) string {
	vsint, ok := i.([]int)
	if ok {
		out := ""
		for _, v := range vsint {
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
	vsint32, ok := i.([]int32)
	if ok {
		out := ""
		for _, v := range vsint32 {
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
	vsint64, ok := i.([]int64)
	if ok {
		out := ""
		for _, v := range vsint64 {
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
	panic("invalid type: expected int, int32 or int64")
})

var floatSerializer = fieldTypeSerializer(func(i interface{}) string {
	return fmt.Sprintf("%.4f", i)
})

var floatSliceSerializer = fieldTypeSerializer(func(i interface{}) string {
	vsf32, ok := i.([]float32)
	if ok {
		out := ""
		for _, v := range vsf32 {
			out += fmt.Sprintf("%.4f", v)
		}
		return out
	}
	vsf64, ok := i.([]float64)
	if ok {
		out := ""
		for _, v := range vsf64 {
			out += fmt.Sprintf("%.4f", v)
		}
		return out
	}
	panic("invalid type: expected float32 or float64")
})
`
