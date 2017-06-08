package types

import "github.com/alanctgardner/gogen-avro/generator"

// AddUUIDSerializerToPackage will add a file with Serializer functions
// to the generated package code
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

	// unions
	"UnionNullString":    true,
	"UnionNullInt":       true,
	"UnionNullLong":      true,
	"UnionNullFloat":     true,
	"UnionNullDouble":    true,
	"UnionNullBool":      true,
	"UnionNullIPAddress": true,
}

var typeSerializerFuncs = map[string]string{
	"byte": "byteSerializer", "[]byte": "byteSliceSerializer",
	"bool": "boolSerializer", "[]bool": "boolSliceSerializer",
	"string": "stringSerializer", "[]string": "stringSliceSerializer",

	// int
	"int": "intSerializer", "[]int": "intSliceSerializer",
	"int32": "int32Serializer", "[]int32": "int32SliceSerializer",
	"int64": "int64Serializer", "[]int64": "int64SliceSerializer",

	// float
	"float32": "float32Serializer", "[]float32": "float32SliceSerializer",
	"float64": "float64Serializer", "[]float64": "float64SliceSerializer",

	// IP related
	"IPAddress": "ipSerializer",

	// unions
	"UnionNullString":    "unionNullStringSerializer",
	"UnionNullInt":       "unionNullIntSerializer",
	"UnionNullLong":      "unionNullLongSerializer",
	"UnionNullFloat":     "unionNullFloatSerializer",
	"UnionNullDouble":    "unionNullDoubleSerializer",
	"UnionNullBool":      "unionNullBoolSerializer",
	"UnionNullIPAddress": "unionNullIPAddressSerializer",
}

var uuidSerializersFileContent = `
import (
	"fmt"
	"reflect"
)

func ipSerializer(i interface{}) string {
	vs := reflect.ValueOf(i).Convert(reflect.TypeOf([16]byte{})).Interface().([16]byte)
	out := ""
	for _, v := range vs {
		out += fmt.Sprintf("%d", v)
	}
	return out
}

// byte

func byteSerializer(v byte) string {
	return fmt.Sprintf("%d", v)
}

func byteSliceSerializer(vs []byte) string {
	out := ""
	for _, v := range vs {
		out += byteSerializer(v)
	}
	return out
}

// string

func stringSerializer(v string) string {
	return v
}

func stringSliceSerializer(vs []string) string {
	out := ""
	for _, v := range vs {
		out += v
	}
	return out
}

// bool

func boolSerializer(v bool) string {
	return fmt.Sprintf("%v", v)
}

func boolSliceSerializer(vs []bool) string {
	out := ""
	for _, v := range vs {
		out += boolSerializer(v)
	}
	return out
}

// int, int32, int64

func intSerializer(v int) string {
	return fmt.Sprintf("%d", v)
}

func int32Serializer(v int32) string {
	return fmt.Sprintf("%d", v)
}

func int64Serializer(v int64) string {
	return fmt.Sprintf("%d", v)
}

func intSliceSerializer(vs []int) string {
	out := ""
	for _, v := range vs {
		out += intSerializer(v)
	}
	return out
}

func int32SliceSerializer(vs []int32) string {
	out := ""
	for _, v := range vs {
		out += int32Serializer(v)
	}
	return out
}

func int64SliceSerializer(vs []int64) string {
	out := ""
	for _, v := range vs {
		out += int64Serializer(v)
	}
	return out
}

// float32, float64

func float32Serializer(v float32) string {
	return fmt.Sprintf("%.4f", v)
}

func float64Serializer(v float64) string {
	return fmt.Sprintf("%.4f", v)
}

func float32SliceSerializer(vs []float32) string {
	out := ""
	for _, v := range vs {
		out += float32Serializer(v)
	}
	return out
}

func float64SliceSerializer(vs []float64) string {
	out := ""
	for _, v := range vs {
		out += float64Serializer(v)
	}
	return out
}

// unions

func unionNullStringSerializer(un UnionNullString) string {
	if un.UnionType == UnionNullStringTypeEnumString {
		return un.String
	}
	return ""
}

func unionNullIntSerializer(un UnionNullInt) string {
	if un.UnionType == UnionNullIntTypeEnumInt {
		return fmt.Sprintf("%d", un.Int)
	}
	return ""
}

func unionNullLongSerializer(un UnionNullLong) string {
	if un.UnionType == UnionNullLongTypeEnumLong {
		return fmt.Sprintf("%d", un.Long)
	}
	return ""
}

func unionNullFloatSerializer(un UnionNullFloat) string {
	if un.UnionType == UnionNullFloatTypeEnumFloat {
		return fmt.Sprintf("%.4f", un.Float)
	}
	return ""
}

func unionNullDoubleSerializer(un UnionNullDouble) string {
	if un.UnionType == UnionNullDoubleTypeEnumDouble {
		return fmt.Sprintf("%.4f", un.Double)
	}
	return ""
}

func unionNullBoolSerializer(un UnionNullBool) string {
	if un.UnionType == UnionNullBoolTypeEnumBool {
		return fmt.Sprintf("%v", un.Bool)
	}
	return ""
}

func unionNullIPAddressSerializer(un UnionNullIPAddress) string {
	if un.UnionType == UnionNullIPAddressTypeEnumIPAddress {
		out := ""
		for _, v := range un.IPAddress {
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
	return ""
}
`
