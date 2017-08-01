package types

import (
	"fmt"

	"github.com/alanctgardner/gogen-avro/generator"
)

// AddUUIDSerializerToPackage will add a file with Serializer functions
// to the generated package code
func AddUUIDSerializerToPackage(pkg *generator.Package, requiredSerializers []string) {
	// Don't create the file if no uuid serializers are required
	if len(requiredSerializers) == 0 {
		return
	}

	fName := "uuid_serializers.go"

	pkg.AddImport(fName, "fmt")
	pkg.AddConstant(fName, "FieldSeparator", string(0x1E))
	pkg.AddConstant(fName, "ArraySeparator", string(0x1F))

	for _, reqSer := range requiredSerializers {
		ser, ok := serializers[reqSer]
		if !ok {
			panic(fmt.Sprintf("uuid serializer for %s not found", reqSer))
		}

		pkg.AddFunction(fName, "", reqSer, ser)
	}
}

var allowedFieldTypes = map[string]bool{
	"string": true, "[]string": true,
	"bool": true, "[]bool": true,
	"byte": true, "[]byte": true,

	// int
	"int": true, "[]int": true,
	"int32": true, "[]int32": true,
	"int64": true, "[]int64": true,

	// ip
	"IPAddress": true,

	// unions
	"UnionNullString":    true,
	"UnionNullInt":       true,
	"UnionNullLong":      true,
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

	// IP related
	"IPAddress": "ipSerializer",

	// unions
	"UnionNullString":    "unionNullStringSerializer",
	"UnionNullInt":       "unionNullIntSerializer",
	"UnionNullLong":      "unionNullLongSerializer",
	"UnionNullBool":      "unionNullBoolSerializer",
	"UnionNullIPAddress": "unionNullIPAddressSerializer",
}

var serializers = map[string]string{
	"byte": byteSerializer, "[]byte": byteSliceSerializer,
	"bool": boolSerializer, "[]bool": boolSliceSerializer,
	"string": stringSerializer, "[]string": stringSliceSerializer,

	// int
	"int": intSerializer, "[]int": intSliceSerializer,
	"int32": int32Serializer, "[]int32": int32SliceSerializer,
	"int64": int64Serializer, "[]int64": int64SliceSerializer,

	// IP related
	"IPAddress": ipSerializer,

	// unions
	"UnionNullString":    unionNullStringSerializer,
	"UnionNullInt":       unionNullIntSerializer,
	"UnionNullLong":      unionNullLongSerializer,
	"UnionNullBool":      unionNullBoolSerializer,
	"UnionNullIPAddress": unionNullIPAddressSerializer,
}

// byte

var byteSerializer = `
	func byteSerializer(v byte) string {
		return fmt.Sprintf("%d", v)
	}
`

var byteSliceSerializer = `
	func byteSliceSerializer(vs []byte) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += ArraySeparator
			}
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
`

// string

var stringSerializer = `
	func stringSerializer(v string) string {
		return v
	}
`

var stringSliceSerializer = `
	func stringSliceSerializer(vs []string) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += ArraySeparator
			}
			out += v
		}
		return out
	}
`

// bool

var boolSerializer = `
	func boolSerializer(v bool) string {
		return fmt.Sprintf("%v", v)
	}
`

var boolSliceSerializer = `
	func boolSliceSerializer(vs []bool) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += ArraySeparator
			}
			out += fmt.Sprintf("%v", v)
		}
		return out
	}
`

// int, int32, int64

var intSerializer = `
	func intSerializer(v int) string {
		return fmt.Sprintf("%d", v)
	}
`

var int32Serializer = `
	func int32Serializer(v int32) string {
		return fmt.Sprintf("%d", v)
	}
`

var int64Serializer = `
	func int64Serializer(v int64) string {
		return fmt.Sprintf("%d", v)
	}
`

var intSliceSerializer = `
	func intSliceSerializer(vs []int) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += ArraySeparator
			}
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
`

var int32SliceSerializer = `
	func int32SliceSerializer(vs []int32) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += ArraySeparator
			}
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
`

var int64SliceSerializer = `
	func int64SliceSerializer(vs []int64) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += ArraySeparator
			}
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
`

// ip

var ipSerializer = `
	func ipSerializer(vs IPAddress) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += ArraySeparator
			}
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
`

// unions

var unionNullStringSerializer = `
	func unionNullStringSerializer(un UnionNullString) string {
		if un.UnionType == UnionNullStringTypeEnumString {
			return un.String
		}
		return ""
	}
`

var unionNullIntSerializer = `
	func unionNullIntSerializer(un UnionNullInt) string {
		if un.UnionType == UnionNullIntTypeEnumInt {
			return fmt.Sprintf("%d", un.Int)
		}
		return ""
	}
`

var unionNullLongSerializer = `
	func unionNullLongSerializer(un UnionNullLong) string {
		if un.UnionType == UnionNullLongTypeEnumLong {
			return fmt.Sprintf("%d", un.Long)
		}
		return ""
	}
`

var unionNullBoolSerializer = `
	func unionNullBoolSerializer(un UnionNullBool) string {
		if un.UnionType == UnionNullBoolTypeEnumBool {
			return fmt.Sprintf("%v", un.Bool)
		}
		return ""
	}
`

var unionNullIPAddressSerializer = `
	func unionNullIPAddressSerializer(un UnionNullIPAddress) string {
		if un.UnionType == UnionNullIPAddressTypeEnumIPAddress {
			out := ""
			for i, v := range un.IPAddress {
				if i != 0 {
					out += ArraySeparator
				}
				out += fmt.Sprintf("%d", v)
			}
			return out
		}
		return ""
	}
`
