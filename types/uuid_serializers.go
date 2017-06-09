package types

import (
	"fmt"

	"github.com/alanctgardner/gogen-avro/generator"
)

// AddUUIDSerializerToPackage will add a file with Serializer functions
// to the generated package code
func AddUUIDSerializerToPackage(pkg *generator.Package, requiredSerializers []string) {
	fileContent := uuidSerializersFileContent

	for _, reqSer := range requiredSerializers {
		ser, ok := serializers[reqSer]
		if !ok {
			panic(fmt.Sprintf("uuid serializer for %s not found", reqSer))
		}

		fileContent += fmt.Sprintf("\n%s\n", ser)
	}

	pkg.AddFile("uuid_serializers.go", fileContent)
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

var serializers = map[string]string{
	"byte": byteSerializer, "[]byte": byteSliceSerializer,
	"bool": boolSerializer, "[]bool": boolSliceSerializer,
	"string": stringSerializer, "[]string": stringSliceSerializer,

	// int
	"int": intSerializer, "[]int": intSliceSerializer,
	"int32": int32Serializer, "[]int32": int32SliceSerializer,
	"int64": int64Serializer, "[]int64": int64SliceSerializer,

	// float
	"float32": float32Serializer, "[]float32": float32SliceSerializer,
	"float64": float64Serializer, "[]float64": float64SliceSerializer,

	// IP related
	"IPAddress": ipSerializer,

	// unions
	"UnionNullString":    unionNullStringSerializer,
	"UnionNullInt":       unionNullIntSerializer,
	"UnionNullLong":      unionNullLongSerializer,
	"UnionNullFloat":     unionNullFloatSerializer,
	"UnionNullDouble":    unionNullDoubleSerializer,
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
				out += "|"
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
				out += "|"
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
				out += "|"
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
				out += "|"
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
				out += "|"
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
				out += "|"
			}
			out += fmt.Sprintf("%d", v)
		}
		return out
	}
`

// float32, float64

var float32Serializer = `
	func float32Serializer(v float32) string {
		return fmt.Sprintf("%.4f", v)
	}
`

var float64Serializer = `
	func float64Serializer(v float64) string {
		return fmt.Sprintf("%.4f", v)
	}
`

var float32SliceSerializer = `
	func float32SliceSerializer(vs []float32) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += "|"
			}
			out += fmt.Sprintf("%.4f", v)
		}
		return out
	}
`

var float64SliceSerializer = `
	func float64SliceSerializer(vs []float64) string {
		out := ""
		for i, v := range vs {
			if i != 0 {
				out += "|"
			}
			out += fmt.Sprintf("%.4f", v)
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
				out += "|"
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

var unionNullFloatSerializer = `
	func unionNullFloatSerializer(un UnionNullFloat) string {
		if un.UnionType == UnionNullFloatTypeEnumFloat {
			return fmt.Sprintf("%.4f", un.Float)
		}
		return ""
	}
`

var unionNullDoubleSerializer = `
	func unionNullDoubleSerializer(un UnionNullDouble) string {
		if un.UnionType == UnionNullDoubleTypeEnumDouble {
			return fmt.Sprintf("%.4f", un.Double)
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
					out += "|"
				}
				out += fmt.Sprintf("%d", v)
			}
			return out
		}
		return ""
	}
`

var uuidSerializersFileContent = `
import (
	"fmt"
)
`
