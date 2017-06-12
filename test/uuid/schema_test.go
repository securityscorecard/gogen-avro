package avro

import "testing"

var (
	IPAddressZero   = IPAddress{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	IPAddressV4Full = IPAddress{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255}
)

func TestUUIDGenerationDeterministic(t *testing.T) {
	u := UUID{
		String:    "",
		Boolean:   true,
		Int:       1,
		Long:      2,
		Float:     4.0,
		Double:    3.0,
		IPAddress: IPAddressZero,
		Object: &Object{
			Name: "Boop",
		},

		// lists
		StringArray:  []string{""},
		BooleanArray: []bool{true},
		IntArray:     []int32{1, 2},
		LongArray:    []int64{3, 4},
		FloatArray:   []float32{4.5, 5.5},
		DoubleArray:  []float64{1.0, -1.0},

		// unions
		NullableString: UnionNullString{
			String:    "",
			UnionType: UnionNullStringTypeEnumString,
		},
		NullableBoolean: UnionNullBool{
			Bool:      true,
			UnionType: UnionNullBoolTypeEnumBool,
		},
		NullableInt: UnionNullInt{
			Int:       1,
			UnionType: UnionNullIntTypeEnumInt,
		},
		NullableLong: UnionNullLong{
			Long:      2,
			UnionType: UnionNullLongTypeEnumLong,
		},
		NullableFloat: UnionNullFloat{
			Float:     3.0,
			UnionType: UnionNullFloatTypeEnumFloat,
		},
		NullableDouble: UnionNullDouble{
			Double:    4.0,
			UnionType: UnionNullDoubleTypeEnumDouble,
		},
	}

	// Test that ID generation is deterministic and depends on field values
	idA := u.GenerateID()
	idB := u.GenerateID()

	if idA != idB {
		t.Fatalf("expected ids to be identical")
	}

	// Changing a field value should change the ID
	u.String = "string"
	idC := u.GenerateID()

	if idA == idC {
		t.Fatalf("expected ids to be different")
	}
}

func TestStringSerializer(t *testing.T) {
	if s := stringSerializer("hello"); s != "hello" {
		t.Fatalf("string serializer provided wrong result")
	}

	// list
	if s := stringSliceSerializer([]string{"hello", "bye"}); s != "hello,bye" {
		t.Fatalf("string slice serializer provided wrong result")
	}

	// union
	if s := unionNullStringSerializer(UnionNullString{
		String:    "hello",
		UnionType: UnionNullStringTypeEnumString,
	}); s != "hello" {
		t.Fatalf("nullable string serializer provided wrong result")
	}
	if s := unionNullStringSerializer(UnionNullString{
		UnionType: UnionNullStringTypeEnumNull,
	}); s != "" {
		t.Fatalf("nullable string serializer provided wrong result")
	}
}

func TestInt32Serializer(t *testing.T) {
	if s := int32Serializer(1); s != "1" {
		t.Fatalf("int32 serializer provided wrong result")
	}
	if s := int32Serializer(-1); s != "-1" {
		t.Fatalf("int32 serializer provided wrong result")
	}

	// list
	if s := int32SliceSerializer([]int32{1, -1}); s != "1,-1" {
		t.Fatalf("int32 slice serializer provided wrong result")
	}

	// union
	if s := unionNullIntSerializer(UnionNullInt{
		Int:       -1,
		UnionType: UnionNullIntTypeEnumInt,
	}); s != "-1" {
		t.Fatalf("nullable int32 serializer provided wrong result")
	}
	if s := unionNullIntSerializer(UnionNullInt{
		UnionType: UnionNullIntTypeEnumNull,
	}); s != "" {
		t.Fatalf("nullable int32 serializer provided wrong result")
	}
}

func TestInt64Serializer(t *testing.T) {
	if s := int64Serializer(1); s != "1" {
		t.Fatalf("int64 serializer provided wrong result")
	}
	if s := int64Serializer(-1); s != "-1" {
		t.Fatalf("int64 serializer provided wrong result")
	}

	// list
	if s := int64SliceSerializer([]int64{1, -1}); s != "1,-1" {
		t.Fatalf("int64 slice serializer provided wrong result")
	}

	// union
	if s := unionNullLongSerializer(UnionNullLong{
		Long:      -1,
		UnionType: UnionNullLongTypeEnumLong,
	}); s != "-1" {
		t.Fatalf("nullable int64 serializer provided wrong result")
	}
	if s := unionNullLongSerializer(UnionNullLong{
		UnionType: UnionNullLongTypeEnumNull,
	}); s != "" {
		t.Fatalf("nullable int64 serializer provided wrong result")
	}
}

func TestFloat32Serializer(t *testing.T) {
	if s := float32Serializer(1); s != "1.0000" {
		t.Fatalf("float32 serializer provided wrong result")
	}
	if s := float32Serializer(-1); s != "-1.0000" {
		t.Fatalf("float32 serializer provided wrong result")
	}

	// list
	if s := float32SliceSerializer([]float32{1, -1}); s != "1.0000,-1.0000" {
		t.Fatalf("float32 slice serializer provided wrong result")
	}

	// union
	if s := unionNullFloatSerializer(UnionNullFloat{
		Float:     1.0,
		UnionType: UnionNullFloatTypeEnumFloat,
	}); s != "1.0000" {
		t.Fatalf("nullable float32 serializer provided wrong result")
	}
	if s := unionNullFloatSerializer(UnionNullFloat{
		UnionType: UnionNullFloatTypeEnumNull,
	}); s != "" {
		t.Fatalf("nullable float32 serializer provided wrong result")
	}
}

func TestFloat64Serializer(t *testing.T) {
	if s := float64Serializer(1); s != "1.0000" {
		t.Fatalf("float64 serializer provided wrong result")
	}
	if s := float64Serializer(-1); s != "-1.0000" {
		t.Fatalf("float64 serializer provided wrong result")
	}

	// list
	if s := float64SliceSerializer([]float64{1, -1}); s != "1.0000,-1.0000" {
		t.Fatalf("float64 slice serializer provided wrong result")
	}

	// union
	if s := unionNullDoubleSerializer(UnionNullDouble{
		Double:    1.0,
		UnionType: UnionNullDoubleTypeEnumDouble,
	}); s != "1.0000" {
		t.Fatalf("nullable float64 serializer provided wrong result")
	}
	if s := unionNullDoubleSerializer(UnionNullDouble{
		UnionType: UnionNullDoubleTypeEnumNull,
	}); s != "" {
		t.Fatalf("nullable float64 serializer provided wrong result")
	}
}

func TestBooleanSerializer(t *testing.T) {
	if s := boolSerializer(false); s != "false" {
		t.Fatalf("bool serializer provided wrong result")
	}
	if s := boolSerializer(true); s != "true" {
		t.Fatalf("bool serializer provided wrong result")
	}

	// list
	if s := boolSliceSerializer([]bool{true, false}); s != "true,false" {
		t.Fatalf("bool slice serializer provided wrong result")
	}

	// union
	if s := unionNullBoolSerializer(UnionNullBool{
		Bool:      true,
		UnionType: UnionNullBoolTypeEnumBool,
	}); s != "true" {
		t.Fatalf("nullable boolean serializer provided wrong result")
	}
	if s := unionNullBoolSerializer(UnionNullBool{
		UnionType: UnionNullBoolTypeEnumNull,
	}); s != "" {
		t.Fatalf("nullable boolean serializer provided wrong result")
	}
}

func TestIPSerializer(t *testing.T) {
	if s := ipSerializer(IPAddressZero); s != "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0" {
		t.Fatalf("ip serialize provided wrong result")
	}
	if s := ipSerializer(IPAddressV4Full); s != "0,0,0,0,0,0,0,0,0,0,255,255,255,255,255,255" {
		t.Fatalf("ip serialize provided wrong result")
	}
}
