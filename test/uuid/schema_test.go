package avro

import (
	"strings"
	"testing"
)

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
		IPAddress: IPAddressZero,
		Object: &Object{
			Name: "Boop",
		},

		// lists
		StringArray:  []string{""},
		BooleanArray: []bool{true},
		IntArray:     []int32{1, 2},
		LongArray:    []int64{3, 4},

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
	expectedResult := "hello" + ArraySeparator + "bye"
	if s := stringSliceSerializer([]string{"hello", "bye"}); s != expectedResult {
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
	expectedResult := "1" + ArraySeparator + "-1"
	if s := int32SliceSerializer([]int32{1, -1}); s != expectedResult {
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
	expectedResult := "1" + ArraySeparator + "-1"
	if s := int64SliceSerializer([]int64{1, -1}); s != expectedResult {
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

func TestBooleanSerializer(t *testing.T) {
	if s := boolSerializer(false); s != "false" {
		t.Fatalf("bool serializer provided wrong result")
	}
	if s := boolSerializer(true); s != "true" {
		t.Fatalf("bool serializer provided wrong result")
	}

	// list
	expectedResult := "true" + ArraySeparator + "false"
	if s := boolSliceSerializer([]bool{true, false}); s != expectedResult {
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
	// case IPAddressZero
	expectedResult := strings.Join(
		[]string{"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"},
		ArraySeparator,
	)
	if s := ipSerializer(IPAddressZero); s != expectedResult {
		t.Fatalf("ip serialize provided wrong result")
	}

	// case IPAddressV4Full
	expectedResult = strings.Join(
		[]string{"0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "255", "255", "255", "255", "255", "255"},
		ArraySeparator,
	)
	if s := ipSerializer(IPAddressV4Full); s != expectedResult {
		t.Fatalf("ip serialize provided wrong result")
	}
}
