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
		Double:    3.0,
		IPAddress: IPAddressZero,
		Object: &Object{
			Name: "Boop",
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
	if s := stringSliceSerializer([]string{"hello", "bye"}); s != "hellobye" {
		t.Fatalf("string slice serializer provided wrong result")
	}
}

func TestIntSerializer(t *testing.T) {
	if s := intSerializer(1); s != "1" {
		t.Fatalf("int serializer provided wrong result")
	}
	if s := intSerializer(-1); s != "-1" {
		t.Fatalf("int serializer provided wrong result")
	}
	if s := intSliceSerializer([]int{1, -1}); s != "1-1" {
		t.Fatalf("int slice serializer provided wrong result")
	}
}

func TestInt32Serializer(t *testing.T) {
	if s := int32Serializer(1); s != "1" {
		t.Fatalf("int32 serializer provided wrong result")
	}
	if s := int32Serializer(-1); s != "-1" {
		t.Fatalf("int32 serializer provided wrong result")
	}
	if s := int32SliceSerializer([]int32{1, -1}); s != "1-1" {
		t.Fatalf("int32 slice serializer provided wrong result")
	}
}

func TestInt64Serializer(t *testing.T) {
	if s := int64Serializer(1); s != "1" {
		t.Fatalf("int64 serializer provided wrong result")
	}
	if s := int64Serializer(-1); s != "-1" {
		t.Fatalf("int64 serializer provided wrong result")
	}
	if s := int64SliceSerializer([]int64{1, -1}); s != "1-1" {
		t.Fatalf("int64 slice serializer provided wrong result")
	}
}

func TestFloat32Serializer(t *testing.T) {
	if s := float32Serializer(1); s != "1.0000" {
		t.Fatalf("float32 serializer provided wrong result")
	}
	if s := float32Serializer(-1); s != "-1.0000" {
		t.Fatalf("float32 serializer provided wrong result")
	}
	if s := float32SliceSerializer([]float32{1, -1}); s != "1.0000-1.0000" {
		t.Fatalf("float32 slice serializer provided wrong result")
	}
}

func TestFloat64Serializer(t *testing.T) {
	if s := float64Serializer(1); s != "1.0000" {
		t.Fatalf("float64 serializer provided wrong result")
	}
	if s := float64Serializer(-1); s != "-1.0000" {
		t.Fatalf("float64 serializer provided wrong result")
	}
	if s := float64SliceSerializer([]float64{1, -1}); s != "1.0000-1.0000" {
		t.Fatalf("float64 slice serializer provided wrong result")
	}
}

func TestBooleanSerializer(t *testing.T) {
	if s := boolSerializer(false); s != "false" {
		t.Fatalf("bool serializer provided wrong result")
	}
	if s := boolSerializer(true); s != "true" {
		t.Fatalf("bool serializer provided wrong result")
	}
	if s := boolSliceSerializer([]bool{true, false}); s != "truefalse" {
		t.Fatalf("bool slice serializer provided wrong result")
	}
}

func TestIPSerializer(t *testing.T) {
	if s := ipSerializer(IPAddressZero); s != "0000000000000000" {
		t.Fatalf("ip serialize provided wrong result")
	}
	if s := ipSerializer(IPAddressV4Full); s != "0000000000255255255255255255" {
		t.Fatalf("ip serialize provided wrong result")
	}
}
