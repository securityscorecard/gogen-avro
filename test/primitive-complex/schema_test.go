package avro

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/linkedin/goavro"
	"github.com/stretchr/testify/assert"
)

/* Round-trip some primitive values through our serializer and goavro to verify */
const fixtureJson = `
[
{"IntField": 1, "LongField": 2, "FloatField": 3.4, "DoubleField": 5.6, "StringField": "789"},
{"IntField": 2147483647, "LongField": 9223372036854775807, "FloatField": 3.402823e+38, "DoubleField": 1.7976931348623157e+308, "StringField": ""},
{"IntField": -2147483647, "LongField": -9223372036854775807, "FloatField": 3.402823e-38, "DoubleField": 2.2250738585072014e-308, "StringField": ""}
]
`

func compareFixtureGoAvro(t *testing.T, actual interface{}, expected interface{}) {
	record := actual.(*goavro.Record)
	value := reflect.ValueOf(expected)
	for i := 0; i < value.NumField(); i++ {
		fieldName := value.Type().Field(i).Name
		structVal := value.Field(i).Interface()
		avroVal, err := record.Get(fieldName)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(structVal, avroVal) {
			t.Fatalf("Field %v not equal: %v != %v", fieldName, structVal, avroVal)
		}
	}
}

func TestPrimitiveFixture(t *testing.T) {
	fixtures := make([]PrimitiveTestRecord, 0)
	err := json.Unmarshal([]byte(fixtureJson), &fixtures)
	if err != nil {
		t.Fatal(err)
	}

	schemaJson, err := ioutil.ReadFile("primitives.avsc")
	if err != nil {
		t.Fatal(err)
	}
	codec, err := goavro.NewCodec(string(schemaJson))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for _, f := range fixtures {
		buf.Reset()
		err = f.Serialize(&buf)
		if err != nil {
			t.Fatal(err)
		}
		datum, err := codec.Decode(&buf)
		if err != nil {
			t.Fatal(err)
		}
		compareFixtureGoAvro(t, datum, f)
	}
}

func TestRoundTrip(t *testing.T) {
	fixtures := make([]PrimitiveTestRecord, 0)
	err := json.Unmarshal([]byte(fixtureJson), &fixtures)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for _, f := range fixtures {
		buf.Reset()
		err = f.Serialize(&buf)
		if err != nil {
			t.Fatal(err)
		}
		datum, err := DeserializePrimitiveTestRecord(&buf)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, *datum, f)
	}
}

func TestSchema(t *testing.T) {
	expectedSchemaStr := []byte(`{"fields":[{"name":"IntField","type":{"type":"int"}},{"name":"LongField","type":{"type":"long"}},{"name":"FloatField","type":{"type":"float"}},{"name":"DoubleField","type":{"type":"double"}},{"name":"StringField","type":{"type":"string"}}],"name":"PrimitiveTestRecord","type":"record"}`)
	var expectedSchema interface{}
	if err := json.Unmarshal(expectedSchemaStr, &expectedSchema); err != nil {
		t.Fatalf("failed to unmarshal expected schema: %s", err)
	}

	tmp := PrimitiveTestRecord{}
	schemaStr := tmp.Schema()

	var schema interface{}
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		t.Fatalf("failed to unmarshal schema: %s", err)
	}

	if !reflect.DeepEqual(schema, expectedSchema) {
		t.Fatalf("expected schemas to match, but they dont. Expected %+v but got %+v", expectedSchema, schema)
	}
}
