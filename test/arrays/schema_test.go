package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/linkedin/goavro"
	"io/ioutil"
	"reflect"
	"testing"
)

/* Round-trip some primitive values through our serializer and goavro to verify */
const fixtureJson = `
[
{"IntField": [1, -2147483647, 2147483647], "LongField": [2, 9223372036854775807, -9223372036854775807], "FloatField": [3.4, 3.402823e-38, 3.402823e+38], "DoubleField": [ 5.6, 2.2250738585072014e-308], "StringField": ["short", "789", "longer", "a slightly longer string"], "BoolField": [true, false], "BytesField": []}
]
`

func TestArrayFixture(t *testing.T) {
	fixtures := make([]ArrayTestRecord, 0)
	err := json.Unmarshal([]byte(fixtureJson), &fixtures)
	if err != nil {
		t.Fatal(err)
	}

	schemaJson, err := ioutil.ReadFile("arrays.avsc")
	if err != nil {
		t.Fatal(err)
	}
	codec, err := goavro.NewCodec(string(schemaJson))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	for i, f := range fixtures {
		fmt.Printf("Serializing fixture %v\n", i)
		buf.Reset()
		err = f.Serialize(&buf)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("%v\n", buf.Bytes())
		datum, err := codec.Decode(&buf)
		if err != nil {
			t.Fatal(err)
		}
		record := datum.(*goavro.Record)
		value := reflect.ValueOf(f)
		for i := 0; i < value.NumField(); i++ {
			fieldName := value.Type().Field(i).Name
			avroVal, err := record.Get(fieldName)
			if err != nil {
				t.Fatal(err)
			}
			avroArray := avroVal.([]interface{})
			if len(avroArray) != value.Field(i).Len() {
				t.Fatalf("Got %v elements from goavro but expected %v", len(avroArray), value.Field(i).Len())
			}
			for j := 0; j < value.Field(i).Len(); j++ {
				avroArrayVal := avroArray[j]
				structArrayVal := value.Field(i).Index(j).Interface()
				if !reflect.DeepEqual(avroArrayVal, structArrayVal) {
					t.Fatalf("Field %v element %v not equal: %v != %v", fieldName, j, avroArrayVal, structArrayVal)
				}
			}
		}
	}
}

/*
func BenchmarkPrimitiveRecord(b *testing.B) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		record := PrimitiveTestRecord{1, 2, 3.4, 5.6, "789", true, []byte{1, 2, 3, 4}}
		record.Serialize(buf)
	}
}

func BenchmarkPrimitiveGoavro(b *testing.B) {
	schemaJson, err := ioutil.ReadFile("primitives.avsc")
	if err != nil {
		b.Fatal(err)
	}
	codec, err := goavro.NewCodec(string(schemaJson))
	if err != nil {
		b.Fatal(err)
	}
	someRecord, err := goavro.NewRecord(goavro.RecordSchema(string(schemaJson)))
	if err != nil {
		b.Fatal(err)
	}
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		someRecord.Set("IntField", 1)
		someRecord.Set("LongField", 2)
		someRecord.Set("FloatField", 3.4)
		someRecord.Set("DoubleField", 5.6)
		someRecord.Set("StringField", "789")
		someRecord.Set("BoolField", true)
		someRecord.Set("BytesField", []byte{1, 2, 3, 4})

		codec.Encode(buf, someRecord)
	}

}
*/