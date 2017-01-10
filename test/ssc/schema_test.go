package avro

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	schemaregistry "github.com/securityscorecard/go-schema-registry-client"
	stats "github.com/securityscorecard/go-stats"
)

func TestGenerateID(t *testing.T) {
	type testCase struct {
		in  TestRecord
		out string
	}

	testCases := []testCase{
		testCase{
			in:  TestRecord{Age: 0},
			out: "bf428e1d-f221-55de-a77f-a61755a4d727",
		},
		testCase{
			in:  TestRecord{Age: 1},
			out: "996ad860-2a9a-504f-8861-aeafd0b2ae29",
		},
		testCase{
			// Repeat the previous test case to make sure value repeats
			in:  TestRecord{Age: 1},
			out: "996ad860-2a9a-504f-8861-aeafd0b2ae29",
		},
	}

	for _, tc := range testCases {
		if id := tc.in.GenerateID(); id != tc.out {
			t.Fatalf("failed to generate correct id, expected %s but got %s", tc.out, id)
		}
	}
}

func TestMetric(t *testing.T) {
	called := false

	statser := &stats.MockStatser{
		CountFn: func(name string, count int64, tags ...stats.Tags) {
			if name != "test_record" {
				t.Fatal("wrong name", name)
			}

			if len(tags) != 1 {
				t.Fatal("expected one set of tags")
			}

			expectedTags := stats.Tags{"age": "10"}
			if !reflect.DeepEqual(tags[0], expectedTags) {
				t.Fatalf("wrong tags. expected %v but got %v", expectedTags, tags[0])
			}

			called = true
		},
	}

	rec := &TestRecord{Age: 10}
	rec.Metric(statser)

	// Wait a bit to make sure statser was called
	time.Sleep(100 * time.Millisecond)
	if called != true {
		t.Fatal("statser was not called")
	}
}

func TestCheckSchema(t *testing.T) {
	c := &schemaregistry.MockClient{
		GetSchemaBySubjectFn: func(subject string, ver int) (schemaregistry.Schema, error) {
			return schemaregistry.Schema{
				Subject: "test_record",
				Version: 1,
				Schema:  `{"type":"record","name":"test_record","subject":"test_record","version":1,"fields":[{"name":"id","type":"string"},{"name":"age","type":"int"}],"uuid_keys":["age"],"metric_tags":["age"]}`,
			}, nil
		},
	}

	buf := &bytes.Buffer{}
	w, err := NewTestRecordContainerWriter(buf, Snappy, 10)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.CheckSchema(c); err != nil {
		t.Fatalf("failed to check the schema successfully: %s", err)
	}
}
