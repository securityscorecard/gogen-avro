package avro

import (
	"fmt"
	"testing"
)

func TestDoubleReferenceSchema(t *testing.T) {
	// If the generator fails to generate code due to the double reference
	// the following snippet would fail to run
	fmt.Println(&RecB{
		RecA1: &RecA{String: "hello"},
		RecA2: &RecA{String: "goodbye"},
	})
}
