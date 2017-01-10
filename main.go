package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/alanctgardner/gogen-avro/container"
	"github.com/alanctgardner/gogen-avro/generator"
	"github.com/alanctgardner/gogen-avro/types"
)

func main() {
	generateContainer := flag.Bool("container", false, "Whether to emit container file writer code")
	packageName := flag.String("package", "avro", "Name of generated package")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "Usage: gogen-avro [--container] [--package=<package name>] <target directory> <schema files>\n")
		os.Exit(1)
	}
	targetDir := flag.Arg(0)
	files := flag.Args()[1:]

	var err error
	pkg := generator.NewPackage(*packageName)

	if *generateContainer {
		err = addRecordDefinition([]byte(container.AVRO_BLOCK_SCHEMA), pkg, false, false)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating Avro container block schema - %v\n", err)
			os.Exit(2)
		}

		err = addRecordDefinition([]byte(container.AVRO_HEADER_SCHEMA), pkg, false, false)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating Avro container header schema - %v\n", err)
			os.Exit(2)
		}
	}

	for _, fileName := range files {
		schema, err := ioutil.ReadFile(fileName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading file %q - %v\n", fileName, err)
			os.Exit(2)
		}

		err = addRecordDefinition(schema, pkg, *generateContainer, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding schema for file %q - %v\n", fileName, err)
			os.Exit(3)
		}
	}

	// Add header comment to all generated files.
	for _, f := range pkg.Files() {
		pkg.AddHeader(f, codegenComment(files))
	}

	err = pkg.WriteFiles(targetDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing source files to directory %q - %v\n", targetDir, err)
		os.Exit(4)
	}
}

func addRecordDefinition(schema []byte, pkg *generator.Package, generateContainer bool, enhanceWithSSCStuff bool) error {
	recordDefinition, err := types.RecordDefinitionForSchema(schema)
	if err != nil {
		return err
	}
	recordDefinition.AddStruct(pkg)
	recordDefinition.AddSerializer(pkg)
	recordDefinition.AddDeserializer(pkg)

	if generateContainer {
		containerWriter := container.NewAvroContainerWriter(schema, recordDefinition)
		containerWriter.AddAvroContainerWriter(pkg)
	}

	if enhanceWithSSCStuff {
		// GenerateID
		type schemaUUIDKeys struct {
			UUIDKeys []string `json:"uuidKeys"`
		}

		var schemaObj schemaUUIDKeys
		if err := json.Unmarshal(schema, &schemaObj); err != nil {
			return err
		}

		if len(schemaObj.UUIDKeys) > 0 {
			recordDefinition.AddGenerateID(pkg, schemaObj.UUIDKeys)
		}

		// Metric
		type schemaMetricTags struct {
			MetricTags []string `json:"metricTags"`
		}

		var smt schemaMetricTags
		if err := json.Unmarshal(schema, &smt); err != nil {
			return err
		}

		if len(smt.MetricTags) > 0 {
			recordDefinition.AddMetric(pkg, smt.MetricTags)
		}
	}

	return nil
}

// codegenComment generates a comment informing readers they are looking at
// generated code and lists the source avro files used to generate the code
//
// invariant: sources > 0
func codegenComment(sources []string) string {
	const fileComment = `/*
 * CODE GENERATED AUTOMATICALLY WITH github.com/alanctgardner/gogen-avro
 * THIS FILE SHOULD NOT BE EDITED BY HAND
 *
 * %s
 */`
	var sourceBlock []string
	if len(sources) == 1 {
		sourceBlock = append(sourceBlock, "SOURCE:")
	} else {
		sourceBlock = append(sourceBlock, "SOURCES:")
	}

	for _, source := range sources {
		sourceBlock = append(sourceBlock, fmt.Sprintf(" *     %s", source))
	}

	return fmt.Sprintf(fileComment, strings.Join(sourceBlock, "\n"))
}
