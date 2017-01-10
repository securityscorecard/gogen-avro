package container

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/alanctgardner/gogen-avro/generator"
	"github.com/alanctgardner/gogen-avro/types"
)

const containerWriterCommonFile = "avro_container.go"

const codecDef = `
type Codec string

const (
	Null Codec = "null"
	Deflate Codec = "deflate"
	Snappy Codec = "snappy"
)
`

const closeableResettableWriterDef = `
type CloseableResettableWriter interface {
	Close() error
	Reset(io.Writer)
}
`

const containerWriterTemplate = `
type %v struct {
	writer io.Writer
	syncMarker [16]byte
	codec Codec
	recordsPerBlock int64

	blockBuffer *bytes.Buffer
	compressedWriter io.Writer
	nextBlockRecords int64
}
`

const snappyWriterDef = `
// A Writer that buffers until it's closed, then
// emits one Snappy-encoded block with the CRC suffix
// required by the Avro spec
type snappyWriter struct {
	writer io.Writer
	inputBuffer *bytes.Buffer
	outputBytes []byte
}

func newSnappyWriter(writer io.Writer) *snappyWriter {
	return &snappyWriter{
		writer: writer,
		inputBuffer: bytes.NewBuffer(make([]byte, 0)),
		outputBytes: make([]byte, 0),
	}
}

func (w *snappyWriter) Write(buf []byte) (int, error) {
	return w.inputBuffer.Write(buf)
}

func (w *snappyWriter) Close() error {
	w.outputBytes = snappy.Encode(w.outputBytes, w.inputBuffer.Bytes())
	_, err := w.writer.Write(w.outputBytes)
	if err != nil {
		return err
	}
	return binary.Write(w.writer, binary.BigEndian, crc32.ChecksumIEEE(w.inputBuffer.Bytes()))
}

func (w *snappyWriter) Reset(writer io.Writer) {
	w.outputBytes = w.outputBytes[:0]
	w.inputBuffer.Reset()
	w.writer = writer
}
`

const containerWriterConstructorTemplate = `
func %v(writer io.Writer, codec Codec, recordsPerBlock int64) (*%v, error) {
	blockBytes := make([]byte, 0)
	blockBuffer := bytes.NewBuffer(blockBytes)
	syncMarker := [16]byte{'g', 'o', 'g', 'e', 'n','a','v','r','o','m','a','g','i','c','1','0'}

	// Write the header when we construct the writer
	header := &AvroContainerHeader {
		Magic: [4]byte{'O', 'b', 'j', 1},
		Meta: map[string][]byte{
			"avro.schema": []byte(%v),
			"avro.codec": []byte(codec),
		},
		Sync: syncMarker,
	}

	err := header.Serialize(writer)
	if (err != nil) {
		return nil, err
	}

	avroWriter := &%v{
		writer: writer,
		syncMarker: syncMarker,
		codec: codec,
		recordsPerBlock: recordsPerBlock,
		blockBuffer: blockBuffer,
	}

	if codec == Deflate {
		avroWriter.compressedWriter, err = flate.NewWriter(avroWriter.blockBuffer, flate.DefaultCompression)
		if err != nil {
			return nil, err
		}
	} else if codec == Snappy {
		avroWriter.compressedWriter = newSnappyWriter(avroWriter.blockBuffer)
	} else if codec == Null {
		avroWriter.compressedWriter = avroWriter.blockBuffer
	}

	return avroWriter, nil
}
`

const containerWriterWriteTemplate = `
func (avroWriter *%v) WriteRecord(record %v) error {
	// Serialize the new record into the compressed writer
	err := record.Serialize(avroWriter.compressedWriter)
	if err != nil {
		return err
	}
	avroWriter.nextBlockRecords += 1

	// If the block if full, flush and reset the compressed writer,
	// write the header and the block contents
	if avroWriter.nextBlockRecords >= avroWriter.recordsPerBlock {
		return avroWriter.Flush()
	}

	return nil
}
`

const containerWriterFlushTemplate = `
func (avroWriter *%v) Flush() error {
	// Write out all of the buffered records as a new block
	// Must be called before closing to ensure the last block is written
	if fwWriter, ok := avroWriter.compressedWriter.(CloseableResettableWriter); ok {
		fwWriter.Close()
		fwWriter.Reset(avroWriter.blockBuffer)
	}

	block := &AvroContainerBlock {
		NumRecords: avroWriter.nextBlockRecords,
		RecordBytes: avroWriter.blockBuffer.Bytes(),
		Sync: avroWriter.syncMarker,
	}
	err := block.Serialize(avroWriter.writer)
	if err != nil {
		return err
	}

	avroWriter.blockBuffer.Reset()
	avroWriter.nextBlockRecords = 0

	return nil
}
`

type AvroContainerWriter struct {
	schema []byte
	record *types.RecordDefinition
}

func NewAvroContainerWriter(schema []byte, record *types.RecordDefinition) *AvroContainerWriter {
	// compact the schema
	buf := &bytes.Buffer{}
	json.Compact(buf, schema)

	return &AvroContainerWriter{
		schema: buf.Bytes(),
		record: record,
	}
}

func (a *AvroContainerWriter) filename() string {
	return generator.ToSnake(a.name()) + ".go"
}

func (a *AvroContainerWriter) name() string {
	return fmt.Sprintf("%vContainerWriter", a.record.GoType())
}

func (a *AvroContainerWriter) structDef() string {
	return fmt.Sprintf(containerWriterTemplate, a.name())
}

func (a *AvroContainerWriter) constructor() string {
	return fmt.Sprintf("New%v", a.name())
}

func (a *AvroContainerWriter) constructorDef() string {
	return fmt.Sprintf(containerWriterConstructorTemplate, a.constructor(), a.name(), a.schemaVariable(), a.name())
}

func (a *AvroContainerWriter) writeRecordDef() string {
	return fmt.Sprintf(containerWriterWriteTemplate, a.name(), a.record.GoType())
}

func (a *AvroContainerWriter) schemaVariable() string {
	return fmt.Sprintf("%vSchema", a.record.GoType())
}

func (a *AvroContainerWriter) flushDef() string {
	return fmt.Sprintf(containerWriterFlushTemplate, a.name())
}

func (a *AvroContainerWriter) AddAvroContainerWriter(p *generator.Package) {
	p.AddImport(a.filename(), "io")
	p.AddImport(a.filename(), "bytes")
	p.AddImport(a.filename(), "compress/flate")
	p.AddImport(containerWriterCommonFile, "io")
	p.AddImport(containerWriterCommonFile, "bytes")
	p.AddImport(containerWriterCommonFile, "encoding/binary")
	p.AddImport(containerWriterCommonFile, "hash/crc32")
	p.AddImport(containerWriterCommonFile, "github.com/golang/snappy")
	p.AddStruct(containerWriterCommonFile, "Codec", codecDef)
	p.AddStruct(containerWriterCommonFile, "snappyWriter", snappyWriterDef)
	p.AddStruct(containerWriterCommonFile, "CloseableResettableWriter", closeableResettableWriterDef)
	p.AddStruct(a.filename(), a.name(), a.structDef())
	p.AddConstant(a.filename(), a.schemaVariable(), string(a.schema))
	p.AddFunction(a.filename(), "", a.constructor(), a.constructorDef())
	p.AddFunction(a.filename(), a.name(), "WriteRecord", a.writeRecordDef())
	p.AddFunction(a.filename(), a.name(), "Flush", a.flushDef())
}

func (a *AvroContainerWriter) AddCheckSchema(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(a.filename(), "", "CheckSchema") {
		p.AddImport(a.filename(), "encoding/json")
		p.AddImport(a.filename(), "reflect")
		p.AddImport(a.filename(), "github.com/securityscorecard/go-schema-registry-client")

		fnDef := fmt.Sprintf(`
			func (avroWriter *%s) CheckSchema(c schemaregistry.Client) error {
				type Schema struct {
					Subject string `+"`json:\"subject\"`"+`
					Version int `+"`json:\"version\"`"+`
				}

				// Get the generated schema info
				var gsch Schema
				if err := json.Unmarshal([]byte(%sSchema), &gsch); err != nil {
					return fmt.Errorf("failed to unmarshal generated schema: %%s", err)
				}

				// Retrieve the schema from the registry
				ssch, err := c.GetSchemaBySubject(gsch.Subject, gsch.Version)
				if err != nil {
					return fmt.Errorf("failed to retrieve schema from registry: %%s", err)
				}

				// Compare the two schemas
				var a, b map[string]interface{}
				if err := json.Unmarshal([]byte(%sSchema), &a); err != nil {
					return fmt.Errorf("failed to unmarshal generated schema: %%s", err)
				}
				if err := json.Unmarshal([]byte(ssch.Schema), &b); err != nil {
					return fmt.Errorf("failed to unmarshal schema from registry: %%s", err)
				}

				if !reflect.DeepEqual(a, b) {
					return fmt.Errorf("warning: incompatible schemas")
				}

				return nil
			}
		`, a.name(), a.record.GoType(), a.record.GoType())

		p.AddFunction(a.filename(), a.name(), "CheckSchema", fnDef)
	}
}
