package types

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/alanctgardner/gogen-avro/generator"
	mapstruct "github.com/rikonor/go-mapstruct"
	"github.com/serenize/snaker"
)

const recordStructDefTemplate = `type %v struct {
%v
}
`

const recordSchemaTemplate = `func (r %v) Schema() string {
 return %v
}
`

const recordStructPublicSerializerTemplate = `
func (r %v) Serialize(w io.Writer) error {
	return %v(r, w)
}
`

const recordStructDeserializerTemplate = `
func %v(r io.Reader) (%v, error) {
	var str = &%v{}
	var err error
	%v
	return str, nil
}
`

const recordStructPublicDeserializerTemplate = `
func %v(r io.Reader) (%v, error) {
	return %v(r)
}
`

type RecordDefinition struct {
	name     QualifiedName
	aliases  []QualifiedName
	fields   []Field
	metadata map[string]interface{}
}

func (r *RecordDefinition) AvroName() QualifiedName {
	return r.name
}

func (r *RecordDefinition) Aliases() []QualifiedName {
	return r.aliases
}

func (r *RecordDefinition) GoType() string {
	return fmt.Sprintf("*%v", r.FieldType())
}

func (r *RecordDefinition) FieldType() string {
	return generator.ToPublicName(r.name.Name)
}

func (r *RecordDefinition) structFields() string {
	var fieldDefinitions string
	for _, f := range r.fields {
		fieldDefinitions += fmt.Sprintf("%v %v\n", f.GoName(), f.GoType())
	}
	return fieldDefinitions
}

func (r *RecordDefinition) fieldSerializers() string {
	serializerMethods := "var err error\n"
	for _, f := range r.fields {
		serializerMethods += fmt.Sprintf("err = %v(r.%v, w)\nif err != nil {return err}\n", f.SerializerMethod(), f.GoName())
	}
	return serializerMethods
}

func (r *RecordDefinition) fieldDeserializers() string {
	deserializerMethods := ""
	for _, f := range r.fields {
		deserializerMethods += fmt.Sprintf("str.%v, err = %v(r)\nif err != nil {return nil, err}\n", f.GoName(), f.DeserializerMethod())
	}
	return deserializerMethods
}

func (r *RecordDefinition) structDefinition() string {
	return fmt.Sprintf(recordStructDefTemplate, r.FieldType(), r.structFields())
}

func (r *RecordDefinition) serializerMethodDef() string {
	return fmt.Sprintf("func %v(r %v, w io.Writer) error {\n%v\nreturn nil\n}", r.SerializerMethod(), r.GoType(), r.fieldSerializers())
}

func (r *RecordDefinition) deserializerMethodDef() string {
	return fmt.Sprintf(recordStructDeserializerTemplate, r.DeserializerMethod(), r.GoType(), r.FieldType(), r.fieldDeserializers())
}

func (r *RecordDefinition) SerializerMethod() string {
	return fmt.Sprintf("write%v", r.FieldType())
}

func (r *RecordDefinition) DeserializerMethod() string {
	return fmt.Sprintf("read%v", r.FieldType())
}

func (r *RecordDefinition) publicDeserializerMethod() string {
	return fmt.Sprintf("Deserialize%v", r.FieldType())
}

func (r *RecordDefinition) publicSerializerMethodDef() string {
	return fmt.Sprintf(recordStructPublicSerializerTemplate, r.GoType(), r.SerializerMethod())
}

func (r *RecordDefinition) publicDeserializerMethodDef() string {
	return fmt.Sprintf(recordStructPublicDeserializerTemplate, r.publicDeserializerMethod(), r.GoType(), r.DeserializerMethod())
}

func (r *RecordDefinition) filename() string {
	return generator.ToSnake(r.FieldType()) + ".go"
}

func (r *RecordDefinition) schemaMethod() string {
	schemaJson, _ := json.Marshal(r.Schema(make(map[QualifiedName]interface{})))
	return fmt.Sprintf(recordSchemaTemplate, r.GoType(), strconv.Quote(string(schemaJson)))
}

func (r *RecordDefinition) AddStruct(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasStruct(r.filename(), r.GoType()) {
		p.AddStruct(r.filename(), r.GoType(), r.structDefinition())
		for _, f := range r.fields {
			f.AddStruct(p)
		}
		p.AddFunction(r.filename(), r.GoType(), "Schema", r.schemaMethod())

		// For Records we also want to add a GenerateID and SendStats methods
		r.AddGenerateID(p)
		r.AddSendStats(p)
	}
}

func (r *RecordDefinition) AddSerializer(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(UTIL_FILE, "", r.SerializerMethod()) {
		p.AddImport(r.filename(), "io")
		p.AddFunction(UTIL_FILE, "", r.SerializerMethod(), r.serializerMethodDef())
		p.AddFunction(r.filename(), r.GoType(), "Serialize", r.publicSerializerMethodDef())
		for _, f := range r.fields {
			f.AddSerializer(p)
		}
	}
}

func (r *RecordDefinition) AddDeserializer(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(UTIL_FILE, "", r.DeserializerMethod()) {
		p.AddImport(r.filename(), "io")
		p.AddFunction(UTIL_FILE, "", r.DeserializerMethod(), r.deserializerMethodDef())
		p.AddFunction(r.filename(), "", r.publicDeserializerMethod(), r.publicDeserializerMethodDef())
		for _, f := range r.fields {
			f.AddDeserializer(p)
		}
	}
}

func (r *RecordDefinition) ResolveReferences(n *Namespace) error {
	var err error
	for _, f := range r.fields {
		err = f.ResolveReferences(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RecordDefinition) Schema(names map[QualifiedName]interface{}) interface{} {
	name := r.name.String()
	if _, ok := names[r.name]; ok {
		return name
	}
	names[r.name] = 1
	fields := make([]interface{}, 0, len(r.fields))
	for _, f := range r.fields {
		fieldDef := map[string]interface{}{
			"name": f.AvroName(),
			"type": f.Schema(names),
		}
		if f.HasDefault() {
			fieldDef["default"] = f.Default()
		}
		fields = append(fields, fieldDef)
	}
	return mergeMaps(map[string]interface{}{
		"type":   "record",
		"name":   name,
		"fields": fields,
	}, r.metadata)
}

// AddGenerateID adds a GenerateID method which creates a uuidV5 from a set of fields
func (r *RecordDefinition) AddGenerateID(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(r.filename(), "", "GenerateID") {
		p.AddImport(r.filename(), "fmt")
		p.AddImport(r.filename(), "github.com/satori/go.uuid")

		// Create function definition
		fnDef := fmt.Sprintf(`
			func (r %v) GenerateID() string {
				s := fmt.Sprintf(%s)
				return uuid.NewV5(uuid.NamespaceOID, s).String()
			}
		`, r.GoType(), r.uuidStrDef())

		p.AddFunction(r.filename(), r.GoType(), "GenerateID", fnDef)
	}
}

// uuidStrDef generates the fmt.Sprintf compatible input for the AddGenerateID method
// e.g. for uuidKeys = []string{"A", "B"} => `"%v%v", A, B`
func (r *RecordDefinition) uuidStrDef() string {
	type Schema struct {
		UUIDKeys []string `json:"uuid_keys"`
	}

	var schema Schema
	if err := mapstruct.Decode(r.metadata, &schema); err != nil {
		fmt.Printf("failed to decode metadata: %s\n", err)
		return ""
	}

	type uuidField struct {
		Name string
		Type string
	}

	allowedFieldTypes := map[string]bool{
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
	}

	availFields := map[string]string{}
	for _, f := range r.fields {
		// primitive case
		if _, ok := allowedFieldTypes[f.GoType()]; ok {
			availFields[f.GoName()] = f.GoType()
		}

		// TODO: handle record type so we can get nested fields
		// record type
		// rec, ok := f.(*RecordDefinition)
		// if ok {
		// 	for _, ff := range rec.fields {
		// 		if _, ok := allowedFieldTypes[f.GoType()]; ok {
		// 			availFields = append(availFields, uuidField{
		// 				Name: ff.GoName(),
		// 				Type: ff.GoType(),
		// 			})
		// 		}
		// 	}
		// }
	}

	// uuidToFieldName is an auxiliary function to convert a uuid_key name to
	// an appropriate Go CamelCased name.
	uuidToFieldName := func(uuidKey string) string {
		ps := []string{}
		for _, p := range strings.Split(uuidKey, ".") {
			ps = append(ps, snaker.SnakeToCamel(p))
		}
		return strings.Join(ps, ".")
	}

	fieldsToInclude := []uuidField{}
	for _, uuidKey := range schema.UUIDKeys {
		fName := uuidToFieldName(uuidKey)
		fieldsToInclude = append(fieldsToInclude, uuidField{Name: fName, Type: availFields[fName]})
	}

	strDef := `"`
	for i := 0; i < len(fieldsToInclude); i++ {
		strDef += "%s"
	}
	strDef += `"`
	for _, fti := range fieldsToInclude {
		strDef += fmt.Sprintf(", %s(r.%s)", typeSerializerFuncs[fti.Type], fti.Name)
	}

	return strDef
}

// AddSendStats add a SendStats method which submits stats for this record
func (r *RecordDefinition) AddSendStats(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(r.filename(), "", "SendStats") {
		p.AddImport(r.filename(), "fmt")
		p.AddImport(r.filename(), "github.com/securityscorecard/go-stats")

		// Create function definition
		fnDef := fmt.Sprintf(`
			func (r %v) SendStats(statser stats.Statser) {
				statser.Count("%s", 1, stats.Tags{
					%s
				})
			}
		`, r.GoType(), r.name, r.metricTagsDef())

		p.AddFunction(r.filename(), r.GoType(), "SendStats", fnDef)
	}
}

func (r *RecordDefinition) metricTagsDef() string {
	type Schema struct {
		MetricTags []string `json:"metric_tags"`
	}

	var schema Schema
	if err := mapstruct.Decode(r.metadata, &schema); err != nil {
		fmt.Printf("failed to decode metadata: %s\n", err)
		return ""
	}

	strDefParts := []string{}
	for _, tag := range schema.MetricTags {
		part := fmt.Sprintf(
			`"%s": fmt.Sprint(r.%s),`,
			tag, snaker.SnakeToCamel(tag),
		)
		strDefParts = append(strDefParts, part)
	}
	return strings.Join(strDefParts, "\n")
}
