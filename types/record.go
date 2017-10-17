package types

import (
	"encoding/json"
	"fmt"
	"os"
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
	name := r.name.Name

	// If name already seen
	if v, ok := names[r.name]; ok {
		// v is times name was seen
		ts := v.(int)

		// Add a suffix to the name to avoid name collisions
		name += "_" + fmt.Sprintf("%d", ts)

		// Update the times seen count
		names[r.name] = ts + 1
	}

	// mark name as seen
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
		"name":   name, // Name field should be unqualified (not including namespace)
		"fields": fields,
	}, r.metadata)
}

// AddGenerateID adds a GenerateID method which creates a uuidV5 from a set of fields
func (r *RecordDefinition) AddGenerateID(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(r.filename(), "", "GenerateID") {
		p.AddImport(r.filename(), "fmt")
		p.AddImport(r.filename(), "github.com/satori/go.uuid")

		uuidStrDef, requiredSerializers := r.uuidStrDef()

		// Create function definition
		fnDef := fmt.Sprintf(`
			func (r %v) GenerateID() string {
				s := fmt.Sprint(%s)
				return uuid.NewV5(uuid.NamespaceOID, s).String()
			}
		`, r.GoType(), uuidStrDef)

		p.AddFunction(r.filename(), r.GoType(), "GenerateID", fnDef)

		// Add serializers to the output package as required
		AddUUIDSerializerToPackage(p, requiredSerializers)
	}
}

func extractAvailableFields(f Field) map[string]string {
	availableFields := map[string]string{}

	// primitive case
	if _, ok := allowedFieldTypes[f.GoType()]; ok {
		availableFields[f.GoName()] = f.GoType()
		return availableFields
	}

	// union case
	un, ok := f.(*unionField)
	if ok {
		// The second type must be an allowed type
		typ := un.itemType[1].GoType()
		if _, ok := allowedFieldTypes[typ]; ok {
			availableFields[f.GoName()] = f.GoType()
			return availableFields
		}
	}

	// reference type
	ref, ok := f.(*Reference)
	if ok {
		// fixed type
		// pass

		// record type
		rec, ok := ref.def.(*RecordDefinition)
		if ok {
			for _, f := range rec.fields {
				moreAvailableFields := extractAvailableFields(f)

				// update availableFields with moreAvailableFields
				for fn, ft := range moreAvailableFields {
					fullFieldName := fmt.Sprintf("%s.%s", rec.FieldType(), fn)
					availableFields[fullFieldName] = ft
				}
			}
		}
	}

	return availableFields
}

// uuidStrDef generates the fmt.Sprintf compatible input for the AddGenerateID method
// e.g. for uuidKeys = []string{"A", "B"} => `"%v%v", A, B`
// It also returns a list of required uuid serializers
func (r *RecordDefinition) uuidStrDef() (string, []string) {
	type Schema struct {
		UUIDKeys []string `json:"uuid_keys"`
	}

	var schema Schema
	if err := mapstruct.Decode(r.metadata, &schema); err != nil {
		fmt.Printf("failed to decode metadata: %s\n", err)
		return "", nil
	}

	// Extract fields from the schema which can be used for uuid generation
	availableFields := map[string]string{}
	for _, f := range r.fields {
		moreAvailableFields := extractAvailableFields(f)
		for fn, ft := range moreAvailableFields {
			availableFields[fn] = ft
		}
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

	type uuidField struct {
		Name string
		Type string
	}

	// decide on which keys are to be used for uuid generation
	fieldsToInclude := []uuidField{}
	for _, uuidKey := range schema.UUIDKeys {
		fName := uuidToFieldName(uuidKey)
		if _, ok := availableFields[fName]; !ok {
			fmt.Printf("Error: can't use %s as a uuid key\n", uuidKey)
			keys := []string{}
			for key := range availableFields {
				keys = append(keys, strings.ToLower(key))
			}
			fmt.Printf("Error: valid UUID keys are %s\n", strings.Join(keys, ", "))
			os.Exit(1)
		}

		fieldsToInclude = append(fieldsToInclude, uuidField{Name: fName, Type: availableFields[fName]})
	}

	// track the serializers that we need
	requiredSerializers := map[string]bool{}
	for _, fti := range fieldsToInclude {
		requiredSerializers[fti.Type] = true
	}
	requiredSerializersList := []string{}
	for k := range requiredSerializers {
		requiredSerializersList = append(requiredSerializersList, k)
	}

	serializedFields := []string{}
	for _, fti := range fieldsToInclude {
		serializerFn, ok := typeSerializerFuncs[fti.Type]
		if !ok {
			fmt.Printf("Error: no serializer available for %s for use as a uuid key\n", fti.Name)
			os.Exit(1)
		}

		serializedFields = append(serializedFields, fmt.Sprintf("%s(r.%s)", serializerFn, fti.Name))
	}
	strDef := strings.Join(serializedFields, " + FieldSeparator + ")

	return strDef, requiredSerializersList
}

// AddSendStats add a SendStats method which submits stats for this record
func (r *RecordDefinition) AddSendStats(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(r.filename(), "", "SendStats") {
		metricTagsStrDef := r.metricTagsDef()

		// Only add "fmt" if necessary (only when there are tags)
		if metricTagsStrDef != "" {
			p.AddImport(r.filename(), "fmt")
		}
		p.AddImport(r.filename(), "github.com/securityscorecard/go-stats")

		// Create function definition
		fnDef := fmt.Sprintf(`
			func (r %v) SendStats(statser stats.Statser) {
				statser.Count("%s", 1, stats.Tags{
					%s
				})
			}
		`, r.GoType(), r.name, metricTagsStrDef)

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
