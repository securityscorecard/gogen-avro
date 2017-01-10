package types

import (
	"fmt"

	"github.com/alanctgardner/gogen-avro/generator"
	"github.com/serenize/snaker"
)

const recordStructDefTemplate = `type %v struct {
%v
}
`

const recordStructPublicSerializerTemplate = `
func (r %v) Serialize(w io.Writer) error {
	return %v(&r, w)
}
`

const recordStructDeserializerTemplate = `
func %v(r io.Reader) (*%v, error) {
	var str %v
	var err error
	%v
	return &str, nil
}
`

const recordStructPublicDeserializerTemplate = `
func %v(r io.Reader) (*%v, error) {
	return %v(r)
}
`

type RecordDefinition struct {
	name   string
	fields []Field
}

func (r *RecordDefinition) GoType() string {
	return generator.ToPublicName(r.name)
}

func (r *RecordDefinition) structFields() string {
	var fieldDefinitions string
	for _, f := range r.fields {
		fieldDefinitions += fmt.Sprintf("%v %v\n", f.Name(), f.GoType())
	}
	return fieldDefinitions
}

func (r *RecordDefinition) fieldSerializers() string {
	serializerMethods := "var err error\n"
	for _, f := range r.fields {
		serializerMethods += fmt.Sprintf("err = %v(r.%v, w)\nif err != nil {return err}\n", f.SerializerMethod(), f.Name())
	}
	return serializerMethods
}

func (r *RecordDefinition) fieldDeserializers() string {
	deserializerMethods := ""
	for _, f := range r.fields {
		deserializerMethods += fmt.Sprintf("str.%v, err = %v(r)\nif err != nil {return nil, err}\n", f.Name(), f.DeserializerMethod())
	}
	return deserializerMethods
}

func (r *RecordDefinition) structDefinition() string {
	return fmt.Sprintf(recordStructDefTemplate, r.GoType(), r.structFields())
}

func (r *RecordDefinition) serializerMethodDef() string {
	return fmt.Sprintf("func %v(r *%v, w io.Writer) error {\n%v\nreturn nil\n}", r.serializerMethod(), r.GoType(), r.fieldSerializers())
}

func (r *RecordDefinition) deserializerMethodDef() string {
	return fmt.Sprintf(recordStructDeserializerTemplate, r.deserializerMethod(), r.GoType(), r.GoType(), r.fieldDeserializers())
}

func (r *RecordDefinition) serializerMethod() string {
	return fmt.Sprintf("write%v", r.GoType())
}

func (r *RecordDefinition) deserializerMethod() string {
	return fmt.Sprintf("read%v", r.GoType())
}

func (r *RecordDefinition) publicDeserializerMethod() string {
	return fmt.Sprintf("Deserialize%v", r.GoType())
}

func (r *RecordDefinition) publicSerializerMethodDef() string {
	return fmt.Sprintf(recordStructPublicSerializerTemplate, r.GoType(), r.serializerMethod())
}

func (r *RecordDefinition) publicDeserializerMethodDef() string {
	return fmt.Sprintf(recordStructPublicDeserializerTemplate, r.publicDeserializerMethod(), r.GoType(), r.deserializerMethod())
}

func (r *RecordDefinition) filename() string {
	return generator.ToSnake(r.GoType()) + ".go"
}

func (r *RecordDefinition) AddStruct(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasStruct(r.filename(), r.GoType()) {
		p.AddStruct(r.filename(), r.GoType(), r.structDefinition())
		for _, f := range r.fields {
			f.AddStruct(p)
		}
	}
}

func (r *RecordDefinition) AddSerializer(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(r.filename(), "", r.serializerMethod()) {
		p.AddImport(r.filename(), "io")
		p.AddFunction(UTIL_FILE, "", r.serializerMethod(), r.serializerMethodDef())
		p.AddFunction(r.filename(), r.GoType(), "Serialize", r.publicSerializerMethodDef())
		for _, f := range r.fields {
			f.AddSerializer(p)
		}
	}
}

func (r *RecordDefinition) AddDeserializer(p *generator.Package) {
	// Import guard, to avoid circular dependencies
	if !p.HasFunction(r.filename(), "", r.deserializerMethod()) {
		p.AddImport(r.filename(), "io")
		p.AddFunction(UTIL_FILE, "", r.deserializerMethod(), r.deserializerMethodDef())
		p.AddFunction(r.filename(), "", r.publicDeserializerMethod(), r.publicDeserializerMethodDef())
		for _, f := range r.fields {
			f.AddDeserializer(p)
		}
	}
}

// AddGenerateID adds a GenerateID method which creates a uuidV5 from a set of fields
func (r *RecordDefinition) AddGenerateID(p *generator.Package, uuidKeys []string) {
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
		`, r.GoType(), r.uuidStrDef(uuidKeys))

		p.AddFunction(r.filename(), r.GoType(), "GenerateID", fnDef)
	}
}

// uuidStrDef generates the fmt.Sprintf compatible input for the AddGenerateID method
// e.g. for uuidKeys = []string{"A", "B"} => `"%v%v", A, B`
func (r *RecordDefinition) uuidStrDef(uuidKeys []string) string {
	// Create CamelCase uuidKeysSet
	uuidKeysSet := make(map[string]bool)
	for _, uuidKey := range uuidKeys {
		uuidKeysSet[snaker.SnakeToCamel(uuidKey)] = true
	}

	fieldsToInclude := []string{}
	for _, f := range r.fields {
		fName := snaker.SnakeToCamel(f.Name())
		if _, ok := uuidKeysSet[fName]; ok {
			fieldsToInclude = append(fieldsToInclude, fName)
		}
	}

	strDef := `"`
	for i := 0; i < len(fieldsToInclude); i++ {
		strDef += "%v"
	}
	strDef += `"`
	for _, fName := range fieldsToInclude {
		strDef += fmt.Sprintf(", r.%s", fName)
	}

	return strDef
}
