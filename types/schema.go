package types

import (
	"encoding/json"
	"fmt"
	"strings"

	uuid "github.com/satori/go.uuid"
)

const UTIL_FILE = "primitive.go"

/*
  An Avro qualified name, which includes an optional namespace and the type name.
*/
type QualifiedName struct {
	Namespace string
	Name      string
}

func (q QualifiedName) String() string {
	if q.Namespace == "" {
		return q.Name
	}
	return q.Namespace + "." + q.Name
}

type Schema struct {
	Root       Field
	JSONSchema []byte
}

/*
  Namespace is a mapping of QualifiedNames to their Definitions, used to resolve
  type lookups within a schema.
*/
type Namespace struct {
	Definitions map[QualifiedName]Definition
	Schemas     []Schema
}

func NewNamespace() *Namespace {
	return &Namespace{
		Definitions: make(map[QualifiedName]Definition),
		Schemas:     make([]Schema, 0),
	}
}

/*
  Add a new type definition to the namespace. Returns an error if the type is already defined.
*/
func (n *Namespace) RegisterDefinition(d Definition) error {
	if _, ok := n.Definitions[d.AvroName()]; ok {
		return fmt.Errorf("Conflicting definitions for %v", d.AvroName())
	}
	n.Definitions[d.AvroName()] = d

	for _, alias := range d.Aliases() {
		if _, ok := n.Definitions[alias]; ok {
			return fmt.Errorf("Conflicting alias for %v - %v", d.AvroName(), alias)
		}
		n.Definitions[alias] = d
	}
	return nil
}

/*
  Parse a name according to the Avro spec:
  - If the name contains a dot ('.'), the last part is the name and the rest is the namespace
  - Otherwise, the enclosing namespace is used
*/
func ParseAvroName(enclosing, name string) QualifiedName {
	lastIndex := strings.LastIndex(name, ".")
	if lastIndex != -1 {
		return QualifiedName{name[:lastIndex], name[lastIndex+1:]}
	}
	return QualifiedName{enclosing, name}
}

/*
  Given an Avro schema as a JSON string, decode it and return the Field defined at the top level:
    - a single record definition (JSON map)
    - a union of multiple types (JSON array)
    - an already-defined type (JSON string)

The Field defined at the top level and all the type definitions beneath it will also be added to this Namespace.
*/
func (n *Namespace) FieldDefinitionForSchema(schemaJson []byte) (Field, error) {
	var schema interface{}
	if err := json.Unmarshal(schemaJson, &schema); err != nil {
		return nil, err
	}

	field, err := n.decodeFieldDefinitionType("", "", schema, nil, false)
	if err != nil {
		return nil, err
	}

	n.Schemas = append(n.Schemas, Schema{field, schemaJson})
	return field, nil
}

func (n *Namespace) decodeFieldDefinitionType(namespace, nameStr string, t, def interface{}, hasDef bool) (Field, error) {
	switch t.(type) {
	case string:
		typeStr := t.(string)
		return n.createFieldStruct(namespace, nameStr, typeStr, def, hasDef)
	case []interface{}:
		return n.decodeUnionDefinition(namespace, nameStr, def, hasDef, t.([]interface{}))
	case map[string]interface{}:
		return n.decodeComplexDefinition(namespace, nameStr, t.(map[string]interface{}), def, hasDef)
	}
	return nil, NewSchemaError(nameStr, NewWrongMapValueTypeError("type", "array, string, map", t))
}

/*
   Given a map representing a record definition, validate the definition and build the RecordDefinition struct.
*/
func (n *Namespace) decodeRecordDefinition(namespace string, schemaMap map[string]interface{}) (Definition, error) {
	typeStr, err := getMapString(schemaMap, "type")
	if err != nil {
		return nil, err
	}

	if typeStr != "record" {
		return nil, fmt.Errorf("Type of record must be 'record'")
	}

	name, err := getMapString(schemaMap, "name")
	if err != nil {
		return nil, err
	}

	if _, ok := schemaMap["namespace"]; ok {
		namespace, err = getMapString(schemaMap, "namespace")
		if err != nil {
			return nil, err
		}
	}

	fieldList, err := getMapArray(schemaMap, "fields")
	if err != nil {
		return nil, err
	}

	decodedFields := make([]Field, 0)
	for _, f := range fieldList {
		field, ok := f.(map[string]interface{})
		if !ok {
			return nil, NewWrongMapValueTypeError("fields", "map[]", field)
		}
		fieldName, err := getMapString(field, "name")
		if err != nil {
			return nil, err
		}
		t, ok := field["type"]
		if !ok {
			return nil, NewRequiredMapKeyError("type")
		}
		def, hasDef := field["default"]
		fieldStruct, err := n.decodeFieldDefinitionType(namespace, fieldName, t, def, hasDef)
		if err != nil {
			return nil, err
		}

		decodedFields = append(decodedFields, fieldStruct)
	}

	// Version doesn't exist for every schema, frustratingly. So the zero
	// value, which we never use as a version, will indicate its absence.
	var version int
	if untypedVersion, ok := schemaMap["version"]; ok {
		if floatVersion, ok := untypedVersion.(float64); ok {
			version = int(floatVersion)
		}
	}

	aliases, err := parseAliases(schemaMap, namespace)
	if err != nil {
		return nil, err
	}

	return &RecordDefinition{
		name:     ParseAvroName(namespace, name),
		version:  version,
		aliases:  aliases,
		fields:   decodedFields,
		metadata: schemaMap,
	}, nil
}

/* Given a map representing an enum definition, validate the definition and build the EnumDefinition struct.
 */
func (n *Namespace) decodeEnumDefinition(namespace string, schemaMap map[string]interface{}) (Definition, error) {
	typeStr, err := getMapString(schemaMap, "type")
	if err != nil {
		return nil, err
	}

	if typeStr != "enum" {
		return nil, fmt.Errorf("Type of enum must be 'enum'")
	}

	if _, ok := schemaMap["namespace"]; ok {
		namespace, err = getMapString(schemaMap, "namespace")
		if err != nil {
			return nil, err
		}
	}

	name, err := getMapString(schemaMap, "name")
	if err != nil {
		return nil, err
	}

	symbolSlice, err := getMapArray(schemaMap, "symbols")
	if err != nil {
		return nil, err
	}

	symbolStr, ok := interfaceSliceToStringSlice(symbolSlice)
	if !ok {
		return nil, fmt.Errorf("'symbols' must be an array of strings")
	}

	aliases, err := parseAliases(schemaMap, namespace)
	if err != nil {
		return nil, err
	}

	return &EnumDefinition{
		name:     ParseAvroName(namespace, name),
		aliases:  aliases,
		symbols:  symbolStr,
		metadata: schemaMap,
	}, nil
}

/* Given a map representing a fixed definition, validate the definition and build the FixedDefinition struct. */
func (n *Namespace) decodeFixedDefinition(namespace string, schemaMap map[string]interface{}) (Definition, error) {
	typeStr, err := getMapString(schemaMap, "type")
	if err != nil {
		return nil, err
	}

	if typeStr != "fixed" {
		return nil, fmt.Errorf("Type of fixed must be 'fixed'")
	}

	if _, ok := schemaMap["namespace"]; ok {
		namespace, err = getMapString(schemaMap, "namespace")
		if err != nil {
			return nil, err
		}
	}

	name, err := getMapString(schemaMap, "name")
	if err != nil {
		return nil, err
	}

	sizeBytes, err := getMapFloat(schemaMap, "size")
	if err != nil {
		return nil, err
	}

	aliases, err := parseAliases(schemaMap, namespace)
	if err != nil {
		return nil, err
	}

	return &FixedDefinition{
		name:      ParseAvroName(namespace, name),
		aliases:   aliases,
		sizeBytes: int(sizeBytes),
		metadata:  schemaMap,
	}, nil
}

func (n *Namespace) decodeUnionDefinition(namespace, nameStr string, def interface{}, hasDef bool, FieldList []interface{}) (Field, error) {
	unionFields := make([]Field, 0)
	for i, f := range FieldList {
		var fieldDef Field
		var err error
		if i == 0 {
			fieldDef, err = n.decodeFieldDefinitionType(namespace, "", f, nil, false)
		} else {
			fieldDef, err = n.decodeFieldDefinitionType(namespace, "", f, nil, false)
		}
		if err != nil {
			return nil, err
		}
		unionFields = append(unionFields, fieldDef)
	}
	return &unionField{
		name:         nameStr,
		hasDefault:   hasDef,
		defaultValue: def,
		itemType:     unionFields,
	}, nil
}

func (n *Namespace) decodeComplexDefinition(namespace, nameStr string, typeMap map[string]interface{}, def interface{}, hasDef bool) (Field, error) {
	typeStr, err := getMapString(typeMap, "type")
	if err != nil {
		return nil, NewSchemaError(nameStr, err)
	}
	switch typeStr {
	case "string":
		var defStr string
		var ok bool
		if hasDef {
			defStr, ok = def.(string)
			if !ok {
				return nil, fmt.Errorf("Default value must be string type")
			}
		}
		return &stringField{
			name:         nameStr,
			defaultValue: defStr,
			hasDefault:   hasDef,
			schema:       typeMap,
		}, nil
	case "int":
		var defVal int
		var ok bool
		if hasDef {
			defVal, ok = def.(int)
			if !ok {
				return nil, fmt.Errorf("Default value must be int type")
			}
		}
		return &intField{
			name:         nameStr,
			defaultValue: int32(defVal),
			hasDefault:   hasDef,
			schema:       typeMap,
		}, nil
	case "long":
		var defVal int
		var ok bool
		if hasDef {
			defVal, ok = def.(int)
			if !ok {
				return nil, fmt.Errorf("Default value must be int type")
			}
		}
		return &longField{
			name:         nameStr,
			defaultValue: int64(defVal),
			hasDefault:   hasDef,
			schema:       typeMap,
		}, nil
	case "float":
		var defVal float32
		var ok bool
		if hasDef {
			defVal, ok = def.(float32)
			if !ok {
				return nil, fmt.Errorf("Default value must be float type")
			}
		}
		return &floatField{
			name:         nameStr,
			defaultValue: defVal,
			hasDefault:   hasDef,
			schema:       typeMap,
		}, nil
	case "double":
		var defVal float64
		var ok bool
		if hasDef {
			defVal, ok = def.(float64)
			if !ok {
				return nil, fmt.Errorf("Default value must be float type")
			}
		}
		return &doubleField{
			name:         nameStr,
			defaultValue: defVal,
			hasDefault:   hasDef,
			schema:       typeMap,
		}, nil
	case "array":
		items, ok := typeMap["items"]
		if !ok {
			return nil, NewSchemaError(nameStr, NewRequiredMapKeyError("items"))
		}
		fieldType, err := n.decodeFieldDefinitionType(namespace, "", items, nil, false)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		return &arrayField{
			name:         nameStr,
			itemType:     fieldType,
			hasDefault:   hasDef,
			defaultValue: def,
			metadata:     typeMap,
		}, nil
	case "map":
		values, ok := typeMap["values"]
		if !ok {
			return nil, NewSchemaError(nameStr, NewRequiredMapKeyError("values"))
		}
		fieldType, err := n.decodeFieldDefinitionType(namespace, "", values, nil, false)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		return &mapField{
			name:         nameStr,
			itemType:     fieldType,
			hasDefault:   hasDef,
			defaultValue: def,
			metadata:     typeMap,
		}, nil
	case "enum":
		definition, err := n.decodeEnumDefinition(namespace, typeMap)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		err = n.RegisterDefinition(definition)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		return &Reference{
			name:         nameStr,
			typeName:     definition.AvroName(),
			def:          nil,
			defaultValue: def,
			hasDefault:   hasDef,
		}, nil
	case "fixed":
		definition, err := n.decodeFixedDefinition(namespace, typeMap)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		err = n.RegisterDefinition(definition)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		return &Reference{
			name:         nameStr,
			typeName:     definition.AvroName(),
			def:          nil,
			defaultValue: def,
			hasDefault:   hasDef,
		}, nil
	case "record":
		definition, err := n.decodeRecordDefinition(namespace, typeMap)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		err = n.RegisterDefinition(definition)
		if err != nil {
			return nil, NewSchemaError(nameStr, err)
		}
		return &Reference{
			name:         nameStr,
			typeName:     definition.AvroName(),
			def:          nil,
			defaultValue: def,
			hasDefault:   hasDef,
		}, nil
	default:
		return nil, NewSchemaError(nameStr, fmt.Errorf("Unknown type name %v", typeStr))
	}
}

func (n *Namespace) createFieldStruct(namespace, nameStr, typeStr string, def interface{}, hasDef bool) (Field, error) {
	switch typeStr {
	case "string":
		var defStr string
		var ok bool
		if hasDef {
			defStr, ok = def.(string)
			if !ok {
				return nil, fmt.Errorf("Default value must be string type")
			}

		}
		return &stringField{
			name:         nameStr,
			defaultValue: defStr,
			hasDefault:   hasDef,
		}, nil
	case "int":
		var defInt int32
		if hasDef {
			defFloat, ok := def.(float64)
			if !ok {
				return nil, fmt.Errorf("Default must be float type")
			}
			defInt = int32(defFloat)

		}
		return &intField{
			name:         nameStr,
			defaultValue: defInt,
			hasDefault:   hasDef,
		}, nil
	case "long":
		var defInt int64
		if hasDef {
			defFloat, ok := def.(float64)
			if !ok {
				return nil, fmt.Errorf("Field %q default must be float type", nameStr)
			}
			defInt = int64(defFloat)
		}
		return &longField{
			name:         nameStr,
			defaultValue: defInt,
			hasDefault:   hasDef,
		}, nil
	case "float":
		var defFloat float64
		var ok bool
		if hasDef {
			defFloat, ok = def.(float64)
			if !ok {
				return nil, fmt.Errorf("Field %q default must be float type", nameStr)
			}
		}
		return &floatField{
			name:         nameStr,
			defaultValue: float32(defFloat),
			hasDefault:   hasDef,
		}, nil
	case "double":
		var defFloat float64
		var ok bool
		if hasDef {
			defFloat, ok = def.(float64)
			if !ok {
				return nil, fmt.Errorf("Field %q default must be float type", nameStr)
			}
		}
		return &doubleField{
			name:         nameStr,
			defaultValue: defFloat,
			hasDefault:   hasDef,
		}, nil
	case "boolean":
		var defBool bool
		var ok bool
		if hasDef {
			defBool, ok = def.(bool)
			if !ok {
				return nil, fmt.Errorf("Field %q default must be bool type", nameStr)
			}

		}
		return &boolField{
			name:         nameStr,
			defaultValue: defBool,
			hasDefault:   hasDef,
		}, nil
	case "bytes":
		var defBytes []byte
		if hasDef {
			defString, ok := def.(string)
			if !ok {
				return nil, fmt.Errorf("Field %q default must be string type", nameStr)
			}
			defBytes = []byte(defString)
		}
		return &bytesField{
			name:         nameStr,
			defaultValue: defBytes,
			hasDefault:   hasDef,
		}, nil
	case "null":
		return &nullField{nameStr, hasDef}, nil
	default:
		return &Reference{
			name:         nameStr,
			typeName:     ParseAvroName(namespace, typeStr),
			def:          nil,
			defaultValue: def,
			hasDefault:   hasDef,
		}, nil
	}
}

/*
  Parse out all the aliases from a definition map - returns an empty slice if no aliases exist.
  Returns an error if the aliases key exists but the value isn't a list of strings.
*/
func parseAliases(objectMap map[string]interface{}, namespace string) ([]QualifiedName, error) {
	aliases, ok := objectMap["aliases"]
	if !ok {
		return make([]QualifiedName, 0), nil
	}

	aliasList, ok := aliases.([]interface{})
	if !ok {
		return nil, fmt.Errorf("Field aliases expected to be array, got %v", aliases)
	}

	qualifiedAliases := make([]QualifiedName, 0, len(aliasList))

	for _, alias := range aliasList {
		aliasString, ok := alias.(string)
		if !ok {
			return nil, fmt.Errorf("Field aliases expected to be array of strings, got %v", aliases)
		}
		qualifiedAliases = append(qualifiedAliases, ParseAvroName(namespace, aliasString))
	}
	return qualifiedAliases, nil
}

// hash is just a little utility function to generate a 4-char hash
func hash() string {
	return uuid.NewV4().String()[:4]
}
