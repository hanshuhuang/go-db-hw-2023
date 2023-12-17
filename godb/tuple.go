package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

func (ft1 *FieldType) equals(ft2 *FieldType) bool {
	if ft1.Fname != ft2.Fname {
		return false
	}

	if ft1.TableQualifier != ft2.TableQualifier {
		return false
	}

	if ft1.Ftype != ft2.Ftype {
		return false
	}

	return true
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}

	for i := 0; i < len(d1.Fields); i++ {
		if !d1.Fields[i].equals(&d2.Fields[i]) {
			return false
		}
	}

	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}
}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	s := make([]FieldType, len(td.Fields))
	copy(s, td.Fields)
	return &TupleDesc{
		Fields: s,
	}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	cp := desc.copy()
	cp.Fields = append(cp.Fields, desc2.Fields...)
	return cp
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	for i, f := range t.Desc.Fields {
		switch f.Ftype {
		case IntType:
			tf, ok := t.Fields[i].(IntField)
			if !ok {
				return fmt.Errorf("unmatch int type")
			}
			res := tf.Value
			myBytes := make([]byte, 4) // Assuming a 4-byte integer, adjust as needed
			binary.LittleEndian.PutUint32(myBytes, uint32(res))
			n, err := b.Write(myBytes)
			if err != nil {
				return err
			}
			if n != 4 {
				return fmt.Errorf("buffer insufficient to write int")
			}
		case StringType:
			ts, ok := t.Fields[i].(StringField)
			if !ok {
				return fmt.Errorf("field con not match string")
			}
			res := ts.Value
			if len(res) > StringLength {
				return fmt.Errorf("too large string")
			}
			n, err := b.WriteString(res)
			if err != nil || n != len(res) {
				return fmt.Errorf("buffer too small")
			}
			for i := 0; i < StringLength-len(res); i++ {
				if err := b.WriteByte(0); err != nil {
					return err
				}
			}
		default:
			panic("unknown type")
		}
	}
	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	fields := make([]DBValue, len(desc.Fields))
	offset := 0
	input := b.Bytes()
	for i, f := range desc.Fields {
		switch f.Ftype {
		case IntType:
			if len(input) < offset+4 {
				return nil, fmt.Errorf("readTupleFrom has not enough buffer to int")
			}
			res := int(binary.LittleEndian.Uint32(input[offset : offset+4]))
			fields[i] = IntField{Value: int64(res)}
			offset += 4
		case StringType:
			if len(input) < offset+StringLength {
				return nil, fmt.Errorf("readTupleFrom has not enough buffer to string")
			}
			res := ""
			for ii := StringLength; ii >= 1; ii-- {
				if input[offset+ii-1] != 0 {
					res = string(input[offset : offset+ii])
					break
				}
			}
			fields[i] = StringField{Value: res}
			offset += StringLength
		default:
			panic("unknown type")
		}
	}

	t := &Tuple{
		Desc:   *desc,
		Fields: fields,
		Rid:    nil,
	}
	return t, nil
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	if !t1.Desc.equals(&t2.Desc) {
		return false
	}

	for i, f := range t1.Fields {
		if f != t2.Fields[i] {
			return false
		}
	}

	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here
	desc1 := TupleDesc{
		Fields: append(t1.Desc.Fields, t2.Desc.Fields...),
	}
	return &Tuple{
		Desc:   desc1,
		Fields: append(t1.Fields, t2.Fields...),
		Rid:    nil,
	}
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

func isSameType(x, y interface{}) bool {
	return reflect.TypeOf(x) == reflect.TypeOf(y)
}

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	// 获取t1的结果
	val1, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err
	}

	// 获取t2的结果
	val2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}

	if !isSameType(val1, val2) {
		return OrderedEqual, fmt.Errorf("compareField val1(%v) not equal val2(%v)", val1, val2)
	}

	switch val1.(type) {
	case StringField:
		v1 := val1.(StringField)
		v2 := val2.(StringField)
		if v1.Value == v2.Value {
			return OrderedEqual, nil
		} else if v1.Value > v2.Value {
			return OrderedGreaterThan, nil
		} else {
			return OrderedLessThan, nil
		}
	case int:
		v1 := val1.(IntField)
		v2 := val2.(IntField)
		if v1.Value == v2.Value {
			return OrderedEqual, nil
		} else if v1.Value > v2.Value {
			return OrderedGreaterThan, nil
		} else {
			return OrderedLessThan, nil
		}
	default:
		return OrderedEqual, fmt.Errorf("compareField unknown type")
	}
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here
	res := &Tuple{
		Desc: TupleDesc{
			Fields: fields,
		},
		Fields: []DBValue{},
		Rid:    nil,
	}

	// 找到对应的fields
	for _, f := range fields {
		i, err := findFieldInTd(f, &t.Desc)
		if err != nil {
			return nil, err
		}

		res.Fields = append(res.Fields, t.Fields[i])
	}

	return res, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
