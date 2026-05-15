package resp

// Type identifies one RESP2 value kind.
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

// Value represents the small RESP2 subset Moxy needs for command handling.
type Value struct {
	Type    Type
	String  string
	Integer int64
	Array   []Value
	Null    bool
}

func SimpleString(value string) Value {
	return Value{Type: TypeSimpleString, String: value}
}

func Error(value string) Value {
	return Value{Type: TypeError, String: value}
}

func Integer(value int64) Value {
	return Value{Type: TypeInteger, Integer: value}
}

func BulkString(value string) Value {
	return Value{Type: TypeBulkString, String: value}
}

func NullBulkString() Value {
	return Value{Type: TypeBulkString, Null: true}
}

func Array(values ...Value) Value {
	return Value{Type: TypeArray, Array: values}
}
