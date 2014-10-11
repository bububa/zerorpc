package zerorpc

import (
	//"encoding/json"
	"errors"
	"github.com/bububa/go/codec"
	uuid "github.com/bububa/gouuid"
	"reflect"
	"time"
)

// ZeroRPC protocol version
const ProtocolVersion = 3

var (
	timeTyp = reflect.TypeOf(time.Time{})

	timeEncExt = func(rv reflect.Value) ([]byte, error) {
		return []byte(rv.Interface().(time.Time).Format(time.RFC3339)), nil
	}

	timeDecExt = func(rv reflect.Value, bs []byte) error {
		tt, err := time.Parse(string(bs), time.RFC3339)
		if err == nil {
			rv.Set(reflect.ValueOf(tt))
		}
		return err
	}

	/*uint64Typ = reflect.TypeOf([]uint64{})

	uint64EncExt = func(rv reflect.Value) ([]byte, error) {
		return json.Marshal(rv.Interface().([]uint64))
	}

	uint64DecExt = func(rv reflect.Value, bs []byte) error {
		var arr []uint64
		err := json.Unmarshal(bs, &arr)
		rv.Set(reflect.ValueOf(arr))
		return err
	}*/
)

// Event representation
type Event struct {
	Header map[string]interface{}
	Name   string
	Args   []interface{}
}

// Returns a pointer to a new event,
// a UUID V4 message_id is generated
func newEvent(name string, args ...interface{}) (*Event, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	header := make(map[string]interface{})
	header["message_id"] = id.String()
	header["v"] = ProtocolVersion

	e := Event{
		Header: header,
		Name:   name,
		Args:   args,
	}

	return &e, nil
}

// Packs an event into MsgPack bytes
func (e *Event) packBytes() ([]byte, error) {
	data := make([]interface{}, 2)
	data[0] = e.Header
	data[1] = e.Name

	for _, a := range e.Args {
		data = append(data, a)
	}

	var (
		buf []byte
		mh  codec.MsgpackHandle
	)

	mh.AddExt(timeTyp, 1, timeEncExt, timeDecExt)
	//mh.AddExt(uint64Typ, 2, uint64EncExt, uint64DecExt)

	enc := codec.NewEncoderBytes(&buf, &mh)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}

	return buf, nil
}

// Unpacks an event fom MsgPack bytes
func unPackBytes(b []byte) (*Event, error) {
	var mh codec.MsgpackHandle
	mh.AddExt(timeTyp, 1, timeEncExt, timeDecExt)
	//mh.AddExt(uint64Typ, 2, uint64EncExt, uint64DecExt)
	//mh.MapType = reflect.TypeOf(map[string]interface{}(nil))
	var v interface{}
	dec := codec.NewDecoderBytes(b, &mh)

	err := dec.Decode(&v)
	if err != nil {
		return nil, err
	}

	// get the event headers
	h, ok := v.([]interface{})[0].(map[interface{}]interface{})
	if !ok {
		return nil, errors.New("zerorpc/event interface conversion error")
	}

	header := make(map[string]interface{})

	for k, v := range h {
		switch t := v.(type) {
		case []byte:
			header[k.(string)] = string(t)

		default:
			header[k.(string)] = t
		}
	}

	// get the event name
	n, ok := v.([]interface{})[1].([]byte)
	if !ok {
		return nil, errors.New("zerorpc/event interface conversion error")
	}

	// converts an interface{} to a type
	convertValue := func(v interface{}) interface{} {
		var out interface{}

		switch t := v.(type) {
		case []byte:
			out = string(t)

		default:
			out = t
		}

		return out
	}

	// get the event args
	args := make([]interface{}, 0)

	for i := 2; i < len(v.([]interface{})); i++ {
		t := v.([]interface{})[i]

		switch t.(type) {
		case []interface{}:
			for _, a := range t.([]interface{}) {
				args = append(args, convertValue(a))
			}

		default:
			args = append(args, convertValue(t))
		}
	}

	e := Event{
		Header: header,
		Name:   string(n),
		Args:   args,
	}

	return &e, nil
}
