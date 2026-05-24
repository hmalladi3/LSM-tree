package record

import (
	"bytes"
	"testing"

	"github.com/harimalladi/slate/internal/keys"
)

func TestEncodeDecode_Inline(t *testing.T) {
	buf := Encode(nil, 42, keys.KindInlineValue, []byte("hello"), []byte("world"))
	seq, kind, key, value, n, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 42 || kind != keys.KindInlineValue {
		t.Errorf("seq=%d kind=%v", seq, kind)
	}
	if !bytes.Equal(key, []byte("hello")) || !bytes.Equal(value, []byte("world")) {
		t.Errorf("key=%q value=%q", key, value)
	}
	if n != len(buf) {
		t.Errorf("n=%d, want %d", n, len(buf))
	}
}

func TestEncodeDecode_Deletion(t *testing.T) {
	buf := Encode(nil, 7, keys.KindDeletion, []byte("k"), nil)
	seq, kind, key, value, _, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 7 || kind != keys.KindDeletion {
		t.Errorf("seq=%d kind=%v", seq, kind)
	}
	if !bytes.Equal(key, []byte("k")) {
		t.Errorf("key=%q", key)
	}
	if value != nil {
		t.Errorf("value should be nil, got %v", value)
	}
}

func TestEncodeDecode_EmptyValue(t *testing.T) {
	buf := Encode(nil, 1, keys.KindInlineValue, []byte("k"), []byte{})
	_, kind, _, value, _, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if kind != keys.KindInlineValue {
		t.Errorf("kind=%v", kind)
	}
	if value == nil || len(value) != 0 {
		t.Errorf("expected empty-but-non-nil value, got %v", value)
	}
}

func TestDecode_Truncated(t *testing.T) {
	full := Encode(nil, 1, keys.KindInlineValue, []byte("hello"), []byte("world"))
	for i := 0; i < len(full); i++ {
		_, _, _, _, _, err := Decode(full[:i])
		if err == nil {
			t.Errorf("Decode of first %d bytes succeeded; expected truncation", i)
		}
	}
}

func TestDecode_AfterAppend_BothReadable(t *testing.T) {
	buf := Encode(nil, 1, keys.KindInlineValue, []byte("k1"), []byte("v1"))
	buf = Encode(buf, 2, keys.KindDeletion, []byte("k2"), nil)
	buf = Encode(buf, 3, keys.KindInlineValue, []byte("k3"), []byte("v3"))

	var got [][]byte
	rem := buf
	for len(rem) > 0 {
		seq, kind, key, value, n, err := Decode(rem)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, append([]byte{byte(seq), byte(kind)}, key...))
		_ = value
		rem = rem[n:]
	}
	if len(got) != 3 {
		t.Errorf("got %d records, want 3", len(got))
	}
}
