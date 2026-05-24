package keys

import (
	"bytes"
	"sort"
	"testing"
)

func TestEncodeParse_RoundTrip(t *testing.T) {
	cases := []struct {
		user []byte
		seq  uint64
		kind Kind
	}{
		{[]byte(""), 0, KindInlineValue},
		{[]byte("a"), 1, KindInlineValue},
		{[]byte("abc"), 42, KindDeletion},
		{[]byte{0xff, 0xff, 0xff}, MaxSeq, KindVlogPointer},
		{[]byte("with\x00null"), 12345, KindRangeDeletion},
	}
	for _, tc := range cases {
		buf := Encode(nil, tc.user, tc.seq, tc.kind)
		gotUser, gotSeq, gotKind, ok := Parse(buf)
		if !ok {
			t.Fatalf("Parse failed for %v", tc)
		}
		if !bytes.Equal(gotUser, tc.user) {
			t.Errorf("user: got %q want %q", gotUser, tc.user)
		}
		if gotSeq != tc.seq {
			t.Errorf("seq: got %d want %d", gotSeq, tc.seq)
		}
		if gotKind != tc.kind {
			t.Errorf("kind: got %v want %v", gotKind, tc.kind)
		}
	}
}

func TestParse_Short(t *testing.T) {
	_, _, _, ok := Parse([]byte("abc"))
	if ok {
		t.Fatal("Parse should reject short key")
	}
}

func TestOrder_UserKeyAscending(t *testing.T) {
	// Different user keys at the same seq should sort by user-key ascending.
	a := Encode(nil, []byte("a"), 100, KindInlineValue)
	b := Encode(nil, []byte("b"), 100, KindInlineValue)
	if bytes.Compare(a, b) >= 0 {
		t.Errorf("user ordering broken: %v vs %v", a, b)
	}
}

func TestOrder_LatestSeqFirst(t *testing.T) {
	// Same user key: higher seq must sort before lower seq.
	older := Encode(nil, []byte("k"), 5, KindInlineValue)
	newer := Encode(nil, []byte("k"), 10, KindInlineValue)
	if bytes.Compare(newer, older) >= 0 {
		t.Errorf("newer should sort first: newer=%v older=%v", newer, older)
	}
}

func TestOrder_TypeTagBreaksTies(t *testing.T) {
	// Same (user, seq): the lookup-tag (0x00) sorts BEFORE any real kind
	// (which start at 0x01). This makes SeekGE land directly on the real
	// entry rather than past it.
	real := Encode(nil, []byte("k"), 7, KindInlineValue)
	lookup := LookupKey(nil, []byte("k"), 7)
	if bytes.Compare(lookup, real) >= 0 {
		t.Errorf("lookup should sort before real entry: real=%v lookup=%v", real, lookup)
	}
}

func TestOrder_LookupKeyLandsCorrectly(t *testing.T) {
	// Place several versions of "k" in sorted order; a LookupKey at
	// snapshotSeq=7 should sort just BEFORE the largest entry with
	// (user_key=k, seq≤7). SeekGE on the lookup returns that entry
	// directly.
	entries := [][]byte{
		Encode(nil, []byte("k"), 10, KindInlineValue),
		Encode(nil, []byte("k"), 7, KindInlineValue),
		Encode(nil, []byte("k"), 3, KindDeletion),
		Encode(nil, []byte("kk"), 100, KindInlineValue),
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i], entries[j]) < 0
	})

	lookup := LookupKey(nil, []byte("k"), 7)
	idx := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i], lookup) >= 0
	})
	if idx == len(entries) {
		t.Fatal("lookup landed past last entry; expected (k, 7, inline)")
	}
	got := entries[idx]
	user, seq, kind, _ := Parse(got)
	if !bytes.Equal(user, []byte("k")) || seq != 7 || kind != KindInlineValue {
		t.Errorf("expected (k, 7, inline) at idx %d, got (%q, %d, %v)", idx, user, seq, kind)
	}
}

func TestCompare_HandlesPrefixOverlap(t *testing.T) {
	// "alpha" and "alphab" — one is a prefix of the other. The trailer of
	// "alpha" begins at byte 5, where "alphab" still has user-key content.
	// Plain bytes.Compare would wrongly put alpha (at high inv-seq) AFTER
	// alphab; the internal-key comparator must look at user-key prefixes
	// first.
	alpha := Encode(nil, []byte("alpha"), 100, KindInlineValue)
	alphab := Encode(nil, []byte("alphab"), 100, KindInlineValue)
	if Compare(alpha, alphab) >= 0 {
		t.Errorf("Compare(alpha, alphab) = %d, want < 0", Compare(alpha, alphab))
	}

	// Inverse case: "alphab" comes before "alphabc".
	alphabc := Encode(nil, []byte("alphabc"), 100, KindInlineValue)
	if Compare(alphab, alphabc) >= 0 {
		t.Errorf("Compare(alphab, alphabc) = %d, want < 0", Compare(alphab, alphabc))
	}

	// Same user key, different seqs — higher seq sorts first.
	older := Encode(nil, []byte("k"), 5, KindInlineValue)
	newer := Encode(nil, []byte("k"), 10, KindInlineValue)
	if Compare(newer, older) >= 0 {
		t.Errorf("Compare(newer, older) = %d, want < 0", Compare(newer, older))
	}
}

func TestPanic_SeqOverflow(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on seq > MaxSeq")
		}
	}()
	Encode(nil, []byte("k"), MaxSeq+1, KindInlineValue)
}

func TestHelpers(t *testing.T) {
	ikey := Encode(nil, []byte("hello"), 42, KindInlineValue)
	if got := UserKey(ikey); !bytes.Equal(got, []byte("hello")) {
		t.Errorf("UserKey = %q", got)
	}
	if got := Seq(ikey); got != 42 {
		t.Errorf("Seq = %d", got)
	}
	if got := KindOf(ikey); got != KindInlineValue {
		t.Errorf("KindOf = %v", got)
	}
}

func TestHelpers_Panic(t *testing.T) {
	for _, fn := range []func([]byte){
		func(b []byte) { _ = UserKey(b) },
		func(b []byte) { _ = Seq(b) },
		func(b []byte) { _ = KindOf(b) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic on short key")
				}
			}()
			fn([]byte("abc"))
		}()
	}
}
