package venti

import "testing"

func TestPackRoot(t *testing.T) {
	r := Root{
		Name:      "foo",
		Type:      "bar",
		Score:     ZeroScore(),
		BlockSize: 256,
		Prev:      ZeroScore(),
	}

	buf := make([]byte, RootSize)
	if err := r.Pack(buf); err != nil {
		t.Fatal(err)
	}

	rr, err := UnpackRoot(buf)
	if err != nil {
		t.Fatal(err)
	}

	if *rr != r {
		t.Fatalf("results differ: \n%v\n\tvs\n%v", r, *rr)
	}
}
