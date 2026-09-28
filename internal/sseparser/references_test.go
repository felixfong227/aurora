package sseparser

import "testing"

func TestReferenceIndexesSurviveEmptyAndReplacedSlots(t *testing.T) {
	state := &PatchState{}
	ApplyPatch(state, "/message/metadata/content_references", "append", []interface{}{
		map[string]interface{}{},
		map[string]interface{}{"matched_text": "marker-one"},
	})
	ApplyPatch(state, "/message/metadata/content_references/1/alt", "replace", "[One](https://example.invalid/one)")
	if state.CiteAlts["marker-one"] != "[One](https://example.invalid/one)" {
		t.Fatal("empty reference shifted the next array slot")
	}
	ApplyPatch(state, "/message/metadata/content_references/4", "replace", map[string]interface{}{"alt": "[Four](https://example.invalid/four)"})
	ApplyPatch(state, "/message/metadata/content_references/4/matched_text", "replace", "marker-four")
	if state.CiteAlts["marker-four"] != "[Four](https://example.invalid/four)" {
		t.Fatal("out-of-order alt-first reference lost its explicit index")
	}
	ApplyPatch(state, "/message/metadata/content_references/1", "replace", map[string]interface{}{"matched_text": "replacement"})
	if state.CiteAlts["replacement"] != "" {
		t.Fatal("replacement inherited the old slot's link")
	}
	ApplyPatch(state, "/message/metadata/content_references/1/alt", "replace", "[Replacement](https://example.invalid/new)")
	if state.CiteAlts["replacement"] != "[Replacement](https://example.invalid/new)" {
		t.Fatal("replacement reference lost its explicit index")
	}
}
