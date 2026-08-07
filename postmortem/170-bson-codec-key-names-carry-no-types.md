# 170 -- A BSON Codec Where Key Names Carry No Types

## Background

mongodb.el's decoder and encoder spoke different languages. Decoding produced Extended-JSON-shaped alists -- `(("$oid" . "..."))` for an ObjectId, lists for arrays, `nil` for empty arrays, empty documents, and null alike, bare `"NaN"` strings for non-finite doubles. Encoding accepted none of those shapes: it wanted wrapper structs and vectors, treated any alist as an embedded document, and rejected lists outright. A decoded document could not be written back, and Clutch's generated single-document mutations reuse the decoded `_id` verbatim in their filters, so an update keyed on a decoded ObjectId sent `{_id: {"$oid": ...}}` -- a document equality that matches nothing, silently.

The first fix taught the encoder to recognize Extended JSON type tags: a single-entry alist keyed `$oid` became an ObjectId again. External audit produced the counterexample that killed it: a legitimate stored document whose field happens to be `{"$oid": "..."}` decodes to exactly the same alist, so re-encoding turned an embedded document into an ObjectId -- wire type 0x03 became 0x07. The ambiguity had been reversed, not removed. Two different BSON values shared one Elisp representation, and no amount of cleverness at the encoding site can recover information the representation already lost.

## Decision

Type information lives in the representation, never in key names. Every non-scalar BSON type decodes to the wrapper struct the encoder already accepted -- `mongodb-object-id`, `mongodb-datetime`, `mongodb-decimal128`, and the rest -- and the tag recognizer is deleted, so an alist always encodes as an embedded document whatever its keys look like. Arrays decode to vectors, since a list is indistinguishable from an alist and an empty list from null; an empty embedded document decodes to the `mongodb-document` wrapper that already existed to express it on the encode side. The property test asserts byte-identical re-encoding over a document carrying every BSON type, and a regression pins the audit's counterexample.

Two consequences surfaced late and belong to the same principle:

- int64 decoded bare, and bare integers re-encode by numeric range, so `(mongodb-int64 7)` came back as int32. The property test's int64 exceeded the int32 range, which is exactly why it never noticed -- a bijectivity property over a width-sensitive type must sweep the boundary, not sample one side of it. 0x12 now keeps its wrapper; int32 stays bare because a bare in-range integer re-encodes as int32 deterministically. The protocol itself consumes int64s: `getMore` rejects an int32 cursor id, so cursor ids now ride the wire wrapped.
- Canonicalization is a class, not a special case. Non-canonical NaN payloads, and the BSON corpus's non-canonical Decimal128 steering encodings, decode to canonical semantic values and re-encode canonically. The contract is stated once in the README as semantic-not-byte equivalence for non-canonical encodings, with tests asserting both the byte difference and that the canonical form is a fixed point.

## Rejected alternative

Making decode produce Extended JSON everywhere and teaching encode the full tag vocabulary was rejected after the counterexample: it preserves the readable-alist surface but requires key names to carry types, which is the defect. Rendering moved to the consumer instead -- Clutch spells wrapper structs back out as Extended JSON text for display, one converter, no protocol semantics.

## Tradeoff

Decoded documents are no longer directly readable as Extended JSON data; every consumer that wants that spelling needs the display converter. Nested empty documents are wrappers while non-empty ones stay alists -- an asymmetry accepted to keep every existing alist consumer working. Decimal128 values remain text, exact but not arithmetic-ready.
