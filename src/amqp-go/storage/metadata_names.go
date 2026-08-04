package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
)

// ---------------------------------------------------------------------------
// Metadata / offset FILE SLOTS from client-controlled names.
//
// review-4 N-1, executed: StoreQueue("../../../pwned-queue") returned nil and
// wrote pwned-queue.cbor OUTSIDE the data directory. Beyond the arbitrary-write
// primitive it is the same SILENT-LOSS shape as the segment tier's S-7:
// ListQueues enumerates metadata/queues/*.cbor, so such a queue's record —
// including its write-once delivery-tag Ordinal — is invisible at the next boot.
// existingOrdinalLocked then reports "no record", StoreQueue mints a FRESH
// ordinal, and every confirmed durable record already on disk under the old
// ordinal takes recovery's Case B "dead incarnation" branch and is discarded
// while the boot reports success.
//
// SAME MECHANISM AS THE SEGMENT TIER, adapted to files. Segments disambiguate a
// directory with a marker FILE inside it; a metadata record cannot hold a file,
// but it does not need to: the CBOR record already carries its own Name, and the
// offset JSON carries QueueName/ConsumerTag. So the record announces its own
// identity, the filename is only a SLOT, and every reader that enumerates a
// directory takes the name from the CONTENT and every reader that looks a name
// up CHECKS the content it found. One property, two tiers:
//
//	no artifact is ever attributed to a name that did not write it.
//
// ZERO MIGRATION, stated precisely. A name stays in its literal slot whenever
// the OLD build could actually write that slot. For the CBOR tiers the bound is
// atomicWrite's temp file (<name>.cbor.tmp), so 246 bytes; "%" is NOT reserved,
// so no existing file moves.
// ---------------------------------------------------------------------------

// metadataEscapePrefix begins a generated slot name. Like the segment tier's
// prefix this is a readability convention, not a decodable encoding: identity
// lives in the record.
const metadataEscapePrefix = "%"

// metadataEscapeDir is the SUBDIRECTORY generated slots live in, and it is what
// makes the two namespaces disjoint rather than merely usually-distinct.
//
// A literal slot is a file DIRECTLY in the tier directory; a generated slot is a
// file one level down. So a generated name can never occupy a literal name's
// file, and — this is the part that matters — A NAME THAT WORKED BEFORE CAN NEVER
// BE REFUSED. Sharing one flat directory would have allowed queue "/" (which
// generates "%2f.cbor") to take the file queue "%2f" owns literally, and then
// refusing "%2f" would be the review-4 B-2 shape: a previously-legal name failing
// forever because of a name that never worked.
//
// The only artifacts that can already be inside this subdirectory are the
// records of legacy queues named "%escaped/<x>" — which contain a separator, so
// the OLD build's non-recursive ListQueues never saw them and they never worked
// either. A conflict there is therefore symmetric between two names that both
// never worked, and refusing it disturbs nothing.
const metadataEscapeDir = "%escaped"

// metadataSlotMaxNameLen is the longest raw name whose LITERAL slot the previous
// build could write. atomicWrite writes <path>+TempFileExtension and renames, so
// the temp entry — not the final one — is the binding constraint.
const metadataSlotMaxNameLen = 255 - len(FileExtension) - len(TempFileExtension)

// metadataNameIsLiteral reports whether a record name can be used verbatim as a
// single path element. The excluded set is the smallest one that closes the
// escape: the empty name, "." and "..", the PATH SEPARATOR, and a NUL.
//
// It must be exactly "could the previous build write <name>+ext here", because
// unlike the segment tier there is no marker to adopt a differently-spelled
// legacy artifact with: a file cannot carry a marker beside itself, and probing
// for one would put a stat on GetQueue's cache-miss path, which is the publish
// path. So a name whose literal file the old build COULD write must keep it, or
// its write-once Ordinal becomes invisible at the next boot.
//
// That is why "\\" is NOT excluded here even though the segment tier excludes it
// from its preferred spelling: on POSIX the old build really did write
// "a\\b.cbor", and moving that file would be the review-4 B-1 defect one tier
// over.
func metadataNameIsLiteral(name string, maxLen int) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if len(name) > maxLen {
		return false
	}
	return !strings.ContainsRune(name, filepath.Separator) && !strings.ContainsRune(name, 0)
}

// metadataEscapedName generates a single path element for a name that cannot be
// used literally, bounded by maxLen. Hex keeps it readable; when hex would not
// fit, the tail becomes a full SHA-256 so distinct names stay distinct.
func metadataEscapedName(name string, maxLen int) string {
	full := metadataEscapePrefix + hex.EncodeToString([]byte(name))
	if len(full) <= maxLen {
		return full
	}
	sum := sha256.Sum256([]byte(name))
	digest := hex.EncodeToString(sum[:])
	keep := maxLen - 1 - len(digest)
	if keep < 1 {
		// Unreachable at any maxLen this package uses; kept so the slice cannot
		// panic if a future caller passes a tiny bound.
		return digest[:maxLen]
	}
	return full[:keep] + "-" + digest
}

// metadataSlotName maps a record name to its slot, relative to the tier
// directory, INCLUDING FileExtension. A name the previous build could write
// literally keeps its exact file; anything else goes into metadataEscapeDir.
func metadataSlotName(name string) string {
	if metadataNameIsLiteral(name, metadataSlotMaxNameLen) {
		return name + FileExtension
	}
	return filepath.Join(metadataEscapeDir,
		metadataEscapedName(name, metadataSlotMaxNameLen)+FileExtension)
}

// metadataSlotIsEscaped reports whether this name's slot is a generated one, and
// therefore whether an occupancy check is needed before writing. A literal slot
// can only ever be written by the one name that owns it, so it needs no check —
// which also keeps the extra read off the declare path for every ordinary name.
func metadataSlotIsEscaped(name string) bool {
	return !metadataNameIsLiteral(name, metadataSlotMaxNameLen)
}

// metadataPathComponent maps one client-controlled name to a single path
// element with no extension. Used where a name is a DIRECTORY component
// (consumers/<queue>/) rather than a file stem.
func metadataPathComponent(name string) string {
	if metadataNameIsLiteral(name, 255) {
		return name
	}
	return metadataEscapedName(name, 255)
}

// filenameComponent maps one client-controlled value to one component of a
// multi-value filename, joined elsewhere as "<A>_<B>" (or more components with
// more "_" separators).
//
// The join is NOT INJECTIVE over raw components: Base("a/b") and Base("b") are
// both "b", and "<A>_<B>" makes ("a_b","c") and ("a","b_c") the same string —
// so one identity's file could silently collide with another's. "_" is
// therefore escaped along with the unsafe bytes, which makes the join
// unambiguous.
//
// The escape prefix here is "_", not "%", and that choice is what makes the
// JOIN injective rather than merely the components. A literal component contains
// no "_" at all, and a generated one is "_"+hex (hex contains no "_"), so in
// "<A>_<B>" the first "_" either opens A (when the string starts with one, and A
// then ends at the next "_") or separates A from B. Every concatenation is
// therefore uniquely attributable to one tuple of source values. With "%" as
// the prefix, generated "%2f" and literal "%2f" would have been the same
// component.
func filenameComponent(name string, maxLen int) string {
	if metadataNameIsLiteral(name, maxLen) && !strings.Contains(name, "_") {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	full := "_" + hex.EncodeToString([]byte(name))
	if len(full) <= maxLen {
		return full
	}
	digest := hex.EncodeToString(sum[:])
	return full[:maxLen-1-len(digest)] + "-" + digest
}
