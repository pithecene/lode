// Package sample holds one error construction per case of the prefix rule.
// Each line that breaks the rule ends with a "violation" comment.
package sample

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

var errBare = errors.New("bare sentinel")

var errPrefixed = errors.New("lode: prefixed sentinel") // violation

var errUnknownPrefix = errors.New("history: prefixed sentinel") // violation

// Errors is a stand-in sentinel set; only the real table entry is honored.
type Errors struct{ NotFound error }

func fromLode() error { return errBare }

// Cases builds one error per case.
func Cases(name string, errs Errors) []error {
	_, err := os.Open(name)
	entered := fmt.Errorf("lode: open %s: %w", name, err)
	unnamed := fmt.Errorf("open %s: %w", name, err) // violation
	created := errors.New("lode: stream is closed")
	bare := errors.New("stream is closed") // violation
	classified := fmt.Errorf("lode: snapshot %s: %w", name, errBare)
	unclassified := fmt.Errorf("snapshot %s: %w", name, errBare) // violation
	lodeErr := fromLode()
	context := fmt.Errorf("load %s: %w", name, lodeErr)
	doubled := fmt.Errorf("lode: load %s: %w", name, lodeErr) // violation
	direct := fmt.Errorf("lode: load: %w", fromLode())        // violation
	lodeErr = fmt.Errorf("retry: %w", lodeErr)
	unlisted := fmt.Errorf("lode: %w", errs.NotFound) // violation
	walked := filepath.WalkDir(name, func(_ string, _ fs.DirEntry, err error) error {
		return fmt.Errorf("lode: walk: %w", err)
	})
	_, statErr := os.Stat(name)
	copied := statErr
	stat := fmt.Errorf("stat: %w", copied) // violation
	return []error{errPrefixed, errUnknownPrefix, entered, unnamed, created, bare, classified, unclassified, context, doubled, direct, lodeErr, unlisted, walked, stat}
}
