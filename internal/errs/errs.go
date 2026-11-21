// Package errs records deliberate error discards. Routing a discard through
// a named helper keeps it visible to review and to the errdiscard analyzer,
// which bans blank assignments and errcheck suppressions that hide one.
package errs

import "io"

// IgnoreError discards err. A call records a deliberate discard: the caller
// has decided the error cannot change its outcome, such as best-effort
// cleanup after the operation's own error is already being returned.
func IgnoreError(_ error) {}

// IgnoreResult discards a value and its error. A call records a deliberate
// discard of both, for calls whose result and failure the caller does not
// need.
func IgnoreResult[T any](_ T, _ error) {}

// DiscardClose closes c and discards the error. A call records a deliberate
// discard, for closers whose close failure cannot affect the outcome, such
// as read-only streams or cleanup on an error path.
func DiscardClose(c io.Closer) {
	IgnoreError(c.Close())
}
