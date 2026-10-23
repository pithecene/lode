// Package errprefix holds the test that enforces the error message prefix
// rule of CONTRACT_ERRORS.md: a source prefix such as "lode:" or "s3:" is
// added only where an error originates, never where a Lode error is
// wrapped. The package has no exported API; its test type-checks Lode's
// packages and reports every error construction that breaks the rule.
package errprefix
