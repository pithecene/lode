# Security Policy

## Supported Versions

Lode is pre-1.0. Security fixes are released as a new patch version of the
latest minor version; earlier versions do not receive backported fixes. To
obtain a fix, upgrade to the latest release.

| Version | Supported |
|---------|-----------|
| Latest release | Yes |
| Earlier releases | No |

## Reporting a Vulnerability

Report vulnerabilities privately. Do **not** open a public issue, pull
request, or discussion for a suspected vulnerability.

- **GitHub (preferred)**: [report a
  vulnerability](https://github.com/pithecene/lode/security/advisories/new)
  through private vulnerability reporting.
- **Email**: contact@pithecene.sh

Include the following information:

1. the affected version (the `github.com/pithecene-io/lode` version in
   `go.mod`) and package (`lode`, `lode/s3`, or `lode/volume`);
2. a description of the issue and its impact;
3. steps to reproduce it, or a proof of concept; and
4. any known mitigation.

## Disclosure

A confirmed vulnerability is corrected in a patch release and disclosed
through a GitHub Security Advisory and under **Security** in that release's
`CHANGELOG.md` entry. Vulnerabilities in dependencies are addressed by
upgrading the dependency and are recorded in the same way.
