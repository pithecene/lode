# Lode Documentation Site

Pithecene publishes this repository's documentation for each release, from
v0.12.1 onward, at [pithecene.sh/docs/lode](https://pithecene.sh/docs/lode).
The site reads the documents at each release's tag and keeps its own list of
the pages that it publishes. A change to the path of a listed document, or to
a README heading that the site selects, therefore needs a matching change to
that list. A new document appears on the site once the list includes it.

## Published Documents

- `README.md`: the introduction and the sections "Core Ideas" and
  "Guarantees" form the Overview page, and the section "Quick Start" forms
  the Quick Start page.
- `PUBLIC_API.md` and `CHANGELOG.md`.
- Each guide under `docs/guides/`.
- Each contract under `docs/contracts/` except `CONTRACT_EXAMPLES.md`.

## Renaming or Removing a Document

The latest release must hold every listed page. When a release lacks one, the
site records the release as incomplete, serves the release before it as the
latest, and tries the new release again an hour later. Readers therefore see
the earlier release until the site's list matches the new one. A pull request
that renames or removes a listed document, or renames a selected README
heading, says so in its description, so that the site's list changes before
the release.
