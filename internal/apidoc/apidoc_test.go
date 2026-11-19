package apidoc

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// root is the module root, relative to this package's directory.
const root = "../.."

// packages maps the name by which PUBLIC_API.md qualifies each public
// package to its directory under the module root.
var packages = map[string]string{
	"lode":   "lode",
	"s3":     "lode/s3",
	"volume": "lode/volume",
}

// unqualified names the package whose identifiers the document may also
// name without a qualifier.
const unqualified = "lode"

var (
	// fence matches a fenced code block, and inline matches an inline code
	// span; the document names identifiers only inside code.
	fence  = regexp.MustCompile("(?s)```.*?```")
	inline = regexp.MustCompile("`[^`\n]+`")
	// reference matches an identifier with its optional qualifier. An
	// identifier after any other selector, such as the field in snap.ID, is
	// not a reference to a package's export.
	reference = regexp.MustCompile(`(?:^|[^.\w])(?:([a-z][a-z0-9]*)\.)?([A-Za-z_][A-Za-z0-9_]*)`)
)

func TestEveryExportIsDocumented(t *testing.T) {
	t.Parallel()
	named := documentReferences(t)
	for name, dir := range packages {
		for _, export := range exports(t, dir) {
			if !named[name+"."+export] {
				t.Errorf("PUBLIC_API.md does not name %s.%s", name, export)
			}
		}
	}
}

func TestEveryQualifiedNameExists(t *testing.T) {
	t.Parallel()
	doc := readDocument(t)
	for name, dir := range packages {
		known := exports(t, dir)
		qualified := regexp.MustCompile(`\b` + name + `\.([A-Z][A-Za-z0-9_]*)`)
		for _, m := range qualified.FindAllStringSubmatch(doc, -1) {
			if !slices.Contains(known, m[1]) {
				t.Errorf("PUBLIC_API.md names %s.%s, which the package does not export", name, m[1])
			}
		}
	}
}

func readDocument(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "PUBLIC_API.md"))
	if err != nil {
		t.Fatalf("read PUBLIC_API.md: %v", err)
	}
	return string(data)
}

// documentReferences returns each package-qualified name that the
// document's code blocks and inline code refer to, as "pkg.Name". An
// unqualified name refers to the package named by unqualified.
func documentReferences(t *testing.T) map[string]bool {
	t.Helper()
	doc := readDocument(t)
	found := map[string]bool{}
	for _, span := range append(fence.FindAllString(doc, -1), inline.FindAllString(fence.ReplaceAllString(doc, ""), -1)...) {
		for _, m := range reference.FindAllStringSubmatch(span, -1) {
			pkg := m[1]
			if pkg == "" {
				pkg = unqualified
			}
			found[pkg+"."+m[2]] = true
		}
	}
	return found
}

// exports returns the exported top-level identifiers of the package in dir,
// read from its non-test sources.
func collectDecls(t *testing.T, dir string, extract func(ast.Decl) []string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, dir))
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	fset := token.NewFileSet()
	var names []string
	for _, entry := range entries {
		if !isSource(entry) {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(root, dir, entry.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, decl := range file.Decls {
			names = append(names, extract(decl)...)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func exports(t *testing.T, dir string) []string {
	return collectDecls(t, dir, declared)
}

// isSource reports whether a directory entry is a non-test Go source file.
func isSource(entry fs.DirEntry) bool {
	name := entry.Name()
	return !entry.IsDir() && strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

// declared returns the exported top-level names that one declaration
// introduces. A method belongs to its type, so it adds none.
func declared(decl ast.Decl) []string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if d.Recv == nil && d.Name.IsExported() {
			return []string{d.Name.Name}
		}
	case *ast.GenDecl:
		var names []string
		for _, spec := range d.Specs {
			names = append(names, specNames(spec)...)
		}
		return names
	}
	return nil
}

func specNames(spec ast.Spec) []string {
	switch s := spec.(type) {
	case *ast.TypeSpec:
		if s.Name.IsExported() {
			return []string{s.Name.Name}
		}
	case *ast.ValueSpec:
		var names []string
		for _, n := range s.Names {
			if n.IsExported() {
				names = append(names, n.Name)
			}
		}
		return names
	}
	return nil
}

func exportedTypes(t *testing.T, dir string) []string {
	return collectDecls(t, dir, func(decl ast.Decl) []string {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			return nil
		}
		var out []string
		for _, spec := range gen.Specs {
			if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.IsExported() {
				out = append(out, ts.Name.Name)
			}
		}
		return out
	})
}

var (
	contractFilePattern = regexp.MustCompile(`\bCONTRACT_[A-Z0-9_]+\.md\b`)
	goFencePattern      = regexp.MustCompile("(?s)```go\n(.*?)```")
	contractTypeDecl    = regexp.MustCompile(`(?m)^type\s+([A-Z][A-Za-z0-9_]*)\b`)
	matrixTestRef       = regexp.MustCompile("`((?:Test|Benchmark|Fuzz)[A-Za-z0-9_]+)(?:/[^`]*)?`")
)

func TestEveryContractIsIndexedAndTypesExist(t *testing.T) {
	t.Parallel()
	indexData, err := os.ReadFile(filepath.Join(root, "docs", "ARCH_INDEX.md"))
	if err != nil {
		t.Fatalf("read docs/ARCH_INDEX.md: %v", err)
	}
	indexed := map[string]bool{}
	for _, m := range contractFilePattern.FindAllString(string(indexData), -1) {
		indexed[m] = true
	}

	allTypes := map[string]bool{}
	for _, dir := range packages {
		for _, typ := range exportedTypes(t, dir) {
			allTypes[typ] = true
		}
	}

	contractsDir := filepath.Join(root, "docs", "contracts")
	entries, err := os.ReadDir(contractsDir)
	if err != nil {
		t.Fatalf("read docs/contracts: %v", err)
	}
	onDisk := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "CONTRACT_") || !strings.HasSuffix(name, ".md") {
			continue
		}
		onDisk[name] = true
		if !indexed[name] {
			t.Errorf("docs/contracts/%s is not listed in docs/ARCH_INDEX.md", name)
		}
		body, err := os.ReadFile(filepath.Join(contractsDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, block := range goFencePattern.FindAllStringSubmatch(string(body), -1) {
			for _, tm := range contractTypeDecl.FindAllStringSubmatch(block[1], -1) {
				if !allTypes[tm[1]] {
					t.Errorf("docs/contracts/%s declares type %s, which no public package exports", name, tm[1])
				}
			}
		}
	}
	for name := range indexed {
		if !onDisk[name] {
			t.Errorf("docs/ARCH_INDEX.md lists %s, which does not exist in docs/contracts/", name)
		}
	}
}

func TestMatrixNamesExistingTests(t *testing.T) {
	t.Parallel()
	matrixData, err := os.ReadFile(filepath.Join(root, "ai", "TEST_MATRIX.md"))
	if err != nil {
		t.Fatalf("read ai/TEST_MATRIX.md: %v", err)
	}
	declaredTests := collectTestFuncs(t)
	for _, m := range matrixTestRef.FindAllStringSubmatch(string(matrixData), -1) {
		if !declaredTests[m[1]] {
			t.Errorf("ai/TEST_MATRIX.md references %s, which does not exist in *_test.go", m[1])
		}
	}
}

func collectTestFuncs(t *testing.T) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	fset := token.NewFileSet()
	for _, base := range []string{"lode", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, base), func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() || !strings.HasSuffix(d.Name(), "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if ok && fn.Recv == nil && fn.Name.IsExported() {
					found[fn.Name.Name] = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", base, err)
		}
	}
	return found
}
