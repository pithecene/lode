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

var (
	// fence matches a fenced code block, and inline matches an inline code
	// span; the document names identifiers only inside code.
	fence  = regexp.MustCompile("(?s)```.*?```")
	inline = regexp.MustCompile("`[^`\n]+`")
	ident  = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
)

func TestEveryExportIsDocumented(t *testing.T) {
	t.Parallel()
	code := documentCode(t)
	for name, dir := range packages {
		for _, export := range exports(t, dir) {
			if !code[export] {
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

// documentCode returns every identifier inside the document's code blocks
// and inline code.
func documentCode(t *testing.T) map[string]bool {
	t.Helper()
	doc := readDocument(t)
	found := map[string]bool{}
	for _, span := range append(fence.FindAllString(doc, -1), inline.FindAllString(fence.ReplaceAllString(doc, ""), -1)...) {
		for _, id := range ident.FindAllString(span, -1) {
			found[id] = true
		}
	}
	return found
}

// exports returns the exported top-level identifiers of the package in dir,
// read from its non-test sources.
func exports(t *testing.T, dir string) []string {
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
			names = append(names, declared(decl)...)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
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
