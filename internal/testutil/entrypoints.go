package testutil

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// EntryPoints returns the exported functions and methods of the package in
// dir that take a parameter of one of the named types, as "Func" or
// "Owner.Method", where Owner is the receiver type or the declaring
// interface. A test that checks every entry point of a kind compares its
// cases with this list, so that a new entry point cannot go unchecked.
func EntryPoints(dir string, types ...string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	var found []string
	for _, entry := range entries {
		if !isSource(entry) {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, entry.Name()), nil, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range file.Decls {
			found = append(found, declEntryPoints(decl, types)...)
		}
	}
	slices.Sort(found)
	return slices.Compact(found), nil
}

// isSource reports whether a directory entry is a non-test Go source file.
func isSource(entry fs.DirEntry) bool {
	name := entry.Name()
	return !entry.IsDir() && strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

// declEntryPoints returns the entry points that one declaration introduces:
// an exported function or method on an exported type, or the methods of an
// exported interface.
func declEntryPoints(decl ast.Decl, types []string) []string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if !d.Name.IsExported() || !takesType(d.Type, types) {
			return nil
		}
		if d.Recv == nil {
			return []string{d.Name.Name}
		}
		if owner := typeName(d.Recv.List[0].Type); ast.IsExported(owner) {
			return []string{owner + "." + d.Name.Name}
		}
	case *ast.GenDecl:
		var found []string
		for _, spec := range d.Specs {
			found = append(found, interfaceEntryPoints(spec, types)...)
		}
		return found
	}
	return nil
}

func interfaceEntryPoints(spec ast.Spec, types []string) []string {
	ts, ok := spec.(*ast.TypeSpec)
	if !ok || !ts.Name.IsExported() {
		return nil
	}
	iface, ok := ts.Type.(*ast.InterfaceType)
	if !ok {
		return nil
	}
	var found []string
	for _, m := range iface.Methods.List {
		fn, ok := m.Type.(*ast.FuncType)
		if !ok || !takesType(fn, types) {
			continue
		}
		for _, name := range m.Names {
			if name.IsExported() {
				found = append(found, ts.Name.Name+"."+name.Name)
			}
		}
	}
	return found
}

// takesType reports whether a function has a parameter whose type, apart
// from pointers, slices, and variadic dots, is one of the named types.
func takesType(fn *ast.FuncType, types []string) bool {
	for _, param := range fn.Params.List {
		if slices.Contains(types, typeName(param.Type)) {
			return true
		}
	}
	return false
}

// typeName is the name of the type that an expression denotes, apart from
// pointers, slices, variadic dots, type arguments, and a package qualifier.
func typeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return typeName(t.X)
	case *ast.ArrayType:
		return typeName(t.Elt)
	case *ast.Ellipsis:
		return typeName(t.Elt)
	case *ast.IndexExpr:
		return typeName(t.X)
	case *ast.IndexListExpr:
		return typeName(t.X)
	case *ast.SelectorExpr:
		return t.Sel.Name
	}
	return ""
}
