package errprefix

import (
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// module is the import path of the module whose packages are checked.
const module = "github.com/pithecene/lode"

// root is the module root, relative to this package's directory.
const root = "../.."

// sources lists, by package, the prefixes an error that originates in the
// package may begin with. A package absent from the table reports as Lode.
var sources = map[string][]string{
	"lode":    {"lode", "fs", "memory", "jsonl", "parquet"},
	"lode/s3": {"s3"},
}

var defaultSources = []string{"lode"}

// exempt holds the packages outside the rule, with the reason for each.
var exempt = map[string]string{
	"internal/chaos":     "the fault-injection harness; its errors never reach a caller",
	"internal/testutil":  "test helpers; their errors never reach a caller",
	"internal/errprefix": "this checker and its sample package",
}

// outsideCallees are callees that Lode declares but that stand for code
// outside Lode, so an error returned by one enters Lode at the call. A key
// names a package function ("lode.openRead"), every method of an interface
// ("lode/s3.API"), or a function-valued field ("lode/s3.Store.createTemp").
// The body of a listed package function is not checked: it returns errors
// in the shape of the code it stands for.
var outsideCallees = map[string]string{
	"lode/s3.API":              "the AWS SDK client implements it",
	"lode/s3.Store.createTemp": "it is os.CreateTemp, replaced only by tests",
	"lode.createTemp":          "it stands for os.CreateTemp and returns its error unchanged",
	"lode.openRead":            "it stands for os.Open on each platform and returns an *os.PathError",
	"lode.replaceFile":         "it stands for os.Rename on each platform and returns an *os.LinkError",
	"lode.lockExclusive":       "it stands for flock or LockFileEx and returns the system error unchanged",
}

// sentinelSets are struct types whose fields hold sentinel errors.
var sentinelSets = map[string]string{
	"internal/history.Errors": "each container passes its sentinels to the shared history",
}

// lodeBuilders are the standard-library functions that build an error in
// Lode's own code rather than report one from outside it.
var lodeBuilders = map[string]string{
	"fmt.Errorf":  "its message is checked where it is called",
	"errors.New":  "its message is checked where it is called",
	"errors.Join": "it joins errors that each carry their own prefix",
}

// platforms are the operating systems whose files are checked. Checking
// each on every runner covers every build-constrained file.
var platforms = []string{"linux", "windows", "plan9"}

// prefixPattern captures the leading word of a message when it is followed
// by a colon, the shape of a source prefix.
var prefixPattern = regexp.MustCompile(`^([a-z0-9]+): `)

// TestErrorPrefixes enforces the error message prefix rule of
// CONTRACT_ERRORS.md on every package under lode/ and internal/.
func TestErrorPrefixes(t *testing.T) {
	packages, err := packageDirs()
	if err != nil {
		t.Fatal(err)
	}
	report := check(t, packages)
	if report.constructions == 0 {
		t.Fatal("no error construction was checked; the loader found nothing")
	}
	for _, v := range report.violations {
		t.Error(v)
	}
}

// TestRuleCatchesEachViolation runs the check on a sample package whose
// violating lines carry a "// violation" comment, so a defect in the check
// that would let the rule pass vacuously fails here instead.
func TestRuleCatchesEachViolation(t *testing.T) {
	const sample = "internal/errprefix/testdata/sample"
	want, err := markedLines(filepath.Join(root, sample, "sample.go"))
	if err != nil {
		t.Fatal(err)
	}
	var got []int
	for _, v := range check(t, []string{sample}).found {
		got = append(got, v.line)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("violations on lines %v, want %v", got, want)
	}
}

// violation is one error construction that breaks the rule.
type violation struct {
	file    string
	line    int
	message string
}

func (v violation) String() string { return fmt.Sprintf("%s:%d: %s", v.file, v.line, v.message) }

// report is the outcome of a check across platforms.
type report struct {
	found         []violation
	violations    []string // found, deduplicated across platforms and sorted
	constructions int
}

// check loads and analyzes the packages once per platform.
func check(t *testing.T, packages []string) report {
	t.Helper()
	var r report
	seen := map[string]bool{}
	for _, goos := range platforms {
		l := newLoader(goos)
		for _, rel := range packages {
			if _, err := l.load(rel); err != nil && !isNoGo(err) {
				t.Fatalf("load %s for %s: %v", rel, goos, err)
			}
		}
		for _, u := range l.units {
			a := newAnalysis(l.fset, u)
			a.run()
			r.constructions += a.constructions
			for _, v := range a.found {
				if key := v.String(); !seen[key] {
					seen[key] = true
					r.found = append(r.found, v)
					r.violations = append(r.violations, key)
				}
			}
		}
	}
	slices.Sort(r.violations)
	return r
}

// packageDirs returns every package directory under lode/ and internal/,
// relative to the module root, except the exempt ones.
func packageDirs() ([]string, error) {
	var dirs []string
	for _, top := range []string{"lode", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if _, skip := exempt[rel]; skip || d.Name() == "testdata" {
				return filepath.SkipDir
			}
			dirs = append(dirs, rel)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return dirs, nil
}

func isNoGo(err error) bool {
	var noGo *build.NoGoError
	return errors.As(err, &noGo)
}

// markedLines returns the lines of a file that carry a "// violation"
// comment.
func markedLines(file string) ([]int, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var lines []int
	for i, line := range strings.Split(string(data), "\n") {
		if strings.HasSuffix(strings.TrimSpace(line), "// violation") {
			lines = append(lines, i+1)
		}
	}
	return lines, nil
}

// -----------------------------------------------------------------------------
// Loading
// -----------------------------------------------------------------------------

// loader type-checks Lode's packages from source for one platform. A
// package outside the module is replaced by an empty stub: a call into it
// then resolves to no object, which is how the analysis recognizes code
// outside Lode, and nothing beyond the module is compiled.
type loader struct {
	ctx   build.Context
	fset  *token.FileSet
	pkgs  map[string]*types.Package
	units []unit
}

// unit is one type-checked package.
type unit struct {
	rel   string
	files []*ast.File
	info  *types.Info
}

func newLoader(goos string) *loader {
	ctx := build.Default
	ctx.GOOS = goos
	ctx.CgoEnabled = false
	return &loader{ctx: ctx, fset: token.NewFileSet(), pkgs: map[string]*types.Package{}}
}

// Import implements types.Importer.
func (l *loader) Import(importPath string) (*types.Package, error) {
	if importPath == "unsafe" {
		return types.Unsafe, nil
	}
	if p, ok := l.pkgs[importPath]; ok {
		return p, nil
	}
	if rel, ok := strings.CutPrefix(importPath, module+"/"); ok {
		return l.load(rel)
	}
	stub := types.NewPackage(importPath, path.Base(importPath))
	stub.MarkComplete()
	l.pkgs[importPath] = stub
	return stub, nil
}

// load type-checks the package at rel, once.
func (l *loader) load(rel string) (*types.Package, error) {
	if p, ok := l.pkgs[module+"/"+rel]; ok {
		return p, nil
	}
	bp, err := l.ctx.ImportDir(filepath.Join(root, filepath.FromSlash(rel)), 0)
	if err != nil {
		return nil, err
	}
	files := make([]*ast.File, 0, len(bp.GoFiles))
	for _, name := range bp.GoFiles {
		f, err := parser.ParseFile(l.fset, filepath.Join(bp.Dir, name), nil, 0)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	info := &types.Info{
		Types:      map[ast.Expr]types.TypeAndValue{},
		Defs:       map[*ast.Ident]types.Object{},
		Uses:       map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
	// Type errors are expected and ignored: every package outside the module
	// is an empty stub, so expressions that use one do not type-check.
	conf := types.Config{Importer: l, Error: func(error) {}}
	pkg, _ := conf.Check(module+"/"+rel, l.fset, files, info)
	l.pkgs[module+"/"+rel] = pkg
	l.units = append(l.units, unit{rel: rel, files: files, info: info})
	return pkg, nil
}

// -----------------------------------------------------------------------------
// Analysis
// -----------------------------------------------------------------------------

// origin classifies an error a construction wraps.
type origin int

const (
	// fromSentinel is a sentinel: a package-level variable, or a field of a
	// sentinel set. It carries no prefix, so wrapping it originates an error.
	fromSentinel origin = iota
	// fromOutside is the result of a call into code outside Lode: the error
	// enters Lode here and gains its source prefix here.
	fromOutside
	// fromLode is the result of a call into Lode, or a value whose source the
	// analysis cannot see. It may already carry a prefix, so wrapping it must
	// not add one.
	fromLode
)

// analysis checks the error constructions of one package.
type analysis struct {
	fset    *token.FileSet
	unit    unit
	allowed []string
	// last maps a variable to the value most recently assigned to it before
	// the point the walk has reached; nil when no value is known.
	last map[*types.Var]ast.Expr
	// supplied holds the parameters of function literals passed to code
	// outside Lode, such as a filepath.WalkDir callback: that code supplies
	// their values, so an error among them comes from outside Lode.
	supplied      map[*types.Var]bool
	found         []violation
	constructions int
}

func newAnalysis(fset *token.FileSet, u unit) *analysis {
	allowed, ok := sources[u.rel]
	if !ok {
		allowed = defaultSources
	}
	return &analysis{
		fset: fset, unit: u, allowed: allowed,
		last: map[*types.Var]ast.Expr{}, supplied: map[*types.Var]bool{},
	}
}

func (a *analysis) run() {
	for _, f := range a.unit.files {
		for _, d := range f.Decls {
			a.decl(d)
		}
	}
}

// decl checks a top-level declaration. A package-level errors.New defines a
// sentinel, which carries no prefix; everything else is walked as code.
func (a *analysis) decl(d ast.Decl) {
	switch d := d.(type) {
	case *ast.FuncDecl:
		if d.Body != nil && !isOutsideFunc(a.unit.info.Defs[d.Name]) {
			ast.Inspect(d.Body, a.visit)
		}
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			if vs, ok := spec.(*ast.ValueSpec); ok {
				a.packageValues(vs)
			}
		}
	}
}

func (a *analysis) packageValues(vs *ast.ValueSpec) {
	for _, value := range vs.Values {
		call, ok := value.(*ast.CallExpr)
		if ok && a.constructor(call) == "errors.New" {
			a.sentinel(call)
			continue
		}
		ast.Inspect(value, a.visit)
	}
}

func (a *analysis) visit(n ast.Node) bool {
	switch n := n.(type) {
	case *ast.AssignStmt:
		a.assign(n.Lhs, n.Rhs)
		return false
	case *ast.ValueSpec:
		names := make([]ast.Expr, len(n.Names))
		for i, name := range n.Names {
			names[i] = name
		}
		a.assign(names, n.Values)
		return false
	case *ast.RangeStmt:
		a.record(n.Key, nil)
		a.record(n.Value, nil)
	case *ast.CallExpr:
		a.construction(n)
		a.callbacks(n)
	}
	return true
}

// callbacks records the parameters of function literals passed to a call
// into code outside Lode. The call is visited before its arguments, so the
// literals' bodies see the record.
func (a *analysis) callbacks(call *ast.CallExpr) {
	if a.callOrigin(call) != fromOutside {
		return
	}
	for _, arg := range call.Args {
		lit, ok := arg.(*ast.FuncLit)
		if !ok {
			continue
		}
		for _, field := range lit.Type.Params.List {
			for _, name := range field.Names {
				if v, ok := a.unit.info.Defs[name].(*types.Var); ok {
					a.supplied[v] = true
				}
			}
		}
	}
}

// assign walks the assigned values before recording them, so that a
// construction on the right-hand side sees the variables as they were.
func (a *analysis) assign(lhs, rhs []ast.Expr) {
	for _, e := range rhs {
		ast.Inspect(e, a.visit)
	}
	for i, e := range lhs {
		var value ast.Expr
		switch {
		case len(rhs) == len(lhs):
			value = rhs[i]
		case len(rhs) == 1:
			value = rhs[0]
		}
		a.record(e, value)
	}
}

func (a *analysis) record(e, value ast.Expr) {
	id, ok := e.(*ast.Ident)
	if !ok {
		return
	}
	obj := a.unit.info.Defs[id]
	if obj == nil {
		obj = a.unit.info.Uses[id]
	}
	if v, ok := obj.(*types.Var); ok {
		a.last[v] = value
	}
}

// constructor names the function a call invokes when it is a package-level
// function of an imported package, such as "fmt.Errorf"; otherwise "".
func (a *analysis) constructor(call *ast.CallExpr) string {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	x, ok := sel.X.(*ast.Ident)
	if !ok {
		return ""
	}
	pkg, ok := a.unit.info.Uses[x].(*types.PkgName)
	if !ok {
		return ""
	}
	return pkg.Imported().Path() + "." + sel.Sel.Name
}

// sentinel checks a package-level errors.New: a sentinel is matched, not
// read, and carries no prefix.
func (a *analysis) sentinel(call *ast.CallExpr) {
	a.constructions++
	format, ok := a.message(call)
	if !ok {
		return
	}
	// Any leading "word: " is refused, not only a known source: a sentinel
	// has no context to precede its text, so such a word can only be a prefix.
	if p := leadingWord(format); p != "" {
		a.report(call, fmt.Sprintf("a sentinel carries no prefix, but this one begins with %q: %q", p, format))
	}
}

// construction checks a call to fmt.Errorf or errors.New inside code.
func (a *analysis) construction(call *ast.CallExpr) {
	switch a.constructor(call) {
	case "fmt.Errorf", "errors.New":
	default:
		return
	}
	a.constructions++
	format, ok := a.message(call)
	if !ok {
		return
	}
	operands, err := wrapped(format, call.Args[1:])
	if err != nil {
		a.report(call, err.Error())
		return
	}
	prefix := leadingWord(format)
	if a.originates(operands) {
		if !slices.Contains(a.allowed, prefix) {
			a.report(call, fmt.Sprintf("an error that originates here begins with one of %v: %q", a.allowed, format))
		}
		return
	}
	if p := knownPrefix(format); p != "" {
		a.report(call, fmt.Sprintf("an error that wraps a Lode error adds context, not the source prefix %q: %q", p, format))
	}
}

// originates reports whether a construction creates a new error: it wraps
// nothing, or only sentinels and errors from outside Lode.
func (a *analysis) originates(operands []ast.Expr) bool {
	for _, op := range operands {
		if a.origin(op) == fromLode {
			return false
		}
	}
	return true
}

// message returns the constant message of a construction, reporting one
// that is not constant: the rule cannot be checked on it.
func (a *analysis) message(call *ast.CallExpr) (string, bool) {
	if len(call.Args) > 0 {
		if tv := a.unit.info.Types[call.Args[0]]; tv.Value != nil && tv.Value.Kind() == constant.String {
			return constant.StringVal(tv.Value), true
		}
	}
	a.report(call, "the message is not a constant string, so its prefix cannot be checked")
	return "", false
}

func (a *analysis) report(call *ast.CallExpr, message string) {
	pos := a.fset.Position(call.Pos())
	file := filepath.ToSlash(strings.TrimPrefix(pos.Filename, root+string(filepath.Separator)))
	a.found = append(a.found, violation{file: file, line: pos.Line, message: message})
}

func (a *analysis) origin(e ast.Expr) origin {
	switch e := ast.Unparen(e).(type) {
	case *ast.CallExpr:
		return a.callOrigin(e)
	case *ast.Ident:
		return a.varOrigin(a.unit.info.Uses[e])
	case *ast.SelectorExpr:
		return a.selectorOrigin(e)
	}
	return fromLode
}

func (a *analysis) varOrigin(obj types.Object) origin {
	return a.traceVar(obj, map[*types.Var]bool{})
}

// traceVar follows a variable to the value last assigned to it; seen ends
// a chain of copies that returns to a variable already followed.
func (a *analysis) traceVar(obj types.Object, seen map[*types.Var]bool) origin {
	v, ok := obj.(*types.Var)
	if !ok || seen[v] {
		return fromLode
	}
	seen[v] = true
	switch {
	case v.Pkg() != nil && v.Parent() == v.Pkg().Scope():
		return fromSentinel
	case a.supplied[v]:
		return fromOutside
	}
	switch value := ast.Unparen(a.last[v]).(type) {
	case *ast.CallExpr:
		return a.callOrigin(value)
	case *ast.Ident:
		// A copy of another variable, such as err = closeErr, has its origin.
		return a.traceVar(a.unit.info.Uses[value], seen)
	}
	return fromLode
}

func (a *analysis) selectorOrigin(e *ast.SelectorExpr) origin {
	if sel, ok := a.unit.info.Selections[e]; ok {
		if _, set := sentinelSets[typeName(sel.Recv())]; set && sel.Kind() == types.FieldVal {
			return fromSentinel
		}
		return fromLode
	}
	// A qualified identifier: a variable of an imported package. One from a
	// stub package does not resolve; its name says whether it is a sentinel.
	if obj := a.unit.info.Uses[e.Sel]; obj != nil {
		return a.varOrigin(obj)
	}
	if strings.HasPrefix(e.Sel.Name, "Err") {
		return fromSentinel
	}
	return fromLode
}

func (a *analysis) callOrigin(call *ast.CallExpr) origin {
	// An error built here is checked as its own construction and so already
	// carries its prefix; joining errors adds none of its own.
	if _, built := lodeBuilders[a.constructor(call)]; built {
		return fromLode
	}
	obj := a.callee(call.Fun)
	if obj == nil || obj.Pkg() == nil || a.standsOutside(call.Fun) {
		return fromOutside
	}
	if _, inLode := strings.CutPrefix(obj.Pkg().Path(), module+"/"); inLode {
		return fromLode
	}
	return fromOutside
}

func (a *analysis) callee(fun ast.Expr) types.Object {
	switch f := ast.Unparen(fun).(type) {
	case *ast.Ident:
		return a.unit.info.Uses[f]
	case *ast.SelectorExpr:
		if sel, ok := a.unit.info.Selections[f]; ok {
			return sel.Obj()
		}
		return a.unit.info.Uses[f.Sel]
	case *ast.IndexExpr:
		return a.callee(f.X)
	case *ast.IndexListExpr:
		return a.callee(f.X)
	}
	return nil
}

// standsOutside reports whether a callee is listed in outsideCallees.
func (a *analysis) standsOutside(fun ast.Expr) bool {
	switch f := ast.Unparen(fun).(type) {
	case *ast.Ident:
		return isOutsideFunc(a.unit.info.Uses[f])
	case *ast.SelectorExpr:
		sel, ok := a.unit.info.Selections[f]
		if !ok {
			return isOutsideFunc(a.unit.info.Uses[f.Sel])
		}
		name := typeName(sel.Recv())
		_, iface := outsideCallees[name]
		_, member := outsideCallees[name+"."+f.Sel.Name]
		return name != "" && (iface || member)
	}
	return false
}

// isOutsideFunc reports whether obj is a package function listed in
// outsideCallees.
func isOutsideFunc(obj types.Object) bool {
	f, ok := obj.(*types.Func)
	if !ok || f.Pkg() == nil || f.Signature().Recv() != nil {
		return false
	}
	_, ok = outsideCallees[relative(f.Pkg())+"."+f.Name()]
	return ok
}

// typeName returns "<package relative to the module>.<name>" for a named
// type or a pointer to one; otherwise "".
func typeName(t types.Type) string {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return ""
	}
	return relative(named.Obj().Pkg()) + "." + named.Obj().Name()
}

func relative(p *types.Package) string {
	return strings.TrimPrefix(p.Path(), module+"/")
}

// leadingWord returns the word before a message's first ": ", when the
// message begins with one.
func leadingWord(format string) string {
	if m := prefixPattern.FindStringSubmatch(format); m != nil {
		return m[1]
	}
	return ""
}

// knownPrefix returns the leading word of a message when it is a source
// prefix of any package; otherwise "".
func knownPrefix(format string) string {
	word := leadingWord(format)
	if slices.Contains(defaultSources, word) {
		return word
	}
	for _, names := range sources {
		if slices.Contains(names, word) {
			return word
		}
	}
	return ""
}

// wrapped returns the arguments that a format's %w verbs consume.
func wrapped(format string, args []ast.Expr) ([]ast.Expr, error) {
	verbs, err := verbsOf(format)
	if err != nil {
		return nil, err
	}
	if len(verbs) > len(args) {
		return nil, fmt.Errorf("the message has %d verbs and %d arguments", len(verbs), len(args))
	}
	var operands []ast.Expr
	for i, verb := range verbs {
		if verb == 'w' {
			operands = append(operands, args[i])
		}
	}
	return operands, nil
}

// verbsOf returns the verbs of a format in order, one per argument.
func verbsOf(format string) ([]byte, error) {
	var verbs []byte
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		i++
		for i < len(format) && strings.IndexByte("+-# 0123456789.", format[i]) >= 0 {
			i++
		}
		if i >= len(format) {
			return nil, errors.New("the message ends inside a verb")
		}
		switch format[i] {
		case '%':
			continue
		case '[', '*':
			return nil, errors.New("the check does not read indexed or starred verbs")
		}
		verbs = append(verbs, format[i])
	}
	return verbs, nil
}
