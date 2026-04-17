//! Source code adapters — Rust, Python, JavaScript, etc.

use crate::input_resolver::adapter::MediaAdapter;

pub struct RustAdapter;
impl MediaAdapter for RustAdapter {
    fn name(&self) -> &'static str { "rust" }
    fn pattern_urn(&self) -> &'static str { "media:rust" }
}

pub struct PythonAdapter;
impl MediaAdapter for PythonAdapter {
    fn name(&self) -> &'static str { "python" }
    fn pattern_urn(&self) -> &'static str { "media:python" }
}

pub struct JavaScriptAdapter;
impl MediaAdapter for JavaScriptAdapter {
    fn name(&self) -> &'static str { "javascript" }
    fn pattern_urn(&self) -> &'static str { "media:javascript" }
}

pub struct TypeScriptAdapter;
impl MediaAdapter for TypeScriptAdapter {
    fn name(&self) -> &'static str { "typescript" }
    fn pattern_urn(&self) -> &'static str { "media:typescript" }
}

pub struct GoAdapter;
impl MediaAdapter for GoAdapter {
    fn name(&self) -> &'static str { "go" }
    fn pattern_urn(&self) -> &'static str { "media:go" }
}

pub struct JavaAdapter;
impl MediaAdapter for JavaAdapter {
    fn name(&self) -> &'static str { "java" }
    fn pattern_urn(&self) -> &'static str { "media:java" }
}

pub struct KotlinAdapter;
impl MediaAdapter for KotlinAdapter {
    fn name(&self) -> &'static str { "kotlin" }
    fn pattern_urn(&self) -> &'static str { "media:kotlin" }
}

pub struct CAdapter;
impl MediaAdapter for CAdapter {
    fn name(&self) -> &'static str { "c" }
    fn pattern_urn(&self) -> &'static str { "media:c" }
}

pub struct CppAdapter;
impl MediaAdapter for CppAdapter {
    fn name(&self) -> &'static str { "cpp" }
    fn pattern_urn(&self) -> &'static str { "media:cpp" }
}

pub struct SwiftAdapter;
impl MediaAdapter for SwiftAdapter {
    fn name(&self) -> &'static str { "swift" }
    fn pattern_urn(&self) -> &'static str { "media:swift" }
}

pub struct ObjCAdapter;
impl MediaAdapter for ObjCAdapter {
    fn name(&self) -> &'static str { "objc" }
    fn pattern_urn(&self) -> &'static str { "media:objc" }
}

pub struct RubyAdapter;
impl MediaAdapter for RubyAdapter {
    fn name(&self) -> &'static str { "ruby" }
    fn pattern_urn(&self) -> &'static str { "media:ruby" }
}

pub struct PhpAdapter;
impl MediaAdapter for PhpAdapter {
    fn name(&self) -> &'static str { "php" }
    fn pattern_urn(&self) -> &'static str { "media:php" }
}

pub struct ShellAdapter;
impl MediaAdapter for ShellAdapter {
    fn name(&self) -> &'static str { "shell" }
    fn pattern_urn(&self) -> &'static str { "media:shell" }
}

pub struct SqlAdapter;
impl MediaAdapter for SqlAdapter {
    fn name(&self) -> &'static str { "sql" }
    fn pattern_urn(&self) -> &'static str { "media:sql" }
}

pub struct PerlAdapter;
impl MediaAdapter for PerlAdapter {
    fn name(&self) -> &'static str { "perl" }
    fn pattern_urn(&self) -> &'static str { "media:perl" }
}

pub struct LuaAdapter;
impl MediaAdapter for LuaAdapter {
    fn name(&self) -> &'static str { "lua" }
    fn pattern_urn(&self) -> &'static str { "media:lua" }
}

pub struct ScalaAdapter;
impl MediaAdapter for ScalaAdapter {
    fn name(&self) -> &'static str { "scala" }
    fn pattern_urn(&self) -> &'static str { "media:scala" }
}

pub struct RLangAdapter;
impl MediaAdapter for RLangAdapter {
    fn name(&self) -> &'static str { "r" }
    fn pattern_urn(&self) -> &'static str { "media:r" }
}

pub struct JuliaAdapter;
impl MediaAdapter for JuliaAdapter {
    fn name(&self) -> &'static str { "julia" }
    fn pattern_urn(&self) -> &'static str { "media:julia" }
}

pub struct HaskellAdapter;
impl MediaAdapter for HaskellAdapter {
    fn name(&self) -> &'static str { "haskell" }
    fn pattern_urn(&self) -> &'static str { "media:haskell" }
}

pub struct ElixirAdapter;
impl MediaAdapter for ElixirAdapter {
    fn name(&self) -> &'static str { "elixir" }
    fn pattern_urn(&self) -> &'static str { "media:elixir" }
}

pub struct ErlangAdapter;
impl MediaAdapter for ErlangAdapter {
    fn name(&self) -> &'static str { "erlang" }
    fn pattern_urn(&self) -> &'static str { "media:erlang" }
}

pub struct ClojureAdapter;
impl MediaAdapter for ClojureAdapter {
    fn name(&self) -> &'static str { "clojure" }
    fn pattern_urn(&self) -> &'static str { "media:clojure" }
}

pub struct CSharpAdapter;
impl MediaAdapter for CSharpAdapter {
    fn name(&self) -> &'static str { "csharp" }
    fn pattern_urn(&self) -> &'static str { "media:csharp" }
}

pub struct VbAdapter;
impl MediaAdapter for VbAdapter {
    fn name(&self) -> &'static str { "vb" }
    fn pattern_urn(&self) -> &'static str { "media:vb" }
}

pub struct DartAdapter;
impl MediaAdapter for DartAdapter {
    fn name(&self) -> &'static str { "dart" }
    fn pattern_urn(&self) -> &'static str { "media:dart" }
}

pub struct VueAdapter;
impl MediaAdapter for VueAdapter {
    fn name(&self) -> &'static str { "vue" }
    fn pattern_urn(&self) -> &'static str { "media:vue" }
}

pub struct SvelteAdapter;
impl MediaAdapter for SvelteAdapter {
    fn name(&self) -> &'static str { "svelte" }
    fn pattern_urn(&self) -> &'static str { "media:svelte" }
}

pub struct ZigAdapter;
impl MediaAdapter for ZigAdapter {
    fn name(&self) -> &'static str { "zig" }
    fn pattern_urn(&self) -> &'static str { "media:zig" }
}

pub struct NimAdapter;
impl MediaAdapter for NimAdapter {
    fn name(&self) -> &'static str { "nim" }
    fn pattern_urn(&self) -> &'static str { "media:nim" }
}

pub struct PowerShellAdapter;
impl MediaAdapter for PowerShellAdapter {
    fn name(&self) -> &'static str { "powershell" }
    fn pattern_urn(&self) -> &'static str { "media:powershell" }
}

pub struct BatchAdapter;
impl MediaAdapter for BatchAdapter {
    fn name(&self) -> &'static str { "batch" }
    fn pattern_urn(&self) -> &'static str { "media:batch" }
}

pub struct MakefileAdapter;
impl MediaAdapter for MakefileAdapter {
    fn name(&self) -> &'static str { "makefile" }
    fn pattern_urn(&self) -> &'static str { "media:makefile" }
}

pub struct DockerfileAdapter;
impl MediaAdapter for DockerfileAdapter {
    fn name(&self) -> &'static str { "dockerfile" }
    fn pattern_urn(&self) -> &'static str { "media:dockerfile" }
}

pub struct CMakeAdapter;
impl MediaAdapter for CMakeAdapter {
    fn name(&self) -> &'static str { "cmake" }
    fn pattern_urn(&self) -> &'static str { "media:cmake" }
}

pub struct DotAdapter;
impl MediaAdapter for DotAdapter {
    fn name(&self) -> &'static str { "dot" }
    fn pattern_urn(&self) -> &'static str { "media:dot" }
}
