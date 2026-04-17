//! Other file type adapters — fonts, 3D models, ML models, databases, etc.

use crate::input_resolver::adapter::MediaAdapter;

pub struct FontAdapter;
impl MediaAdapter for FontAdapter {
    fn name(&self) -> &'static str { "font" }
    fn pattern_urn(&self) -> &'static str { "media:font" }
}

pub struct Model3DAdapter;
impl MediaAdapter for Model3DAdapter {
    fn name(&self) -> &'static str { "3d-model" }
    fn pattern_urn(&self) -> &'static str { "media:3d-model" }
}

pub struct MlModelAdapter;
impl MediaAdapter for MlModelAdapter {
    fn name(&self) -> &'static str { "ml-model" }
    fn pattern_urn(&self) -> &'static str { "media:ml-model" }
}

pub struct DatabaseAdapter;
impl MediaAdapter for DatabaseAdapter {
    fn name(&self) -> &'static str { "database" }
    fn pattern_urn(&self) -> &'static str { "media:database" }
}

pub struct ColumnarDataAdapter;
impl MediaAdapter for ColumnarDataAdapter {
    fn name(&self) -> &'static str { "columnar" }
    fn pattern_urn(&self) -> &'static str { "media:columnar" }
}

pub struct CertificateAdapter;
impl MediaAdapter for CertificateAdapter {
    fn name(&self) -> &'static str { "certificate" }
    fn pattern_urn(&self) -> &'static str { "media:certificate" }
}

pub struct GpgAdapter;
impl MediaAdapter for GpgAdapter {
    fn name(&self) -> &'static str { "gpg" }
    fn pattern_urn(&self) -> &'static str { "media:gpg" }
}

pub struct GeoAdapter;
impl MediaAdapter for GeoAdapter {
    fn name(&self) -> &'static str { "geo" }
    fn pattern_urn(&self) -> &'static str { "media:geo" }
}

pub struct SubtitleAdapter;
impl MediaAdapter for SubtitleAdapter {
    fn name(&self) -> &'static str { "subtitle" }
    fn pattern_urn(&self) -> &'static str { "media:subtitle" }
}

pub struct EmailAdapter;
impl MediaAdapter for EmailAdapter {
    fn name(&self) -> &'static str { "email" }
    fn pattern_urn(&self) -> &'static str { "media:email" }
}

pub struct JupyterAdapter;
impl MediaAdapter for JupyterAdapter {
    fn name(&self) -> &'static str { "jupyter" }
    fn pattern_urn(&self) -> &'static str { "media:jupyter" }
}

pub struct BinarySerializationAdapter;
impl MediaAdapter for BinarySerializationAdapter {
    fn name(&self) -> &'static str { "binary-serialization" }
    fn pattern_urn(&self) -> &'static str { "media:binary-serialization" }
}

pub struct ScientificAdapter;
impl MediaAdapter for ScientificAdapter {
    fn name(&self) -> &'static str { "scientific" }
    fn pattern_urn(&self) -> &'static str { "media:scientific" }
}

pub struct WasmAdapter;
impl MediaAdapter for WasmAdapter {
    fn name(&self) -> &'static str { "wasm" }
    fn pattern_urn(&self) -> &'static str { "media:wasm" }
}

pub struct FallbackAdapter;
impl MediaAdapter for FallbackAdapter {
    fn name(&self) -> &'static str { "fallback" }
    fn pattern_urn(&self) -> &'static str { "media:" }
}
