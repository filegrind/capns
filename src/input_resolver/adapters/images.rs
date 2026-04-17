//! Image adapters — PNG, JPEG, GIF, WebP, etc.

use crate::input_resolver::adapter::MediaAdapter;

pub struct PngAdapter;
impl MediaAdapter for PngAdapter {
    fn name(&self) -> &'static str { "png" }
    fn pattern_urn(&self) -> &'static str { "media:png" }
}

pub struct JpegAdapter;
impl MediaAdapter for JpegAdapter {
    fn name(&self) -> &'static str { "jpeg" }
    fn pattern_urn(&self) -> &'static str { "media:jpeg" }
}

pub struct GifAdapter;
impl MediaAdapter for GifAdapter {
    fn name(&self) -> &'static str { "gif" }
    fn pattern_urn(&self) -> &'static str { "media:gif" }
}

pub struct WebpAdapter;
impl MediaAdapter for WebpAdapter {
    fn name(&self) -> &'static str { "webp" }
    fn pattern_urn(&self) -> &'static str { "media:webp" }
}

pub struct AvifAdapter;
impl MediaAdapter for AvifAdapter {
    fn name(&self) -> &'static str { "avif" }
    fn pattern_urn(&self) -> &'static str { "media:avif" }
}

pub struct HeicAdapter;
impl MediaAdapter for HeicAdapter {
    fn name(&self) -> &'static str { "heic" }
    fn pattern_urn(&self) -> &'static str { "media:heic" }
}

pub struct TiffAdapter;
impl MediaAdapter for TiffAdapter {
    fn name(&self) -> &'static str { "tiff" }
    fn pattern_urn(&self) -> &'static str { "media:tiff" }
}

pub struct BmpAdapter;
impl MediaAdapter for BmpAdapter {
    fn name(&self) -> &'static str { "bmp" }
    fn pattern_urn(&self) -> &'static str { "media:bmp" }
}

pub struct IcoAdapter;
impl MediaAdapter for IcoAdapter {
    fn name(&self) -> &'static str { "ico" }
    fn pattern_urn(&self) -> &'static str { "media:ico" }
}

pub struct SvgAdapter;
impl MediaAdapter for SvgAdapter {
    fn name(&self) -> &'static str { "svg" }
    fn pattern_urn(&self) -> &'static str { "media:svg" }
}

pub struct PsdAdapter;
impl MediaAdapter for PsdAdapter {
    fn name(&self) -> &'static str { "psd" }
    fn pattern_urn(&self) -> &'static str { "media:psd" }
}

pub struct RawImageAdapter;
impl MediaAdapter for RawImageAdapter {
    fn name(&self) -> &'static str { "raw" }
    fn pattern_urn(&self) -> &'static str { "media:raw" }
}

pub struct EpsAdapter;
impl MediaAdapter for EpsAdapter {
    fn name(&self) -> &'static str { "eps" }
    fn pattern_urn(&self) -> &'static str { "media:eps" }
}

pub struct HdrAdapter;
impl MediaAdapter for HdrAdapter {
    fn name(&self) -> &'static str { "hdr" }
    fn pattern_urn(&self) -> &'static str { "media:hdr" }
}
