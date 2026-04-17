//! Video adapters — MP4, WebM, MKV, etc.

use crate::input_resolver::adapter::MediaAdapter;

pub struct Mp4Adapter;
impl MediaAdapter for Mp4Adapter {
    fn name(&self) -> &'static str { "mp4" }
    fn pattern_urn(&self) -> &'static str { "media:mp4" }
}

pub struct WebmAdapter;
impl MediaAdapter for WebmAdapter {
    fn name(&self) -> &'static str { "webm" }
    fn pattern_urn(&self) -> &'static str { "media:webm" }
}

pub struct MkvAdapter;
impl MediaAdapter for MkvAdapter {
    fn name(&self) -> &'static str { "mkv" }
    fn pattern_urn(&self) -> &'static str { "media:mkv" }
}

pub struct MovAdapter;
impl MediaAdapter for MovAdapter {
    fn name(&self) -> &'static str { "mov" }
    fn pattern_urn(&self) -> &'static str { "media:mov" }
}

pub struct AviAdapter;
impl MediaAdapter for AviAdapter {
    fn name(&self) -> &'static str { "avi" }
    fn pattern_urn(&self) -> &'static str { "media:avi" }
}

pub struct MpegAdapter;
impl MediaAdapter for MpegAdapter {
    fn name(&self) -> &'static str { "mpeg" }
    fn pattern_urn(&self) -> &'static str { "media:mpeg" }
}

pub struct WmvAdapter;
impl MediaAdapter for WmvAdapter {
    fn name(&self) -> &'static str { "wmv" }
    fn pattern_urn(&self) -> &'static str { "media:wmv" }
}

pub struct FlvAdapter;
impl MediaAdapter for FlvAdapter {
    fn name(&self) -> &'static str { "flv" }
    fn pattern_urn(&self) -> &'static str { "media:flv" }
}

pub struct ThreeGpAdapter;
impl MediaAdapter for ThreeGpAdapter {
    fn name(&self) -> &'static str { "3gp" }
    fn pattern_urn(&self) -> &'static str { "media:3gp" }
}

pub struct OgvAdapter;
impl MediaAdapter for OgvAdapter {
    fn name(&self) -> &'static str { "ogv" }
    fn pattern_urn(&self) -> &'static str { "media:ogv" }
}

pub struct TsAdapter;
impl MediaAdapter for TsAdapter {
    fn name(&self) -> &'static str { "ts-video" }
    fn pattern_urn(&self) -> &'static str { "media:ts-video" }
}
