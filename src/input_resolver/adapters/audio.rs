//! Audio adapters — WAV, MP3, FLAC, etc.

use crate::input_resolver::adapter::MediaAdapter;

pub struct WavAdapter;
impl MediaAdapter for WavAdapter {
    fn name(&self) -> &'static str {
        "wav"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:wav"
    }
}

pub struct Mp3Adapter;
impl MediaAdapter for Mp3Adapter {
    fn name(&self) -> &'static str {
        "mp3"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:mp3"
    }
}

pub struct FlacAdapter;
impl MediaAdapter for FlacAdapter {
    fn name(&self) -> &'static str {
        "flac"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:flac"
    }
}

pub struct AacAdapter;
impl MediaAdapter for AacAdapter {
    fn name(&self) -> &'static str {
        "aac"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:aac"
    }
}

pub struct OggAdapter;
impl MediaAdapter for OggAdapter {
    fn name(&self) -> &'static str {
        "ogg"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:ogg"
    }
}

pub struct OpusAdapter;
impl MediaAdapter for OpusAdapter {
    fn name(&self) -> &'static str {
        "opus"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:opus"
    }
}

pub struct AiffAdapter;
impl MediaAdapter for AiffAdapter {
    fn name(&self) -> &'static str {
        "aiff"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:aiff"
    }
}

pub struct MidiAdapter;
impl MediaAdapter for MidiAdapter {
    fn name(&self) -> &'static str {
        "midi"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:midi"
    }
}

pub struct WmaAdapter;
impl MediaAdapter for WmaAdapter {
    fn name(&self) -> &'static str {
        "wma"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:wma"
    }
}

pub struct CafAdapter;
impl MediaAdapter for CafAdapter {
    fn name(&self) -> &'static str {
        "caf"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:caf"
    }
}
