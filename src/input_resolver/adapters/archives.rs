//! Archive adapters — ZIP, TAR, GZIP, etc.

use crate::input_resolver::adapter::MediaAdapter;

pub struct ZipAdapter;
impl MediaAdapter for ZipAdapter {
    fn name(&self) -> &'static str {
        "zip"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:zip"
    }
}

pub struct TarAdapter;
impl MediaAdapter for TarAdapter {
    fn name(&self) -> &'static str {
        "tar"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:tar"
    }
}

pub struct GzipAdapter;
impl MediaAdapter for GzipAdapter {
    fn name(&self) -> &'static str {
        "gzip"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:gz"
    }
}

pub struct Bzip2Adapter;
impl MediaAdapter for Bzip2Adapter {
    fn name(&self) -> &'static str {
        "bzip2"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:bz2"
    }
}

pub struct XzAdapter;
impl MediaAdapter for XzAdapter {
    fn name(&self) -> &'static str {
        "xz"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:xz"
    }
}

pub struct ZstdAdapter;
impl MediaAdapter for ZstdAdapter {
    fn name(&self) -> &'static str {
        "zstd"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:zst"
    }
}

pub struct SevenZipAdapter;
impl MediaAdapter for SevenZipAdapter {
    fn name(&self) -> &'static str {
        "7z"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:7z"
    }
}

pub struct RarAdapter;
impl MediaAdapter for RarAdapter {
    fn name(&self) -> &'static str {
        "rar"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:rar"
    }
}

pub struct JarAdapter;
impl MediaAdapter for JarAdapter {
    fn name(&self) -> &'static str {
        "jar"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:jar"
    }
}

pub struct MobileAppAdapter;
impl MediaAdapter for MobileAppAdapter {
    fn name(&self) -> &'static str {
        "mobile-app"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:mobile-app"
    }
}

pub struct DiskImageAdapter;
impl MediaAdapter for DiskImageAdapter {
    fn name(&self) -> &'static str {
        "disk-image"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:disk-image"
    }
}

pub struct PackageAdapter;
impl MediaAdapter for PackageAdapter {
    fn name(&self) -> &'static str {
        "package"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:package"
    }
}
