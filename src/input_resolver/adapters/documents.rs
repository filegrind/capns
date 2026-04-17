//! Document adapters — PDF, EPUB, Office documents, etc.

use crate::input_resolver::adapter::MediaAdapter;

/// PDF document adapter
pub struct PdfAdapter;

impl MediaAdapter for PdfAdapter {
    fn name(&self) -> &'static str {
        "pdf"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:pdf"
    }
}

/// EPUB ebook adapter
pub struct EpubAdapter;

impl MediaAdapter for EpubAdapter {
    fn name(&self) -> &'static str {
        "epub"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:epub"
    }
}

/// Kindle MOBI/AZW adapter
pub struct MobiAdapter;

impl MediaAdapter for MobiAdapter {
    fn name(&self) -> &'static str {
        "mobi"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:mobi"
    }
}

/// DjVu document adapter
pub struct DjvuAdapter;

impl MediaAdapter for DjvuAdapter {
    fn name(&self) -> &'static str {
        "djvu"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:djvu"
    }
}

/// Microsoft Word adapter (doc, docx)
pub struct DocAdapter;

impl MediaAdapter for DocAdapter {
    fn name(&self) -> &'static str {
        "doc"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:doc"
    }
}

/// Microsoft Word OOXML adapter
pub struct DocxAdapter;

impl MediaAdapter for DocxAdapter {
    fn name(&self) -> &'static str {
        "docx"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:docx"
    }
}

/// Microsoft Excel adapter
pub struct XlsAdapter;

impl MediaAdapter for XlsAdapter {
    fn name(&self) -> &'static str {
        "xls"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:xls"
    }
}

/// Microsoft Excel OOXML adapter
pub struct XlsxAdapter;

impl MediaAdapter for XlsxAdapter {
    fn name(&self) -> &'static str {
        "xlsx"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:xlsx"
    }
}

/// Microsoft PowerPoint adapter
pub struct PptAdapter;

impl MediaAdapter for PptAdapter {
    fn name(&self) -> &'static str {
        "ppt"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:ppt"
    }
}

/// Microsoft PowerPoint OOXML adapter
pub struct PptxAdapter;

impl MediaAdapter for PptxAdapter {
    fn name(&self) -> &'static str {
        "pptx"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:pptx"
    }
}

/// OpenDocument Text adapter
pub struct OdtAdapter;

impl MediaAdapter for OdtAdapter {
    fn name(&self) -> &'static str {
        "odt"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:odt"
    }
}

/// OpenDocument Spreadsheet adapter
pub struct OdsAdapter;

impl MediaAdapter for OdsAdapter {
    fn name(&self) -> &'static str {
        "ods"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:ods"
    }
}

/// OpenDocument Presentation adapter
pub struct OdpAdapter;

impl MediaAdapter for OdpAdapter {
    fn name(&self) -> &'static str {
        "odp"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:odp"
    }
}

/// Apple Pages adapter
pub struct PagesAdapter;

impl MediaAdapter for PagesAdapter {
    fn name(&self) -> &'static str {
        "pages"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:pages"
    }
}

/// Apple Numbers adapter
pub struct NumbersAdapter;

impl MediaAdapter for NumbersAdapter {
    fn name(&self) -> &'static str {
        "numbers"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:numbers"
    }
}

/// Apple Keynote adapter
pub struct KeynoteAdapter;

impl MediaAdapter for KeynoteAdapter {
    fn name(&self) -> &'static str {
        "keynote"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:keynote"
    }
}

/// Rich Text Format adapter
pub struct RtfAdapter;

impl MediaAdapter for RtfAdapter {
    fn name(&self) -> &'static str {
        "rtf"
    }
    fn pattern_urn(&self) -> &'static str {
        "media:rtf"
    }
}
