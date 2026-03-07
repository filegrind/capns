//! Real-world multi-cartridge chain tests for capdag orchestrator
//!
//! Unlike the testcartridge integration tests (which use synthetic test caps),
//! these tests exercise real cartridges (pdfcartridge, txtcartridge, modelcartridge,
//! candlecartridge, ggufcartridge) through multi-step pipelines with real input data.
//!
//! Prerequisites:
//! - Cartridge binaries will be auto-built if missing or outdated
//! - ML-dependent tests require pre-downloaded models

use capdag::{Cap, CapUrn, CapUrnBuilder};
use capdag::orchestrator::{
    execute_dag, NodeData,
    parse_dot_to_cap_dag, CapRegistryTrait, ParseOrchestrationError,
};
use serial_test::serial;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

// =============================================================================
// Cap URN Builders — mirror the exact builder calls in each cartridge
// =============================================================================

// -- pdfcartridge caps (matches standard/caps.rs helpers with MEDIA_PDF input) --

fn pdf_generate_thumbnail() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_thumbnail")
        .in_spec("media:pdf")
        .out_spec("media:image;png;thumbnail")
        .build()
        .expect("pdf generate_thumbnail URN")
}

fn pdf_extract_metadata() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_metadata")
        .in_spec("media:pdf")
        .out_spec("media:file-metadata;textable;record")
        .build()
        .expect("pdf extract_metadata URN")
}

fn pdf_disbind() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "disbind")
        .in_spec("media:pdf")
        .out_spec("media:disbound-page;textable;list")
        .build()
        .expect("pdf disbind URN")
}

fn pdf_extract_outline() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_outline")
        .in_spec("media:pdf")
        .out_spec("media:document-outline;textable;record")
        .build()
        .expect("pdf extract_outline URN")
}

// -- txtcartridge caps (matches standard/caps.rs helpers with MEDIA_MD input) --

fn md_generate_thumbnail() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_thumbnail")
        .in_spec("media:md;textable")
        .out_spec("media:image;png;thumbnail")
        .build()
        .expect("md generate_thumbnail URN")
}

fn md_extract_metadata() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_metadata")
        .in_spec("media:md;textable")
        .out_spec("media:file-metadata;textable;record")
        .build()
        .expect("md extract_metadata URN")
}

fn md_extract_outline() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_outline")
        .in_spec("media:md;textable")
        .out_spec("media:document-outline;textable;record")
        .build()
        .expect("md extract_outline URN")
}

// -- candlecartridge caps (matches candlecartridge/src/main.rs builders) --

fn candle_text_embeddings() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_embeddings")
        .solo_tag("ml-model")
        .solo_tag("candle")
        .in_spec("media:textable")
        .out_spec("media:embedding-vector;textable;record")
        .build()
        .expect("candle text embeddings URN")
}

fn candle_embeddings_dimensions() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "embeddings_dimensions")
        .solo_tag("ml-model")
        .solo_tag("candle")
        .in_spec("media:model-spec;textable")
        .out_spec("media:model-dim;integer;textable;numeric")
        .build()
        .expect("candle embeddings_dimensions URN")
}

fn candle_image_embeddings() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_image_embeddings")
        .solo_tag("ml-model")
        .solo_tag("candle")
        .in_spec("media:image;png")  // no bytes tag — retired
        .out_spec("media:embedding-vector;textable;record")
        .build()
        .expect("candle image embeddings URN")
}

fn candle_describe_image() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "describe_image")
        .solo_tag("ml-model")
        .solo_tag("candle")
        .in_spec("media:image;png")
        .out_spec("media:image-description;textable")
        .build()
        .expect("candle describe_image URN")
}

fn candle_transcribe() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "transcribe")
        .solo_tag("ml-model")
        .solo_tag("candle")
        .in_spec("media:audio;wav;speech")  // no bytes tag — retired
        .out_spec("media:transcription;textable;record")
        .build()
        .expect("candle transcribe URN")
}

// -- modelcartridge caps (matches modelcartridge/src/main.rs) --

fn model_availability() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-availability")
        .in_spec("media:model-spec;textable")
        .out_spec("media:model-availability;textable;record")
        .build()
        .expect("model-availability URN")
}

fn model_status() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-status")
        .in_spec("media:model-spec;textable")
        .out_spec("media:model-status;textable;record")
        .build()
        .expect("model-status URN")
}

fn model_contents() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-contents")
        .in_spec("media:model-spec;textable")
        .out_spec("media:model-contents;textable;record")
        .build()
        .expect("model-contents URN")
}

fn model_path() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "model-path")
        .in_spec("media:model-spec;textable")
        .out_spec("media:model-path;textable;record")
        .build()
        .expect("model-path URN")
}

fn model_download() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "download-model")
        .in_spec("media:model-spec;textable")
        .out_spec("media:download-result;textable;record")
        .build()
        .expect("model download URN")
}

// =============================================================================
// MLX Cartridge Caps
// =============================================================================

fn mlx_generate_text() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_text")
        .solo_tag("llm")
        .solo_tag("ml-model")
        .solo_tag("mlx")
        .in_spec("media:model-spec;textable")
        .out_spec("media:generated-text;textable;record")
        .build()
        .expect("mlx generate_text URN")
}

fn mlx_describe_image() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "describe_image")
        .solo_tag("vision")
        .solo_tag("ml-model")
        .solo_tag("mlx")
        .in_spec("media:image;png")
        .out_spec("media:image-description;textable")
        .build()
        .expect("mlx describe_image URN")
}

fn mlx_generate_embeddings() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_embeddings")
        .solo_tag("ml-model")
        .solo_tag("mlx")
        .in_spec("media:textable")
        .out_spec("media:embedding-vector;textable;record")
        .build()
        .expect("mlx generate_embeddings URN")
}

fn mlx_embeddings_dimensions() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "embeddings_dimensions")
        .solo_tag("ml-model")
        .solo_tag("mlx")
        .in_spec("media:model-spec;textable")
        .out_spec("media:model-dim;integer;textable;numeric")
        .build()
        .expect("mlx embeddings_dimensions URN")
}

// =============================================================================
// CartridgeRegistry — populated from the cap URN builders above
// =============================================================================

struct CartridgeRegistry {
    caps: HashMap<String, Cap>,
}

impl CartridgeRegistry {
    fn new() -> Self {
        Self {
            caps: HashMap::new(),
        }
    }

    fn register(&mut self, urn: CapUrn) {
        let op = urn
            .get_tag("op")
            .map(|s| s.to_string())
            .unwrap_or_default();
        let cap = Cap {
            urn: urn.clone(),
            title: format!("Cap {}", op),
            cap_description: None,
            metadata: HashMap::new(),
            command: "cartridge".to_string(),
            media_specs: vec![],
            args: vec![],
            output: None,
            metadata_json: None,
            registered_by: None,
        };
        self.caps.insert(urn.to_string(), cap);
    }
}

#[async_trait::async_trait]
impl CapRegistryTrait for CartridgeRegistry {
    async fn lookup(&self, urn: &str) -> Result<Cap, ParseOrchestrationError> {
        let normalized = CapUrn::from_string(urn)
            .map_err(|e| ParseOrchestrationError::CapUrnParseError(format!("{:?}", e)))?
            .to_string();

        self.caps
            .iter()
            .find(|(k, _)| {
                if let Ok(k_norm) = CapUrn::from_string(k) {
                    k_norm.to_string() == normalized
                } else {
                    false
                }
            })
            .map(|(_, v)| v.clone())
            .ok_or_else(|| ParseOrchestrationError::CapNotFound {
                cap_urn: urn.to_string(),
            })
    }
}

// =============================================================================
// Binary Discovery and Auto-Build
// =============================================================================

/// Get the cartridge directory path
fn cartridge_dir(name: &str) -> Option<PathBuf> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").ok()?;
    let manifest_path = PathBuf::from(&manifest_dir);

    // testcartridge is inside capdag/, others are at workspace root
    if name == "testcartridge" {
        let dir = manifest_path.join(name);
        if dir.exists() {
            return Some(dir);
        }
    }

    // Standard location: machinefabric/{name}
    let dir = manifest_path.parent()?.join(name);
    if dir.exists() {
        Some(dir)
    } else {
        None
    }
}

/// Check if a binary was built with Metal support (macOS only)
#[cfg(target_os = "macos")]
fn has_metal_support(binary_path: &PathBuf) -> bool {
    // Use otool to check if binary links against Metal.framework
    let output = Command::new("otool")
        .arg("-L")
        .arg(binary_path)
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout.contains("Metal.framework")
        }
        Err(_) => false,
    }
}

#[cfg(not(target_os = "macos"))]
fn has_metal_support(_binary_path: &PathBuf) -> bool {
    false
}

/// Check if a cartridge needs rebuilding by comparing source modification times
/// or if GPU features are missing
fn needs_rebuild(name: &str, binary_path: &PathBuf) -> bool {
    // First check if this is a GPU cartridge that needs metal but doesn't have it
    if GPU_CARTRIDGES.contains(&name) {
        if let Some(feature) = gpu_feature_for_platform() {
            if feature == "metal" && !has_metal_support(binary_path) {
                eprintln!("[CartridgeTest] {} binary missing Metal GPU support, will rebuild", name);
                return true;
            }
        }
    }

    let binary_mtime = match binary_path.metadata().and_then(|m| m.modified()) {
        Ok(t) => t,
        Err(_) => return true, // Can't read binary metadata, rebuild
    };

    let cart_dir = match cartridge_dir(name) {
        Some(d) => d,
        None => return false, // No source dir, can't check
    };

    // Check Cargo.toml
    let cargo_toml = cart_dir.join("Cargo.toml");
    if let Ok(meta) = cargo_toml.metadata() {
        if let Ok(mtime) = meta.modified() {
            if mtime > binary_mtime {
                eprintln!("[CartridgeTest] {} Cargo.toml is newer than binary", name);
                return true;
            }
        }
    }

    // Check src/ directory recursively
    let src_dir = cart_dir.join("src");
    if src_dir.exists() {
        if let Ok(entries) = walkdir_check(&src_dir, &binary_mtime) {
            if entries {
                eprintln!("[CartridgeTest] {} src/ has files newer than binary", name);
                return true;
            }
        }
    }

    false
}

/// Check if any file in a directory is newer than the reference time
fn walkdir_check(dir: &PathBuf, reference: &std::time::SystemTime) -> std::io::Result<bool> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if walkdir_check(&path, reference)? {
                return Ok(true);
            }
        } else if path.is_file() {
            if let Ok(meta) = path.metadata() {
                if let Ok(mtime) = meta.modified() {
                    if mtime > *reference {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
}

/// Cartridges that support GPU acceleration via metal/cuda features
const GPU_CARTRIDGES: &[&str] = &["candlecartridge", "ggufcartridge"];

/// Check if a cartridge has a specific feature defined in its Cargo.toml
fn cartridge_has_feature(name: &str, feature: &str) -> bool {
    let cart_dir = match cartridge_dir(name) {
        Some(d) => d,
        None => return false,
    };

    let cargo_toml = cart_dir.join("Cargo.toml");
    let content = match std::fs::read_to_string(&cargo_toml) {
        Ok(c) => c,
        Err(_) => return false,
    };

    // Look for [features] section and check if the feature is defined
    let mut in_features_section = false;
    for line in content.lines() {
        let line = line.trim();
        if line == "[features]" {
            in_features_section = true;
            continue;
        }
        if in_features_section {
            if line.starts_with('[') {
                // New section, stop looking
                break;
            }
            // Check if this line defines the feature we're looking for
            if line.starts_with(feature) && (line.contains('=') || line.contains(" =")) {
                return true;
            }
        }
    }
    false
}

/// Get GPU feature for the current platform
fn gpu_feature_for_platform() -> Option<&'static str> {
    if cfg!(target_os = "macos") {
        Some("metal")
    } else if cfg!(target_os = "linux") || cfg!(target_os = "windows") {
        // On Linux/Windows, try CUDA if available
        // For now, we don't auto-detect CUDA, so return None
        // Users can set CAPDAG_GPU_FEATURE=cuda to enable
        std::env::var("CAPDAG_GPU_FEATURE").ok().and_then(|v| {
            if v == "cuda" { Some("cuda") } else { None }
        }).or(None)
    } else {
        None
    }
}

/// Build a cartridge in release mode with appropriate features
fn build_cartridge(name: &str) -> Result<(), String> {
    let cart_dir = cartridge_dir(name)
        .ok_or_else(|| format!("Cartridge directory not found for {}", name))?;

    // Determine if this cartridge supports GPU and what feature to use
    // Only use the feature if the cartridge actually defines it in Cargo.toml
    let gpu_feature = if GPU_CARTRIDGES.contains(&name) {
        gpu_feature_for_platform().filter(|&feature| cartridge_has_feature(name, feature))
    } else {
        None
    };

    let mut cmd = Command::new("cargo");
    cmd.arg("build").arg("--release").current_dir(&cart_dir);

    if let Some(feature) = gpu_feature {
        cmd.arg("--features").arg(feature);
        eprintln!("[CartridgeTest] Building {} in release mode with {} GPU acceleration...", name, feature);
        eprintln!("[CartridgeTest]   Running: cargo build --release --features {}", feature);
    } else {
        eprintln!("[CartridgeTest] Building {} in release mode...", name);
        eprintln!("[CartridgeTest]   Running: cargo build --release");
    }
    eprintln!("[CartridgeTest]   Directory: {:?}", cart_dir);

    let output = cmd.output()
        .map_err(|e| format!("Failed to run cargo build for {}: {}", name, e))?;

    // Print stdout if any
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !stdout.trim().is_empty() {
        for line in stdout.lines() {
            eprintln!("[CartridgeTest]   {}", line);
        }
    }

    // Print stderr (cargo output goes here)
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !stderr.trim().is_empty() {
        for line in stderr.lines() {
            eprintln!("[CartridgeTest]   {}", line);
        }
    }

    if !output.status.success() {
        return Err(format!("Failed to build {} (exit code: {:?})", name, output.status.code()));
    }

    eprintln!("[CartridgeTest] Successfully built {}", name);
    Ok(())
}

/// Find the most recent release binary for a cartridge.
/// Looks for both unversioned (e.g., `pdfcartridge`) and versioned (e.g., `pdfcartridge-0.93.6217`)
/// names in the cartridge's `target/release/` directory.
fn find_cartridge_binary(name: &str) -> Option<PathBuf> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").ok()?;
    let manifest_path = PathBuf::from(&manifest_dir);

    // testcartridge is inside capdag/, others are at workspace root
    let release_dir = if name == "testcartridge" {
        manifest_path.join(name).join("target").join("release")
    } else {
        manifest_path.parent()?.join(name).join("target").join("release")
    };

    if !release_dir.exists() {
        return None;
    }

    // Try exact name first
    let exact = release_dir.join(name);
    if exact.is_file() {
        return Some(exact);
    }

    // Try versioned names: find most recent file matching <name>-*
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&release_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name()
                    .and_then(|f| f.to_str())
                    .map_or(false, |f| {
                        f.starts_with(&format!("{}-", name))
                            && !f.ends_with(".d")
                            && !f.ends_with(".dSYM")
                    })
        })
        .collect();

    // Sort by modification time, most recent first
    candidates.sort_by(|a, b| {
        b.metadata()
            .and_then(|m| m.modified())
            .ok()
            .cmp(&a.metadata().and_then(|m| m.modified()).ok())
    });

    candidates.into_iter().next()
}

/// Ensure a cartridge binary exists and is up-to-date, building if necessary
fn ensure_cartridge_binary(name: &str) -> Result<PathBuf, String> {
    // First check if binary exists
    let existing = find_cartridge_binary(name);

    let needs_build = match &existing {
        None => {
            eprintln!("[CartridgeTest] {} binary not found, will build", name);
            true
        }
        Some(path) => needs_rebuild(name, path),
    };

    if needs_build {
        build_cartridge(name)?;
    }

    // Now find the binary (should exist after build)
    find_cartridge_binary(name)
        .ok_or_else(|| format!("{} binary not found after build", name))
}

/// Require specific cartridge binaries. Builds them if missing or outdated.
fn require_binaries(names: &[&str]) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for &name in names {
        match ensure_cartridge_binary(name) {
            Ok(path) => {
                eprintln!("[CartridgeTest] Using {}: {:?}", name, path);
                paths.push(path);
            }
            Err(e) => {
                panic!("Failed to build {}: {}", name, e);
            }
        }
    }
    paths
}

// =============================================================================
// Test Fixture Generators
// =============================================================================

/// Generate a minimal valid PDF with one blank page.
fn generate_test_pdf() -> Vec<u8> {
    let mut pdf = Vec::new();
    pdf.extend_from_slice(b"%PDF-1.0\n");

    let obj1_start = pdf.len();
    pdf.extend_from_slice(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n");

    let obj2_start = pdf.len();
    pdf.extend_from_slice(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n");

    let obj3_start = pdf.len();
    pdf.extend_from_slice(
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n",
    );

    let xref_start = pdf.len();
    pdf.extend_from_slice(b"xref\n0 4\n");
    // Each xref entry is exactly 20 bytes: offset(10) + space + gen(5) + space + keyword + space + LF
    pdf.extend_from_slice(format!("{:010} 65535 f \n", 0).as_bytes());
    pdf.extend_from_slice(format!("{:010} 00000 n \n", obj1_start).as_bytes());
    pdf.extend_from_slice(format!("{:010} 00000 n \n", obj2_start).as_bytes());
    pdf.extend_from_slice(format!("{:010} 00000 n \n", obj3_start).as_bytes());
    pdf.extend_from_slice(b"trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n");
    pdf.extend_from_slice(format!("{}\n%%EOF\n", xref_start).as_bytes());

    pdf
}

/// Generate a simple markdown document.
fn generate_test_markdown() -> Vec<u8> {
    b"# Test Document\n\n## Section One\n\nThis is a test document for macino integration tests.\n\n## Section Two\n\nMore content here.\n".to_vec()
}

/// CRC32 computation for PNG chunks.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// Build a PNG chunk with correct CRC.
fn png_chunk(chunk_type: &[u8; 4], data: &[u8]) -> Vec<u8> {
    let mut chunk = Vec::new();
    chunk.extend_from_slice(&(data.len() as u32).to_be_bytes());
    chunk.extend_from_slice(chunk_type);
    chunk.extend_from_slice(data);
    let mut crc_input = Vec::with_capacity(4 + data.len());
    crc_input.extend_from_slice(chunk_type);
    crc_input.extend_from_slice(data);
    chunk.extend_from_slice(&crc32(&crc_input).to_be_bytes());
    chunk
}

/// Adler32 checksum for zlib.
fn adler32(data: &[u8]) -> u32 {
    let mut a: u32 = 1;
    let mut b: u32 = 0;
    for &byte in data {
        a = (a + byte as u32) % 65521;
        b = (b + a) % 65521;
    }
    (b << 16) | a
}

/// Wrap raw data in a zlib container using stored (uncompressed) deflate blocks.
fn zlib_stored(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    // CMF=0x78 (deflate, 32K window), FLG=0x01 (check: (0x78*256+0x01) % 31 == 0)
    out.push(0x78);
    out.push(0x01);

    // Split into stored blocks (max 65535 bytes each)
    let mut offset = 0;
    while offset < data.len() {
        let remaining = data.len() - offset;
        let block_size = remaining.min(65535);
        let is_final = offset + block_size >= data.len();

        out.push(if is_final { 0x01 } else { 0x00 });
        out.extend_from_slice(&(block_size as u16).to_le_bytes());
        out.extend_from_slice(&(!(block_size as u16)).to_le_bytes());
        out.extend_from_slice(&data[offset..offset + block_size]);

        offset += block_size;
    }

    // Handle empty data: emit one final empty stored block
    if data.is_empty() {
        out.push(0x01);
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&(!0u16).to_le_bytes());
    }

    out.extend_from_slice(&adler32(data).to_be_bytes());
    out
}

/// Generate a valid PNG image (32x32 solid color, RGB).
fn generate_test_png(width: u32, height: u32, r: u8, g: u8, b: u8) -> Vec<u8> {
    let mut png = Vec::new();

    // PNG signature
    png.extend_from_slice(&[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]);

    // IHDR
    let mut ihdr = Vec::new();
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.push(8); // bit depth
    ihdr.push(2); // color type: RGB
    ihdr.push(0); // compression: deflate
    ihdr.push(0); // filter: adaptive
    ihdr.push(0); // interlace: none
    png.extend_from_slice(&png_chunk(b"IHDR", &ihdr));

    // Raw pixel data: filter_byte(0) + RGB per pixel, per row
    let mut raw = Vec::with_capacity((1 + width as usize * 3) * height as usize);
    for _ in 0..height {
        raw.push(0); // filter: None
        for _ in 0..width {
            raw.extend_from_slice(&[r, g, b]);
        }
    }

    // IDAT: zlib-compressed pixel data
    let compressed = zlib_stored(&raw);
    png.extend_from_slice(&png_chunk(b"IDAT", &compressed));

    // IEND
    png.extend_from_slice(&png_chunk(b"IEND", &[]));

    png
}

/// Generate a minimal WAV file (16kHz 16-bit mono PCM, 0.1s silence).
fn generate_test_wav() -> Vec<u8> {
    let sample_rate: u32 = 16000;
    let num_channels: u16 = 1;
    let bits_per_sample: u16 = 16;
    let num_samples: u32 = sample_rate / 10; // 0.1 seconds
    let byte_rate = sample_rate * num_channels as u32 * bits_per_sample as u32 / 8;
    let block_align = num_channels * bits_per_sample / 8;
    let data_size = num_samples * block_align as u32;
    let file_size = 36 + data_size;

    let mut wav = Vec::with_capacity(44 + data_size as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&file_size.to_le_bytes());
    wav.extend_from_slice(b"WAVE");
    wav.extend_from_slice(b"fmt ");
    wav.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
    wav.extend_from_slice(&1u16.to_le_bytes()); // PCM format
    wav.extend_from_slice(&num_channels.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&block_align.to_le_bytes());
    wav.extend_from_slice(&bits_per_sample.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_size.to_le_bytes());
    // Silence: all zeros
    wav.resize(wav.len() + data_size as usize, 0);
    wav
}

// =============================================================================
// DOT Construction Helpers
// =============================================================================

/// Escape a cap URN string for use inside a DOT label attribute.
/// The URN's internal double quotes become escaped quotes in the DOT string.
fn escape_for_dot(cap_urn: &str) -> String {
    cap_urn.replace('"', "\\\"")
}

/// Build a DOT edge line from node names and a cap URN.
fn dot_edge(from: &str, to: &str, cap_urn: &CapUrn) -> String {
    format!(
        "        {} -> {} [label=\"{}\"];",
        from,
        to,
        escape_for_dot(&cap_urn.to_string())
    )
}

/// Build a DOT node declaration with an explicit media type attribute.
///
/// Used for secondary-arg fan-in nodes (e.g., model_spec) where the node's
/// actual data type differs from the cap's primary in= spec. The executor
/// uses this declared media type as the stream label, letting the cartridge
/// handler identify each stream by its exact type.
fn dot_node(name: &str, media_urn: &str) -> String {
    format!("        {} [media=\"{}\"];", name, media_urn)
}

/// Build a complete DOT digraph from a name and edge lines.
fn dot_graph(name: &str, edges: &[String]) -> String {
    format!(
        "    digraph {} {{\n{}\n    }}",
        name,
        edges.join("\n")
    )
}

/// Path to test scenario DOT files
fn scenarios_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("scenarios")
}

/// Load a DOT file from the scenarios directory and generate a diagram.
/// Returns the DOT string content for parsing.
fn load_scenario_dot(name: &str) -> String {
    let dot_path = scenarios_dir().join(format!("{}.dot", name));
    let dot_content = std::fs::read_to_string(&dot_path)
        .unwrap_or_else(|e| panic!("Failed to read DOT file {}: {}", dot_path.display(), e));

    // Generate diagram using graphviz dot command
    let diagrams_dir = scenarios_dir().join("diagrams");
    std::fs::create_dir_all(&diagrams_dir).expect("Failed to create diagrams directory");

    let svg_path = diagrams_dir.join(format!("{}.svg", name));
    let output = std::process::Command::new("dot")
        .args(["-Tsvg", "-o", svg_path.to_str().unwrap()])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(stdin) = child.stdin.as_mut() {
                stdin.write_all(dot_content.as_bytes())?;
            }
            child.wait_with_output()
        });

    match output {
        Ok(result) if result.status.success() => {
            eprintln!("[DOT] Generated diagram: {}", svg_path.display());
        }
        Ok(result) => {
            eprintln!("[DOT] Warning: dot command failed: {}", String::from_utf8_lossy(&result.stderr));
        }
        Err(e) => {
            eprintln!("[DOT] Warning: Could not run dot command: {} (graphviz may not be installed)", e);
        }
    }

    dot_content
}

/// Save DOT content to a file in the scenarios directory.
fn save_scenario_dot(name: &str, dot_content: &str) {
    let dot_path = scenarios_dir().join(format!("{}.dot", name));
    std::fs::write(&dot_path, dot_content)
        .unwrap_or_else(|e| panic!("Failed to write DOT file {}: {}", dot_path.display(), e));
    eprintln!("[DOT] Saved: {}", dot_path.display());
}

/// Generate all scenario DOT files.
/// Run with: cargo test generate_all_scenario_dots -- --ignored --nocapture
#[test]
#[ignore]
fn generate_all_scenario_dots() {
    std::fs::create_dir_all(scenarios_dir()).expect("Failed to create scenarios directory");

    // TEST014: PDF document intelligence (fan-out)
    save_scenario_dot("test948_pdf_document_intelligence", &dot_graph(
        "pdf_document_intelligence",
        &[
            dot_edge("pdf_input", "metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "outline", &pdf_extract_outline()),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
        ],
    ));

    // TEST015: PDF thumbnail to image embedding (chain)
    save_scenario_dot("test949_pdf_thumbnail_to_image_embedding", &dot_graph(
        "pdf_thumbnail_to_image_embedding",
        &[
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "embedding", &candle_image_embeddings()),
        ],
    ));

    // TEST016: PDF full intelligence pipeline (fan-out + chain)
    save_scenario_dot("test950_pdf_full_intelligence_pipeline", &dot_graph(
        "pdf_full_intelligence",
        &[
            dot_edge("pdf_input", "metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "outline", &pdf_extract_outline()),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "img_embedding", &candle_image_embeddings()),
        ],
    ));

    // TEST017: Text document intelligence (fan-out)
    save_scenario_dot("test951_text_document_intelligence", &dot_graph(
        "text_document_intelligence",
        &[
            dot_edge("md_input", "metadata", &md_extract_metadata()),
            dot_edge("md_input", "outline", &md_extract_outline()),
            dot_edge("md_input", "thumbnail", &md_generate_thumbnail()),
        ],
    ));

    // TEST018: Multi-format document processing (parallel fan-outs)
    save_scenario_dot("test952_multi_format_document_processing", &dot_graph(
        "multi_format_processing",
        &[
            dot_edge("pdf_input", "pdf_metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "pdf_outline", &pdf_extract_outline()),
            dot_edge("pdf_input", "pdf_thumbnail", &pdf_generate_thumbnail()),
            dot_edge("md_input", "md_metadata", &md_extract_metadata()),
            dot_edge("md_input", "md_outline", &md_extract_outline()),
            dot_edge("md_input", "md_thumbnail", &md_generate_thumbnail()),
        ],
    ));

    // TEST019: Model plus dimensions (fan-out)
    save_scenario_dot("test953_model_plus_dimensions", &dot_graph(
        "model_plus_dimensions",
        &[
            dot_edge("model_spec", "availability", &model_availability()),
            dot_edge("model_spec", "dimensions", &candle_embeddings_dimensions()),
        ],
    ));

    // TEST020: Model availability plus status (fan-out)
    save_scenario_dot("test954_model_availability_plus_status", &dot_graph(
        "model_availability_plus_status",
        &[
            dot_edge("model_spec", "availability", &model_availability()),
            dot_edge("model_spec", "status", &model_status()),
        ],
    ));

    // TEST021: Text embedding (single cap)
    save_scenario_dot("test955_text_embedding", &dot_graph(
        "text_embedding",
        &[
            dot_edge("text_input", "embedding", &candle_text_embeddings()),
        ],
    ));

    // TEST022: Candle describe image (single cap)
    save_scenario_dot("test956_candle_describe_image", &dot_graph(
        "candle_describe_image",
        &[
            dot_edge("image_input", "description", &candle_describe_image()),
        ],
    ));

    // TEST023: Audio transcription (single cap)
    save_scenario_dot("test957_audio_transcription", &dot_graph(
        "audio_transcription",
        &[
            dot_edge("audio_input", "transcription", &candle_transcribe()),
        ],
    ));

    // TEST024: PDF complete analysis (4 cap fan-out)
    save_scenario_dot("test958_pdf_complete_analysis", &dot_graph(
        "pdf_complete_analysis",
        &[
            dot_edge("pdf_input", "metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "outline", &pdf_extract_outline()),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("pdf_input", "pages", &pdf_disbind()),
        ],
    ));

    // TEST025: Model full inspection (4 cap fan-out)
    save_scenario_dot("test959_model_full_inspection", &dot_graph(
        "model_full_inspection",
        &[
            dot_edge("model_spec", "availability", &model_availability()),
            dot_edge("model_spec", "status", &model_status()),
            dot_edge("model_spec", "contents", &model_contents()),
            dot_edge("model_spec", "path", &model_path()),
        ],
    ));

    // TEST026: Two format full analysis (8 cap parallel fan-outs)
    save_scenario_dot("test960_two_format_full_analysis", &dot_graph(
        "two_format_full_analysis",
        &[
            dot_edge("pdf_input", "pdf_metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "pdf_outline", &pdf_extract_outline()),
            dot_edge("pdf_input", "pdf_thumbnail", &pdf_generate_thumbnail()),
            dot_edge("pdf_input", "pdf_pages", &pdf_disbind()),
            dot_edge("md_input", "md_metadata", &md_extract_metadata()),
            dot_edge("md_input", "md_outline", &md_extract_outline()),
            dot_edge("md_input", "md_thumbnail", &md_generate_thumbnail()),
            dot_edge("md_input", "text_embedding", &candle_text_embeddings()),
        ],
    ));

    // TEST027: Model plus PDF combined (5 cap parallel)
    save_scenario_dot("test961_model_plus_pdf_combined", &dot_graph(
        "model_plus_pdf_combined",
        &[
            dot_edge("model_spec", "availability", &model_availability()),
            dot_edge("model_spec", "status", &model_status()),
            dot_edge("pdf_input", "metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "outline", &pdf_extract_outline()),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
        ],
    ));

    // TEST028: Three cartridge pipeline (chain)
    save_scenario_dot("test962_three_cartridge_pipeline", &dot_graph(
        "three_cartridge_pipeline",
        &[
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "img_embedding", &candle_image_embeddings()),
        ],
    ));

    // TEST029: TXT document intelligence (fan-out)
    save_scenario_dot("test963_txt_document_intelligence", &dot_graph(
        "txt_document_intelligence",
        &[
            dot_edge("txt_input", "metadata", &txt_extract_metadata()),
            dot_edge("txt_input", "outline", &txt_extract_outline()),
            dot_edge("txt_input", "thumbnail", &txt_generate_thumbnail()),
        ],
    ));

    // TEST030: RST document intelligence (fan-out)
    save_scenario_dot("test964_rst_document_intelligence", &dot_graph(
        "rst_document_intelligence",
        &[
            dot_edge("rst_input", "metadata", &rst_extract_metadata()),
            dot_edge("rst_input", "outline", &rst_extract_outline()),
            dot_edge("rst_input", "thumbnail", &rst_generate_thumbnail()),
        ],
    ));

    // TEST031: LOG document intelligence (fan-out)
    save_scenario_dot("test965_log_document_intelligence", &dot_graph(
        "log_document_intelligence",
        &[
            dot_edge("log_input", "metadata", &log_extract_metadata()),
            dot_edge("log_input", "outline", &log_extract_outline()),
            dot_edge("log_input", "thumbnail", &log_generate_thumbnail()),
        ],
    ));

    // TEST032: All text formats intelligence (12 cap parallel fan-outs)
    save_scenario_dot("test966_all_text_formats_intelligence", &dot_graph(
        "all_text_formats_intelligence",
        &[
            dot_edge("md_input", "md_metadata", &md_extract_metadata()),
            dot_edge("md_input", "md_outline", &md_extract_outline()),
            dot_edge("md_input", "md_thumbnail", &md_generate_thumbnail()),
            dot_edge("txt_input", "txt_metadata", &txt_extract_metadata()),
            dot_edge("txt_input", "txt_outline", &txt_extract_outline()),
            dot_edge("txt_input", "txt_thumbnail", &txt_generate_thumbnail()),
            dot_edge("rst_input", "rst_metadata", &rst_extract_metadata()),
            dot_edge("rst_input", "rst_outline", &rst_extract_outline()),
            dot_edge("rst_input", "rst_thumbnail", &rst_generate_thumbnail()),
            dot_edge("log_input", "log_metadata", &log_extract_metadata()),
            dot_edge("log_input", "log_outline", &log_extract_outline()),
            dot_edge("log_input", "log_thumbnail", &log_generate_thumbnail()),
        ],
    ));

    // TEST033: Model list models (single cap)
    save_scenario_dot("test967_model_list_models", &dot_graph(
        "model_list_models",
        &[
            dot_edge("repo_input", "model_list", &model_list_models()),
        ],
    ));

    // TEST034: GGUF embeddings dimensions (single cap)
    save_scenario_dot("test968_gguf_embeddings_dimensions", &dot_graph(
        "gguf_embeddings_dimensions",
        &[
            dot_edge("model_spec", "dimensions", &gguf_embeddings_dimensions()),
        ],
    ));

    // TEST035: GGUF LLM model info (single cap)
    save_scenario_dot("test969_gguf_llm_model_info", &dot_graph(
        "gguf_llm_model_info",
        &[
            dot_edge("model_spec", "model_info", &gguf_llm_model_info()),
        ],
    ));

    // TEST036: GGUF LLM vocab (single cap)
    save_scenario_dot("test970_gguf_llm_vocab", &dot_graph(
        "gguf_llm_vocab",
        &[
            dot_edge("model_spec", "vocab", &gguf_llm_vocab()),
        ],
    ));

    // TEST037: GGUF model info plus vocab (fan-out)
    save_scenario_dot("test971_gguf_model_info_plus_vocab", &dot_graph(
        "gguf_model_info_plus_vocab",
        &[
            dot_edge("model_spec", "model_info", &gguf_llm_model_info()),
            dot_edge("model_spec", "vocab", &gguf_llm_vocab()),
        ],
    ));

    // TEST038: GGUF LLM inference (fan-in)
    save_scenario_dot("test972_gguf_llm_inference", &dot_graph(
        "gguf_llm_inference",
        &[
            dot_node("request", "media:llm-generation-request;json;record"),
            dot_edge("request", "response", &gguf_llm_inference()),
        ],
    ));

    // TEST039: GGUF LLM inference constrained (fan-in)
    save_scenario_dot("test973_gguf_llm_inference_constrained", &dot_graph(
        "gguf_llm_inference_constrained",
        &[
            dot_node("request", "media:llm-generation-request;json;record"),
            dot_edge("request", "response", &gguf_llm_inference_constrained()),
        ],
    ));

    // TEST040: GGUF generate embeddings (fan-in)
    save_scenario_dot("test974_gguf_generate_embeddings", &dot_graph(
        "gguf_generate_embeddings",
        &[
            dot_node("model_spec", "media:model-spec;textable"),
            dot_edge("text_input", "embedding", &gguf_generate_embeddings()),
            dot_edge("model_spec", "embedding", &gguf_generate_embeddings()),
        ],
    ));

    // TEST041: GGUF describe image (fan-in)
    save_scenario_dot("test975_gguf_describe_image", &dot_graph(
        "gguf_vision",
        &[
            dot_node("model_spec", "media:model-spec;textable"),
            dot_edge("image_input", "description", &gguf_describe_image()),
            dot_edge("model_spec", "description", &gguf_describe_image()),
        ],
    ));

    // TEST042: PDF thumbnail to GGUF vision (cross-cartridge chain)
    save_scenario_dot("test976_pdf_thumbnail_to_gguf_vision", &dot_graph(
        "pdf_thumbnail_to_gguf_vision",
        &[
            dot_node("model_spec", "media:model-spec;textable"),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "description", &gguf_describe_image()),
            dot_edge("model_spec", "description", &gguf_describe_image()),
        ],
    ));

    // TEST043: GGUF all LLM ops (fan-out)
    save_scenario_dot("test977_gguf_all_llm_ops", &dot_graph(
        "gguf_all_llm_ops",
        &[
            dot_edge("model_spec", "model_info", &gguf_llm_model_info()),
            dot_edge("model_spec", "vocab", &gguf_llm_vocab()),
            dot_edge("model_spec", "dimensions", &gguf_embeddings_dimensions()),
        ],
    ));

    // TEST044: MLX text generation (single cap)
    save_scenario_dot("test978_mlx_generate_text", &dot_graph(
        "mlx_generate_text",
        &[
            dot_edge("model_spec", "generated_text", &mlx_generate_text()),
        ],
    ));

    // TEST045: MLX describe image (single cap)
    save_scenario_dot("test979_mlx_describe_image", &dot_graph(
        "mlx_describe_image",
        &[
            dot_edge("image_input", "description", &mlx_describe_image()),
        ],
    ));

    // TEST046: MLX generate embeddings (single cap)
    save_scenario_dot("test980_mlx_generate_embeddings", &dot_graph(
        "mlx_generate_embeddings",
        &[
            dot_edge("text_input", "embedding", &mlx_generate_embeddings()),
        ],
    ));

    // TEST047: MLX embeddings dimensions (single cap)
    save_scenario_dot("test981_mlx_embeddings_dimensions", &dot_graph(
        "mlx_embeddings_dimensions",
        &[
            dot_edge("model_spec", "dimensions", &mlx_embeddings_dimensions()),
        ],
    ));

    // TEST048: Model download (single cap)
    save_scenario_dot("test982_model_download", &dot_graph(
        "model_download",
        &[
            dot_edge("model_spec", "download_result", &model_download()),
        ],
    ));

    // TEST049: 4-step chain: PDF → thumbnail → candle describe → text embeddings
    save_scenario_dot("test983_pdf_to_thumbnail_to_describe_to_embed", &dot_graph(
        "pdf_thumbnail_describe_embed_chain",
        &[
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "description", &candle_describe_image()),
            dot_edge("description", "embedding", &candle_text_embeddings()),
        ],
    ));

    // TEST050: 3-step chain with fan-in: PDF → thumbnail → gguf describe (with model_spec)
    save_scenario_dot("test984_pdf_thumbnail_to_gguf_describe_fanin", &dot_graph(
        "pdf_thumbnail_gguf_describe_fanin",
        &[
            dot_node("model_spec", "media:model-spec;textable"),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "description", &gguf_describe_image()),
            dot_edge("model_spec", "description", &gguf_describe_image()),
        ],
    ));

    // TEST051: Audio transcription → text embeddings (cross-ML chain)
    save_scenario_dot("test985_audio_transcribe_to_embed", &dot_graph(
        "audio_transcribe_embed_chain",
        &[
            dot_edge("audio_input", "transcription", &candle_transcribe()),
            // Note: transcription output is record with text field, needs extraction
            // For now we test the chain structure
        ],
    ));

    // TEST052: PDF fan-out + chain: metadata + thumbnail → image embedding
    save_scenario_dot("test986_pdf_fanout_with_chain", &dot_graph(
        "pdf_fanout_with_chain",
        &[
            dot_edge("pdf_input", "metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "outline", &pdf_extract_outline()),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "img_embedding", &candle_image_embeddings()),
        ],
    ));

    // TEST053: Multi-format parallel with chains: PDF + MD both get thumbnails and embeddings
    save_scenario_dot("test987_multi_format_parallel_chains", &dot_graph(
        "multi_format_parallel_chains",
        &[
            dot_edge("pdf_input", "pdf_thumbnail", &pdf_generate_thumbnail()),
            dot_edge("pdf_thumbnail", "pdf_img_embed", &candle_image_embeddings()),
            dot_edge("md_input", "md_thumbnail", &md_generate_thumbnail()),
            dot_edge("md_thumbnail", "md_img_embed", &candle_image_embeddings()),
        ],
    ));

    // TEST054: Deep chain: PDF → thumbnail → describe → embed → (parallel with) PDF metadata
    save_scenario_dot("test988_deep_chain_with_parallel", &dot_graph(
        "deep_chain_with_parallel",
        &[
            dot_edge("pdf_input", "metadata", &pdf_extract_metadata()),
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "description", &candle_describe_image()),
            dot_edge("description", "desc_embedding", &candle_text_embeddings()),
            dot_edge("thumbnail", "img_embedding", &candle_image_embeddings()),
        ],
    ));

    // TEST055: 5-step maximum chain: model download → status → (proves model ready) + PDF thumbnail → describe → embed
    save_scenario_dot("test989_five_cartridge_chain", &dot_graph(
        "five_cartridge_stress_test",
        &[
            // Model management path
            dot_edge("model_spec", "availability", &model_availability()),
            dot_edge("model_spec", "status", &model_status()),
            // PDF processing path
            dot_edge("pdf_input", "thumbnail", &pdf_generate_thumbnail()),
            dot_edge("thumbnail", "description", &candle_describe_image()),
            dot_edge("description", "embedding", &candle_text_embeddings()),
        ],
    ));

    // TEST056: All txtcartridge formats → thumbnails → parallel image embeddings
    save_scenario_dot("test990_all_text_formats_to_image_embeds", &dot_graph(
        "all_text_formats_to_image_embeds",
        &[
            dot_edge("txt_input", "txt_thumbnail", &txt_generate_thumbnail()),
            dot_edge("txt_thumbnail", "txt_img_embed", &candle_image_embeddings()),
            dot_edge("md_input", "md_thumbnail", &md_generate_thumbnail()),
            dot_edge("md_thumbnail", "md_img_embed", &candle_image_embeddings()),
            dot_edge("rst_input", "rst_thumbnail", &rst_generate_thumbnail()),
            dot_edge("rst_thumbnail", "rst_img_embed", &candle_image_embeddings()),
            dot_edge("log_input", "log_thumbnail", &log_generate_thumbnail()),
            dot_edge("log_thumbnail", "log_img_embed", &candle_image_embeddings()),
        ],
    ));

    eprintln!("\n[DOT] Generated all scenario DOT files in: {}", scenarios_dir().display());
}

// =============================================================================
// ML Model Specs (matching candlecartridge defaults)
// =============================================================================

const MODEL_BERT: &str = "hf:sentence-transformers/all-MiniLM-L6-v2?include=*.json,*.safetensors";
const MODEL_CLIP: &str = "hf:openai/clip-vit-base-patch32?include=*.json,*.safetensors,pytorch_model.bin";
const MODEL_BLIP: &str = "hf:Salesforce/blip-image-captioning-large?include=*.json,*.safetensors";
const MODEL_WHISPER: &str = "hf:openai/whisper-base?include=*.json,*.safetensors";

// =============================================================================
// MLX Model Specs (matching mlxcartridge defaults)
// =============================================================================

const MODEL_MLX_LLM: &str = "hf:mlx-community/Llama-3.2-3B-Instruct-4bit";
const MODEL_MLX_VISION: &str = "hf:mlx-community/llava-1.5-7b-4bit";
const MODEL_MLX_EMBED: &str = "hf:mlx-community/all-MiniLM-L6-v2-4bit";

// =============================================================================
// Model Pre-Download
// =============================================================================

/// Pre-download ML models via modelcartridge before running ML tests.
/// This prevents ML cartridges from hanging on peer model-download requests
/// during DAG execution. Runs a single-edge DAG: model_spec → download-model.
async fn ensure_model_downloaded(model_spec: &str, modelcartridge_bin: &PathBuf) {
    eprintln!(
        "[PreDownload] Ensuring model is available: {}",
        model_spec
    );

    let download_urn = model_download();
    let mut registry = CartridgeRegistry::new();
    registry.register(download_urn.clone());

    let dot = dot_graph(
        "pre_download",
        &[dot_edge("model", "result", &download_urn)],
    );

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Pre-download DAG parse failed");

    let temp = TempDir::new().expect("temp dir");
    let plugin_dir = temp.path().join("plugins");
    std::fs::create_dir_all(&plugin_dir).expect("plugin dir");

    let mut inputs = HashMap::new();
    inputs.insert("model".to_string(), NodeData::Text(model_spec.to_string()));

    match execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        vec![modelcartridge_bin.clone()],
    )
    .await
    {
        Ok(outputs) => {
            if let Some(NodeData::Bytes(b)) = outputs.get("result") {
                let result = String::from_utf8_lossy(b);
                eprintln!("[PreDownload] Result: {}", &result[..result.len().min(200)]);
            }
        }
        Err(e) => {
            panic!("[PreDownload] Model download failed for '{}': {}", model_spec, e);
        }
    }
}

// =============================================================================
// Test Setup
// =============================================================================

fn setup_test_env(dev_binaries: Vec<PathBuf>) -> (TempDir, PathBuf, Vec<PathBuf>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let plugin_dir = temp_dir.path().join("plugins");
    std::fs::create_dir_all(&plugin_dir).expect("Failed to create plugin dir");
    (temp_dir, plugin_dir, dev_binaries)
}

fn extract_bytes(outputs: &HashMap<String, NodeData>, node: &str) -> Vec<u8> {
    match outputs.get(node).unwrap_or_else(|| panic!("Missing node '{}'", node)) {
        NodeData::Bytes(b) => b.clone(),
        other => panic!("Expected Bytes at node '{}', got {:?}", node, other),
    }
}

fn extract_text(outputs: &HashMap<String, NodeData>, node: &str) -> String {
    let bytes = extract_bytes(outputs, node);
    String::from_utf8(bytes).unwrap_or_else(|_| panic!("Invalid UTF-8 at node '{}'", node))
}

// =============================================================================
// Scenario 1: PDF Document Intelligence (3 caps, fan-out)
// pdfcartridge: extract_metadata + extract_outline + generate_thumbnail
// =============================================================================

// TEST014: PDF fan-out produces metadata, outline, and thumbnail from a single PDF input
#[tokio::test]
#[serial]
async fn test948_pdf_document_intelligence() {
    let dev_binaries = require_binaries(&["pdfcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_extract_metadata());
    registry.register(pdf_extract_outline());
    registry.register(pdf_generate_thumbnail());

    let dot = load_scenario_dot("test948_pdf_document_intelligence");
    eprintln!("[TEST014] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 3);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // Verify metadata is JSON with expected keys
    let metadata_text = extract_text(&outputs, "metadata");
    eprintln!("[TEST014] metadata: {}", &metadata_text[..metadata_text.len().min(200)]);
    assert!(
        metadata_text.contains("page_count") || metadata_text.contains("pages"),
        "Metadata should contain page information"
    );

    // Verify outline is JSON
    let outline_text = extract_text(&outputs, "outline");
    eprintln!("[TEST014] outline: {}", &outline_text[..outline_text.len().min(200)]);
    // Outline might be empty for a blank PDF, but should be valid
    assert!(!outline_text.is_empty(), "Outline should not be empty");

    // Verify thumbnail is PNG (starts with PNG signature)
    let thumbnail_bytes = extract_bytes(&outputs, "thumbnail");
    eprintln!("[TEST014] thumbnail: {} bytes", thumbnail_bytes.len());
    assert!(
        thumbnail_bytes.len() >= 8 && thumbnail_bytes[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail should be valid PNG (signature check)"
    );
}

// =============================================================================
// Scenario 2: PDF Thumbnail to Image Embedding (2 caps, linear chain)
// pdfcartridge → candlecartridge (requires parser fix for media URN compatibility)
// =============================================================================

// TEST015: Cross-cartridge chain: PDF thumbnail piped to CLIP image embedding
#[tokio::test]
#[serial]
async fn test949_pdf_thumbnail_to_image_embedding() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_generate_thumbnail());
    registry.register(candle_image_embeddings());

    // Pre-download CLIP model needed for image embeddings
    let modelcartridge_bin = &dev_binaries.iter().find(|p| {
        p.to_str().map_or(false, |s| s.contains("modelcartridge"))
    }).expect("modelcartridge binary required").clone();
    ensure_model_downloaded(MODEL_CLIP, modelcartridge_bin).await;

    let dot = load_scenario_dot("test949_pdf_thumbnail_to_image_embedding");
    eprintln!("[TEST015] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 2);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // Verify embedding output is JSON with embedding vector
    let embedding_text = extract_text(&outputs, "embedding");
    eprintln!(
        "[TEST015] embedding: {}",
        &embedding_text[..embedding_text.len().min(200)]
    );
    assert!(
        embedding_text.contains("embeddings") || embedding_text.contains("embedding"),
        "Embedding output should contain embedding vector data"
    );
}

// =============================================================================
// Scenario 3: PDF Full Intelligence Pipeline (5 caps, fan-out + chain)
// pdfcartridge ×3 + candlecartridge: metadata + outline + thumbnail → image_embeddings
// =============================================================================

// TEST016: Complete PDF intelligence pipeline with cross-cartridge image embedding
#[tokio::test]
#[serial]
async fn test950_pdf_full_intelligence_pipeline() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_extract_metadata());
    registry.register(pdf_extract_outline());
    registry.register(pdf_generate_thumbnail());
    registry.register(candle_image_embeddings());

    // Pre-download CLIP model needed for image embeddings
    let modelcartridge_bin = &dev_binaries.iter().find(|p| {
        p.to_str().map_or(false, |s| s.contains("modelcartridge"))
    }).expect("modelcartridge binary required").clone();
    ensure_model_downloaded(MODEL_CLIP, modelcartridge_bin).await;

    let dot = load_scenario_dot("test950_pdf_full_intelligence_pipeline");
    eprintln!("[TEST016] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 4);
    assert_eq!(graph.nodes.len(), 5); // pdf_input, metadata, outline, thumbnail, img_embedding

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // All 4 output nodes should have data
    assert!(outputs.contains_key("metadata"), "Missing metadata output");
    assert!(outputs.contains_key("outline"), "Missing outline output");
    assert!(outputs.contains_key("thumbnail"), "Missing thumbnail output");
    assert!(
        outputs.contains_key("img_embedding"),
        "Missing img_embedding output"
    );

    // Verify thumbnail is PNG
    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );

    // Verify metadata is non-empty text
    let meta = extract_text(&outputs, "metadata");
    assert!(!meta.is_empty(), "Metadata must not be empty");

    // Verify embedding has data
    let emb = extract_text(&outputs, "img_embedding");
    assert!(!emb.is_empty(), "Image embedding must not be empty");
}

// =============================================================================
// Scenario 4: Text Document Intelligence (3 caps, fan-out)
// txtcartridge: extract_metadata + extract_outline + generate_thumbnail on markdown
// =============================================================================

// TEST017: Markdown fan-out produces metadata, outline, and thumbnail
#[tokio::test]
#[serial]
async fn test951_text_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(md_extract_metadata());
    registry.register(md_extract_outline());
    registry.register(md_generate_thumbnail());

    let dot = load_scenario_dot("test951_text_document_intelligence");
    eprintln!("[TEST017] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 3);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // Verify metadata
    let meta = extract_text(&outputs, "metadata");
    eprintln!("[TEST017] metadata: {}", &meta[..meta.len().min(200)]);
    assert!(!meta.is_empty(), "Metadata must not be empty");

    // Verify outline (markdown has headers so outline should have content)
    let outline = extract_text(&outputs, "outline");
    eprintln!("[TEST017] outline: {}", &outline[..outline.len().min(200)]);
    assert!(!outline.is_empty(), "Outline must not be empty");

    // Verify thumbnail is PNG
    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );
}

// =============================================================================
// Scenario 5: Multi-Format Document Processing (6 caps, parallel fan-outs)
// pdfcartridge ×3 + txtcartridge ×3: two independent fan-outs from different inputs
// =============================================================================

// TEST018: Parallel processing of PDF and markdown through independent fan-outs
#[tokio::test]
#[serial]
async fn test952_multi_format_document_processing() {
    let dev_binaries = require_binaries(&["pdfcartridge", "txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_extract_metadata());
    registry.register(pdf_extract_outline());
    registry.register(pdf_generate_thumbnail());
    registry.register(md_extract_metadata());
    registry.register(md_extract_outline());
    registry.register(md_generate_thumbnail());

    let dot = load_scenario_dot("test952_multi_format_document_processing");
    eprintln!("[TEST018] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 6);
    assert_eq!(graph.nodes.len(), 8); // 2 inputs + 6 outputs

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // All 6 output nodes should have data
    for node in &[
        "pdf_metadata",
        "pdf_outline",
        "pdf_thumbnail",
        "md_metadata",
        "md_outline",
        "md_thumbnail",
    ] {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
    }

    // Both thumbnails should be PNG
    for node in &["pdf_thumbnail", "md_thumbnail"] {
        let thumb = extract_bytes(&outputs, node);
        assert!(
            thumb.len() >= 8
                && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "{} must be valid PNG",
            node
        );
    }

    // Both metadata outputs should be non-empty
    assert!(!extract_text(&outputs, "pdf_metadata").is_empty());
    assert!(!extract_text(&outputs, "md_metadata").is_empty());
}

// =============================================================================
// Scenario 6: Model + Dimensions (2 caps, fan-out)
// modelcartridge + candlecartridge: model-spec → availability + candle_dimensions
// =============================================================================

// TEST019: Fan-out from model spec to availability check and embedding dimensions
#[tokio::test]
#[serial]
async fn test953_model_plus_dimensions() {
    let dev_binaries = require_binaries(&["modelcartridge", "candlecartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_availability());
    registry.register(candle_embeddings_dimensions());

    // Pre-download BERT model needed for embeddings dimensions
    let modelcartridge_bin = &dev_binaries.iter().find(|p| {
        p.to_str().map_or(false, |s| s.contains("modelcartridge"))
    }).expect("modelcartridge binary required").clone();
    ensure_model_downloaded(MODEL_BERT, modelcartridge_bin).await;

    let dot = load_scenario_dot("test953_model_plus_dimensions");
    eprintln!("[TEST019] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 2);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // Verify availability output
    let avail = extract_text(&outputs, "availability");
    eprintln!("[TEST019] availability: {}", avail);
    assert!(!avail.is_empty(), "Availability must not be empty");

    // Verify dimensions output (should contain 384 for MiniLM)
    let dim = extract_text(&outputs, "dimensions");
    eprintln!("[TEST019] dimensions: {}", dim);
    assert!(
        dim.contains("384"),
        "MiniLM-L6-v2 should have 384 dimensions, got: {}",
        dim
    );
}

// =============================================================================
// Scenario 7: Model Availability + Status (2 caps, fan-out)
// modelcartridge: model-spec → availability + status
// =============================================================================

// TEST020: Model spec fan-out to availability and status checks
#[tokio::test]
#[serial]
async fn test954_model_availability_plus_status() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_availability());
    registry.register(model_status());

    let dot = load_scenario_dot("test954_model_availability_plus_status");
    eprintln!("[TEST020] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 2);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let avail = extract_text(&outputs, "availability");
    eprintln!("[TEST020] availability: {}", avail);
    assert!(!avail.is_empty());

    let status = extract_text(&outputs, "status");
    eprintln!("[TEST020] status: {}", status);
    assert!(!status.is_empty());
}

// =============================================================================
// Scenario 8: Text Embedding (1 cap, single step)
// candlecartridge: text → BERT embedding vector
// =============================================================================

// TEST021: Generate text embedding with BERT via candlecartridge
#[tokio::test]
#[serial]
async fn test955_text_embedding() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(candle_text_embeddings());

    // Pre-download BERT model needed for text embeddings
    let modelcartridge_bin = &dev_binaries.iter().find(|p| {
        p.to_str().map_or(false, |s| s.contains("modelcartridge"))
    }).expect("modelcartridge binary required").clone();
    ensure_model_downloaded(MODEL_BERT, modelcartridge_bin).await;

    let dot = load_scenario_dot("test955_text_embedding");
    eprintln!("[TEST021] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "text_input".to_string(),
        NodeData::Text("The quick brown fox jumps over the lazy dog.".to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let embedding = extract_text(&outputs, "embedding");
    eprintln!(
        "[TEST021] embedding: {}",
        &embedding[..embedding.len().min(200)]
    );
    assert!(
        embedding.contains("embeddings") || embedding.contains("embedding"),
        "Output should contain embedding vector"
    );
}

// =============================================================================
// Scenario 9: Image Description (1 cap, single step)
// candlecartridge: PNG → BLIP description
// =============================================================================

// TEST022: Generate image description with BLIP via candlecartridge
#[tokio::test]
#[serial]
async fn test956_candle_describe_image() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(candle_describe_image());

    // Pre-download BLIP model needed for image description
    let modelcartridge_bin = &dev_binaries.iter().find(|p| {
        p.to_str().map_or(false, |s| s.contains("modelcartridge"))
    }).expect("modelcartridge binary required").clone();
    ensure_model_downloaded(MODEL_BLIP, modelcartridge_bin).await;

    let dot = load_scenario_dot("test956_candle_describe_image");
    eprintln!("[TEST022] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "image_input".to_string(),
        NodeData::Bytes(generate_test_png(32, 32, 255, 0, 0)), // 32x32 red image
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let description = extract_text(&outputs, "description");
    eprintln!("[TEST022] description: {}", description);
    assert!(!description.is_empty(), "Description must not be empty");
}

// =============================================================================
// Scenario 10: Audio Transcription (1 cap, single step)
// candlecartridge: WAV → Whisper transcription
// =============================================================================

// TEST023: Transcribe audio with Whisper via candlecartridge
#[tokio::test]
#[serial]
async fn test957_audio_transcription() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(candle_transcribe());

    // Pre-download Whisper model needed for audio transcription
    let modelcartridge_bin = &dev_binaries.iter().find(|p| {
        p.to_str().map_or(false, |s| s.contains("modelcartridge"))
    }).expect("modelcartridge binary required").clone();
    ensure_model_downloaded(MODEL_WHISPER, modelcartridge_bin).await;

    let dot = load_scenario_dot("test957_audio_transcription");
    eprintln!("[TEST023] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "audio_input".to_string(),
        NodeData::Bytes(generate_test_wav()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let transcription = extract_text(&outputs, "transcription");
    eprintln!("[TEST023] transcription: {}", transcription);
    // Silence might produce empty transcription or whitespace, but should not error
    assert!(
        outputs.contains_key("transcription"),
        "Transcription output node must exist"
    );
}

// =============================================================================
// Scenario 11: PDF Complete Analysis (4 caps, all pdfcartridge ops)
// pdfcartridge: extract_metadata + extract_outline + generate_thumbnail + disbind
// =============================================================================

// TEST024: All 4 pdfcartridge ops on a single PDF — full document analysis pipeline
#[tokio::test]
#[serial]
async fn test958_pdf_complete_analysis() {
    let dev_binaries = require_binaries(&["pdfcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_extract_metadata());
    registry.register(pdf_extract_outline());
    registry.register(pdf_generate_thumbnail());
    registry.register(pdf_disbind());

    let dot = load_scenario_dot("test958_pdf_complete_analysis");
    eprintln!("[TEST024] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 4, "4 edges expected");
    assert_eq!(graph.nodes.len(), 5, "1 input + 4 outputs");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // All 4 output nodes must exist
    for node in &["metadata", "outline", "thumbnail", "pages"] {
        assert!(outputs.contains_key(*node), "Missing output node '{}'", node);
    }

    // Metadata is JSON with page info
    let meta = extract_text(&outputs, "metadata");
    eprintln!("[TEST024] metadata: {}", &meta[..meta.len().min(200)]);
    assert!(!meta.is_empty());

    // Outline is valid (may be minimal for blank PDF)
    let outline = extract_text(&outputs, "outline");
    eprintln!("[TEST024] outline: {}", &outline[..outline.len().min(200)]);
    assert!(!outline.is_empty());

    // Thumbnail is PNG
    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );

    // Disbind produces page content (may be empty text for blank PDF, but should exist)
    let pages = extract_text(&outputs, "pages");
    eprintln!("[TEST024] pages: {}", &pages[..pages.len().min(200)]);
    assert!(!pages.is_empty(), "Disbind output must not be empty");
}

// =============================================================================
// Scenario 12: Model Full Inspection (4 caps, all non-download modelcartridge ops)
// modelcartridge: availability + status + contents + path
// =============================================================================

// TEST025: All 4 modelcartridge inspection ops on a single model spec
#[tokio::test]
#[serial]
async fn test959_model_full_inspection() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_availability());
    registry.register(model_status());
    registry.register(model_contents());
    registry.register(model_path());

    let dot = load_scenario_dot("test959_model_full_inspection");
    eprintln!("[TEST025] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 4);
    assert_eq!(graph.nodes.len(), 5);

    // model-path requires the model to be locally cached — download first
    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    for node in &["availability", "status", "contents", "path"] {
        assert!(outputs.contains_key(*node), "Missing output node '{}'", node);
    }

    // Availability should indicate local presence
    let avail = extract_text(&outputs, "availability");
    eprintln!("[TEST025] availability: {}", &avail[..avail.len().min(200)]);
    assert!(!avail.is_empty());

    // Status should have state field
    let status = extract_text(&outputs, "status");
    eprintln!("[TEST025] status: {}", &status[..status.len().min(200)]);
    assert!(!status.is_empty());

    // Contents should list model files
    let contents = extract_text(&outputs, "contents");
    eprintln!("[TEST025] contents: {}", &contents[..contents.len().min(300)]);
    assert!(!contents.is_empty());

    // Path should contain filesystem path
    let path = extract_text(&outputs, "path");
    eprintln!("[TEST025] path: {}", &path[..path.len().min(200)]);
    assert!(!path.is_empty());
}

// =============================================================================
// Scenario 13: Two-Format Full Analysis (7 caps, pdf ×4 + md ×3)
// pdfcartridge: metadata + outline + thumbnail + disbind
// txtcartridge: metadata + outline + thumbnail (no disbind — markdown has no pages)
// =============================================================================

// TEST026: 7-cap parallel analysis — all pdf ops + all md ops on two documents
#[tokio::test]
#[serial]
async fn test960_two_format_full_analysis() {
    let dev_binaries = require_binaries(&["pdfcartridge", "txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_extract_metadata());
    registry.register(pdf_extract_outline());
    registry.register(pdf_generate_thumbnail());
    registry.register(pdf_disbind());
    registry.register(md_extract_metadata());
    registry.register(md_extract_outline());
    registry.register(md_generate_thumbnail());

    let dot = load_scenario_dot("test960_two_format_full_analysis");
    eprintln!("[TEST026] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 7, "7 edges expected");
    assert_eq!(graph.nodes.len(), 9, "2 inputs + 7 outputs");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // All 7 output nodes must exist
    let expected_nodes = [
        "pdf_metadata", "pdf_outline", "pdf_thumbnail", "pdf_pages",
        "md_metadata", "md_outline", "md_thumbnail",
    ];
    for node in &expected_nodes {
        assert!(outputs.contains_key(*node), "Missing output node '{}'", node);
    }

    // Both thumbnails must be PNG
    for node in &["pdf_thumbnail", "md_thumbnail"] {
        let thumb = extract_bytes(&outputs, node);
        assert!(
            thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "{} must be valid PNG",
            node
        );
    }

    // All text outputs must be non-empty
    for node in &expected_nodes {
        if !node.contains("thumbnail") {
            let text = extract_text(&outputs, node);
            assert!(!text.is_empty(), "{} must not be empty", node);
        }
    }

    eprintln!(
        "[TEST026] All 7 outputs verified: {} nodes with data",
        outputs.len()
    );
}

// =============================================================================
// Scenario 14: Model + PDF Combined Pipeline (5 caps, 2 sources)
// modelcartridge ×2 + pdfcartridge ×3: model inspection + PDF analysis
// =============================================================================

// TEST027: 5-cap cross-domain pipeline — model inspection + PDF document analysis
#[tokio::test]
#[serial]
async fn test961_model_plus_pdf_combined() {
    let dev_binaries = require_binaries(&["modelcartridge", "pdfcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_availability());
    registry.register(model_status());
    registry.register(pdf_extract_metadata());
    registry.register(pdf_extract_outline());
    registry.register(pdf_generate_thumbnail());

    let dot = load_scenario_dot("test961_model_plus_pdf_combined");
    eprintln!("[TEST027] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 5);
    assert_eq!(graph.nodes.len(), 7); // 2 inputs + 5 outputs

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    for node in &["availability", "status", "metadata", "outline", "thumbnail"] {
        assert!(outputs.contains_key(*node), "Missing output node '{}'", node);
    }

    // Model outputs
    let avail = extract_text(&outputs, "availability");
    eprintln!("[TEST027] availability: {}", &avail[..avail.len().min(200)]);
    assert!(!avail.is_empty());

    let status = extract_text(&outputs, "status");
    eprintln!("[TEST027] status: {}", &status[..status.len().min(200)]);
    assert!(!status.is_empty());

    // PDF outputs
    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );

    assert!(!extract_text(&outputs, "metadata").is_empty());
    assert!(!extract_text(&outputs, "outline").is_empty());

    eprintln!("[TEST027] 5-cap cross-domain pipeline complete");
}

// =============================================================================
// Scenario 15: Three-Cartridge 6-Cap Pipeline (model + pdf + txt)
// modelcartridge ×2 + pdfcartridge ×2 + txtcartridge ×2: 3 sources, 6 caps
// =============================================================================

// TEST028: 6-cap three-cartridge pipeline — model + PDF + markdown analysis
#[tokio::test]
#[serial]
async fn test962_three_cartridge_pipeline() {
    let dev_binaries = require_binaries(&["modelcartridge", "pdfcartridge", "txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_availability());
    registry.register(model_status());
    registry.register(pdf_extract_metadata());
    registry.register(pdf_generate_thumbnail());
    registry.register(md_extract_metadata());
    registry.register(md_generate_thumbnail());

    let dot = load_scenario_dot("test962_three_cartridge_pipeline");
    eprintln!("[TEST028] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 6);
    assert_eq!(graph.nodes.len(), 9); // 3 inputs + 6 outputs

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let expected = [
        "availability", "status",
        "pdf_metadata", "pdf_thumbnail",
        "md_metadata", "md_thumbnail",
    ];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output node '{}'", node);
    }

    // Both thumbnails are PNG
    for node in &["pdf_thumbnail", "md_thumbnail"] {
        let thumb = extract_bytes(&outputs, node);
        assert!(
            thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "{} must be valid PNG",
            node
        );
    }

    // All text outputs non-empty
    for node in &["availability", "status", "pdf_metadata", "md_metadata"] {
        assert!(!extract_text(&outputs, node).is_empty(), "{} must not be empty", node);
    }

    eprintln!(
        "[TEST028] 6-cap three-cartridge pipeline complete: {} outputs",
        outputs.len()
    );
}

// =============================================================================
// Additional Cap URN Builders
// =============================================================================

// -- txtcartridge txt format (matches txtcartridge's plain-text media type) --

fn txt_generate_thumbnail() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_thumbnail")
        .in_spec("media:txt;textable")
        .out_spec("media:image;png;thumbnail")
        .build()
        .expect("txt generate_thumbnail URN")
}

fn txt_extract_metadata() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_metadata")
        .in_spec("media:txt;textable")
        .out_spec("media:file-metadata;textable;record")
        .build()
        .expect("txt extract_metadata URN")
}

fn txt_extract_outline() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_outline")
        .in_spec("media:txt;textable")
        .out_spec("media:document-outline;textable;record")
        .build()
        .expect("txt extract_outline URN")
}

// -- txtcartridge rst format --

fn rst_generate_thumbnail() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_thumbnail")
        .in_spec("media:rst;textable")
        .out_spec("media:image;png;thumbnail")
        .build()
        .expect("rst generate_thumbnail URN")
}

fn rst_extract_metadata() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_metadata")
        .in_spec("media:rst;textable")
        .out_spec("media:file-metadata;textable;record")
        .build()
        .expect("rst extract_metadata URN")
}

fn rst_extract_outline() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_outline")
        .in_spec("media:rst;textable")
        .out_spec("media:document-outline;textable;record")
        .build()
        .expect("rst extract_outline URN")
}

// -- txtcartridge log format --

fn log_generate_thumbnail() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_thumbnail")
        .in_spec("media:log;textable")
        .out_spec("media:image;png;thumbnail")
        .build()
        .expect("log generate_thumbnail URN")
}

fn log_extract_metadata() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_metadata")
        .in_spec("media:log;textable")
        .out_spec("media:file-metadata;textable;record")
        .build()
        .expect("log extract_metadata URN")
}

fn log_extract_outline() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "extract_outline")
        .in_spec("media:log;textable")
        .out_spec("media:document-outline;textable;record")
        .build()
        .expect("log extract_outline URN")
}

// -- modelcartridge list-models --

fn model_list_models() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "list-models")
        .in_spec("media:model-repo;textable;record")
        .out_spec("media:model-list;textable;record")
        .build()
        .expect("model list-models URN")
}

// -- ggufcartridge caps (mirrors exact builder calls in ggufcartridge/src/main.rs) --

fn gguf_embeddings_dimensions() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "embeddings_dimensions")
        .solo_tag("ml-model")
        .solo_tag("gguf")
        .in_spec("media:model-spec;textable")
        .out_spec("media:integer;textable;numeric")
        .build()
        .expect("gguf embeddings_dimensions URN")
}

fn gguf_llm_model_info() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "llm_model_info")
        .solo_tag("llm")
        .solo_tag("ml-model")
        .solo_tag("gguf")
        .in_spec("media:llm-generation-request;json;record")
        .out_spec("media:llm-model-info;json;record")
        .build()
        .expect("gguf llm_model_info URN")
}

fn gguf_llm_vocab() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "llm_vocab")
        .solo_tag("llm")
        .solo_tag("ml-model")
        .solo_tag("gguf")
        .in_spec("media:llm-generation-request;json;record")
        .out_spec("media:llm-vocab-response;json;record")
        .build()
        .expect("gguf llm_vocab URN")
}

fn gguf_llm_inference() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "llm_inference")
        .solo_tag("llm")
        .solo_tag("ml-model")
        .solo_tag("gguf")
        .in_spec("media:llm-generation-request;json;record")
        .out_spec("media:llm-text-stream;ndjson;streaming")
        .build()
        .expect("gguf llm_inference URN")
}

fn gguf_llm_inference_constrained() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "llm_inference_constrained")
        .solo_tag("constrained")
        .solo_tag("llm")
        .solo_tag("ml-model")
        .solo_tag("gguf")
        .in_spec("media:llm-generation-request;json;record")
        .out_spec("media:llm-text-stream;ndjson;streaming")
        .build()
        .expect("gguf llm_inference_constrained URN")
}

fn gguf_generate_embeddings() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "generate_embeddings")
        .solo_tag("ml-model")
        .solo_tag("gguf")
        .in_spec("media:textable")
        .out_spec("media:embedding-vector;textable;record")
        .build()
        .expect("gguf generate_embeddings URN")
}

fn gguf_describe_image() -> CapUrn {
    CapUrnBuilder::new()
        .tag("op", "describe_image")
        .solo_tag("vision")
        .solo_tag("ml-model")
        .solo_tag("gguf")
        .in_spec("media:image;png")
        .out_spec("media:image-description;textable")
        .build()
        .expect("gguf describe_image URN")
}

// =============================================================================
// Additional Model Constants (GGUF)
// =============================================================================

/// Small GGUF embedding model (~84MB) for embedding tests
const MODEL_GGUF_EMBED: &str =
    "hf:nomic-ai/nomic-embed-text-v1.5-GGUF?include=nomic-embed-text-v1.5.Q4_K_M.gguf";

/// Small 0.5B GGUF LLM for generation tests (~320MB)
const MODEL_GGUF_LLM: &str =
    "hf:bartowski/Qwen2.5-0.5B-Instruct-GGUF?include=Qwen2.5-0.5B-Instruct-Q4_K_M.gguf";

/// Small GGUF vision model for image analysis tests (~1.8GB, test skips if not present)
const MODEL_GGUF_VISION: &str =
    "hf:moondream/moondream2-gguf?include=moondream2-mmproj-f16.gguf,moondream2-text-model-f16.gguf";

// =============================================================================
// Additional Test Fixtures
// =============================================================================

/// Generate a simple plain-text document.
fn generate_test_txt() -> Vec<u8> {
    b"Hello World\n\nThis is a plain text document.\nIt has multiple lines of content for testing.\n\nAnother paragraph here.".to_vec()
}

/// Generate a reStructuredText document with headers.
fn generate_test_rst() -> Vec<u8> {
    b"Test RST Document\n=================\n\nSection One\n-----------\n\nThis is a reStructuredText document.\nIt has proper RST formatting.\n\nSection Two\n-----------\n\nMore content in section two.\n".to_vec()
}

/// Generate a log file with timestamped entries.
fn generate_test_log() -> Vec<u8> {
    b"2024-01-01 12:00:00 INFO Application started\n2024-01-01 12:00:01 INFO Processing files\n2024-01-01 12:00:02 WARN Low memory warning\n2024-01-01 12:00:03 ERROR File not found: /tmp/test.txt\n2024-01-01 12:00:04 INFO Shutdown complete\n".to_vec()
}

/// Build a minimal LLM generation request as JSON bytes.
/// model_spec and prompt are the only required fields for ggufcartridge LLM caps.
fn build_llm_request(model_spec: &str, prompt: &str) -> Vec<u8> {
    // Escape special JSON chars in model_spec and prompt (HF specs use ? * / : which are safe)
    let ms = model_spec.replace('\\', "\\\\").replace('"', "\\\"");
    let pr = prompt.replace('\\', "\\\\").replace('"', "\\\"");
    format!(r#"{{"model_spec":"{ms}","prompt":"{pr}"}}"#).into_bytes()
}

/// Build an LLM generation request with a JSON schema constraint.
fn build_llm_constrained_request(model_spec: &str, prompt: &str) -> Vec<u8> {
    let ms = model_spec.replace('\\', "\\\\").replace('"', "\\\"");
    let pr = prompt.replace('\\', "\\\\").replace('"', "\\\"");
    // Minimal JSON schema: object with a single "result" string field
    format!(
        r#"{{"model_spec":"{ms}","prompt":"{pr}","json_schema":{{"type":"object","properties":{{"result":{{"type":"string"}}}},"required":["result"]}}}}"#
    ).into_bytes()
}

// =============================================================================
// Scenario 16: txtcartridge Plain Text Format (3 caps, fan-out)
// txtcartridge: extract_metadata + extract_outline + generate_thumbnail on .txt
// =============================================================================

// TEST029: Plain text fan-out produces metadata, outline, and thumbnail from txt input
#[tokio::test]
#[serial]
async fn test963_txt_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(txt_extract_metadata());
    registry.register(txt_extract_outline());
    registry.register(txt_generate_thumbnail());

    let dot = load_scenario_dot("test963_txt_document_intelligence");
    eprintln!("[TEST029] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 3);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("txt_input".to_string(), NodeData::Bytes(generate_test_txt()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let meta = extract_text(&outputs, "metadata");
    eprintln!("[TEST029] metadata: {}", &meta[..meta.len().min(200)]);
    assert!(!meta.is_empty(), "txt metadata must not be empty");

    let outline = extract_text(&outputs, "outline");
    eprintln!("[TEST029] outline: {}", &outline[..outline.len().min(200)]);
    assert!(!outline.is_empty(), "txt outline must not be empty");

    let thumb = extract_bytes(&outputs, "thumbnail");
    eprintln!("[TEST029] thumbnail: {} bytes", thumb.len());
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "txt thumbnail must be valid PNG"
    );
}

// =============================================================================
// Scenario 17: txtcartridge RST Format (3 caps, fan-out)
// txtcartridge: extract_metadata + extract_outline + generate_thumbnail on .rst
// =============================================================================

// TEST030: RST document fan-out produces metadata, outline (with headers), and thumbnail
#[tokio::test]
#[serial]
async fn test964_rst_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(rst_extract_metadata());
    registry.register(rst_extract_outline());
    registry.register(rst_generate_thumbnail());

    let dot = load_scenario_dot("test964_rst_document_intelligence");
    eprintln!("[TEST030] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 3);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("rst_input".to_string(), NodeData::Bytes(generate_test_rst()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let meta = extract_text(&outputs, "metadata");
    eprintln!("[TEST030] metadata: {}", &meta[..meta.len().min(200)]);
    assert!(!meta.is_empty(), "rst metadata must not be empty");

    // RST has section headers — outline should have content
    let outline = extract_text(&outputs, "outline");
    eprintln!("[TEST030] outline: {}", &outline[..outline.len().min(200)]);
    assert!(!outline.is_empty(), "rst outline must not be empty");

    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "rst thumbnail must be valid PNG"
    );
}

// =============================================================================
// Scenario 18: txtcartridge Log Format (3 caps, fan-out)
// txtcartridge: extract_metadata + extract_outline + generate_thumbnail on .log
// =============================================================================

// TEST031: Log file fan-out produces metadata, outline, and thumbnail from log input
#[tokio::test]
#[serial]
async fn test965_log_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(log_extract_metadata());
    registry.register(log_extract_outline());
    registry.register(log_generate_thumbnail());

    let dot = load_scenario_dot("test965_log_document_intelligence");
    eprintln!("[TEST031] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 3);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("log_input".to_string(), NodeData::Bytes(generate_test_log()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let meta = extract_text(&outputs, "metadata");
    eprintln!("[TEST031] metadata: {}", &meta[..meta.len().min(200)]);
    assert!(!meta.is_empty(), "log metadata must not be empty");

    // Log files don't have section headers — outline may be minimal but must not error
    let outline = extract_text(&outputs, "outline");
    eprintln!("[TEST031] outline: {}", &outline[..outline.len().min(200)]);
    assert!(!outline.is_empty(), "log outline response must not be empty");

    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "log thumbnail must be valid PNG"
    );
}

// =============================================================================
// Scenario 19: All Four Text Formats in One DAG (12 caps, 4 parallel fan-outs)
// txtcartridge: txt + rst + log + md each → metadata + outline + thumbnail
// =============================================================================

// TEST032: 12-cap DAG processing all four text formats simultaneously
#[tokio::test]
#[serial]
async fn test966_all_text_formats_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(txt_extract_metadata());
    registry.register(txt_extract_outline());
    registry.register(txt_generate_thumbnail());
    registry.register(rst_extract_metadata());
    registry.register(rst_extract_outline());
    registry.register(rst_generate_thumbnail());
    registry.register(log_extract_metadata());
    registry.register(log_extract_outline());
    registry.register(log_generate_thumbnail());
    registry.register(md_extract_metadata());
    registry.register(md_extract_outline());
    registry.register(md_generate_thumbnail());

    let dot = load_scenario_dot("test966_all_text_formats_intelligence");
    eprintln!("[TEST032] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 12, "12 edges expected");
    assert_eq!(graph.nodes.len(), 16, "4 inputs + 12 outputs");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("txt_input".to_string(), NodeData::Bytes(generate_test_txt()));
    inputs.insert("rst_input".to_string(), NodeData::Bytes(generate_test_rst()));
    inputs.insert("log_input".to_string(), NodeData::Bytes(generate_test_log()));
    inputs.insert("md_input".to_string(), NodeData::Bytes(generate_test_markdown()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // All 12 output nodes must have data
    let expected_nodes = [
        "txt_metadata", "txt_outline", "txt_thumbnail",
        "rst_metadata", "rst_outline", "rst_thumbnail",
        "log_metadata", "log_outline", "log_thumbnail",
        "md_metadata", "md_outline", "md_thumbnail",
    ];
    for node in &expected_nodes {
        assert!(outputs.contains_key(*node), "Missing output node '{}'", node);
    }

    // All thumbnails must be valid PNG
    for node in &["txt_thumbnail", "rst_thumbnail", "log_thumbnail", "md_thumbnail"] {
        let thumb = extract_bytes(&outputs, node);
        assert!(
            thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "{} must be valid PNG",
            node
        );
    }

    // All metadata outputs must be non-empty
    for node in &["txt_metadata", "rst_metadata", "log_metadata", "md_metadata"] {
        assert!(!extract_text(&outputs, node).is_empty(), "{} must not be empty", node);
    }

    eprintln!("[TEST032] All 12 outputs verified across 4 text formats");
}

// =============================================================================
// Scenario 20: modelcartridge list-models (1 cap)
// modelcartridge: model-repo → model-list
// =============================================================================

// TEST033: List all locally cached models via modelcartridge
#[tokio::test]
#[serial]
async fn test967_model_list_models() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_list_models());

    let dot = load_scenario_dot("test967_model_list_models");
    eprintln!("[TEST033] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 1);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    // Empty JSON object: list all models without filtering
    inputs.insert("repo_input".to_string(), NodeData::Bytes(b"{}".to_vec()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let result = extract_text(&outputs, "model_list");
    eprintln!("[TEST033] model_list: {}", &result[..result.len().min(300)]);
    assert!(!result.is_empty(), "model list output must not be empty");
    // Should be valid JSON (either empty list or list of cached models)
    assert!(
        result.starts_with('{') || result.starts_with('['),
        "model list must be JSON, got: {}",
        &result[..result.len().min(50)]
    );
}

// =============================================================================
// Scenario 21: ggufcartridge Embedding Dimensions (1 cap)
// ggufcartridge: model-spec → embedding dimensions integer
// =============================================================================

// TEST034: Query GGUF embedding model dimensions via ggufcartridge
#[tokio::test]
#[serial]
async fn test968_gguf_embeddings_dimensions() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_embeddings_dimensions());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_EMBED, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test968_gguf_embeddings_dimensions");
    eprintln!("[TEST034] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_GGUF_EMBED.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let dim_text = extract_text(&outputs, "dimensions");
    eprintln!("[TEST034] dimensions: {}", dim_text);
    // Should be a positive integer (embedding dim for nomic-embed is 768)
    let dim: usize = dim_text.trim().parse()
        .unwrap_or_else(|_| panic!("Dimensions output must be a number, got: {}", dim_text));
    assert!(dim > 0, "Embedding dimensions must be positive, got: {}", dim);
}

// =============================================================================
// Scenario 22: ggufcartridge LLM Model Info (1 cap)
// ggufcartridge: llm-generation-request → llm-model-info
// =============================================================================

// TEST035: Query GGUF model metadata via llm_model_info cap
#[tokio::test]
#[serial]
async fn test969_gguf_llm_model_info() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_llm_model_info());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test969_gguf_llm_model_info");
    eprintln!("[TEST035] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_request(MODEL_GGUF_LLM, " ")),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let info = extract_text(&outputs, "model_info");
    eprintln!("[TEST035] model_info: {}", &info[..info.len().min(300)]);
    assert!(!info.is_empty(), "Model info must not be empty");
    assert!(
        info.contains("model_spec") || info.contains("vocab_size"),
        "Model info must contain metadata fields, got: {}",
        &info[..info.len().min(200)]
    );
}

// =============================================================================
// Scenario 23: ggufcartridge LLM Vocabulary (1 cap)
// ggufcartridge: llm-generation-request → llm-vocab-response
// =============================================================================

// TEST036: Extract vocabulary tokens from a GGUF model via llm_vocab cap
#[tokio::test]
#[serial]
async fn test970_gguf_llm_vocab() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_llm_vocab());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test970_gguf_llm_vocab");
    eprintln!("[TEST036] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_request(MODEL_GGUF_LLM, " ")),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let vocab = extract_text(&outputs, "vocab");
    eprintln!("[TEST036] vocab (first 300): {}", &vocab[..vocab.len().min(300)]);
    assert!(!vocab.is_empty(), "Vocab output must not be empty");
    assert!(
        vocab.contains("vocab") || vocab.contains("vocab_size"),
        "Vocab output must contain vocab data, got: {}",
        &vocab[..vocab.len().min(200)]
    );
}

// =============================================================================
// Scenario 24: ggufcartridge Model Info + Vocab Fan-out (2 caps)
// ggufcartridge: same request → model_info + vocab
// =============================================================================

// TEST037: Fan-out from one LLM request to both model_info and vocab outputs
#[tokio::test]
#[serial]
async fn test971_gguf_model_info_plus_vocab() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_llm_model_info());
    registry.register(gguf_llm_vocab());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test971_gguf_model_info_plus_vocab");
    eprintln!("[TEST037] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 2);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_request(MODEL_GGUF_LLM, " ")),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    assert!(outputs.contains_key("model_info"), "Missing model_info output");
    assert!(outputs.contains_key("vocab"), "Missing vocab output");

    let info = extract_text(&outputs, "model_info");
    eprintln!("[TEST037] model_info: {}", &info[..info.len().min(200)]);
    assert!(!info.is_empty());

    let vocab = extract_text(&outputs, "vocab");
    eprintln!("[TEST037] vocab (first 200): {}", &vocab[..vocab.len().min(200)]);
    assert!(!vocab.is_empty());
}

// =============================================================================
// Scenario 25: ggufcartridge LLM Text Generation (1 cap, streaming)
// ggufcartridge: llm-generation-request → llm-text-stream
// =============================================================================

// TEST038: Generate text with a small GGUF LLM via llm_inference cap
#[tokio::test]
#[serial]
async fn test972_gguf_llm_inference() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_llm_inference());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test972_gguf_llm_inference");
    eprintln!("[TEST038] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_request(
            MODEL_GGUF_LLM,
            "Write a single sentence about the sky.",
        )),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let generation = extract_text(&outputs, "generation");
    eprintln!("[TEST038] generation: {}", &generation[..generation.len().min(300)]);
    assert!(!generation.is_empty(), "Generation output must not be empty");
}

// =============================================================================
// Scenario 26: ggufcartridge Constrained LLM Generation (1 cap)
// ggufcartridge: llm-generation-request (with JSON schema) → llm-text-stream
// =============================================================================

// TEST039: Generate JSON-constrained output with GGUF LLM via llm_inference_constrained cap
#[tokio::test]
#[serial]
async fn test973_gguf_llm_inference_constrained() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_llm_inference_constrained());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test973_gguf_llm_inference_constrained");
    eprintln!("[TEST039] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_constrained_request(
            MODEL_GGUF_LLM,
            "Describe the color blue in one word.",
        )),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let generation = extract_text(&outputs, "generation");
    eprintln!("[TEST039] constrained generation: {}", &generation[..generation.len().min(300)]);
    assert!(!generation.is_empty(), "Constrained generation output must not be empty");
}

// =============================================================================
// Scenario 27: ggufcartridge Text Embeddings (fan-in: text + model-spec)
// ggufcartridge: text_input + model_spec → embedding vector
// The generate_embeddings cap requires both the text stream (media:textable)
// and the model-spec stream (media:model-spec;textable) simultaneously.
// Fan-in via two edges with the same cap URN to the same output node.
// =============================================================================

// TEST040: Generate GGUF text embeddings with fan-in of text and model-spec inputs
#[tokio::test]
#[serial]
async fn test974_gguf_generate_embeddings() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_generate_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_EMBED, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test974_gguf_generate_embeddings");
    eprintln!("[TEST040] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "text_input".to_string(),
        NodeData::Text("The quick brown fox jumps over the lazy dog.".to_string()),
    );
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_GGUF_EMBED.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let embedding = extract_text(&outputs, "embedding");
    eprintln!(
        "[TEST040] embedding: {}",
        &embedding[..embedding.len().min(200)]
    );
    assert!(!embedding.is_empty(), "GGUF embedding output must not be empty");
    assert!(
        embedding.contains("embeddings") || embedding.contains("embedding"),
        "Output should contain embedding vector data"
    );
}

// =============================================================================
// Scenario 28: ggufcartridge Vision Analysis (fan-in: image + model-spec)
// ggufcartridge: image_input + model_spec → llm-text-stream (image analysis)
// =============================================================================

// TEST041: Describe image with GGUF vision model via fan-in of image and model-spec
#[tokio::test]
#[serial]
async fn test975_gguf_describe_image() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_describe_image());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    // Vision model is large (~1.8GB) — pre-download; test proceeds regardless of download outcome
    ensure_model_downloaded(MODEL_GGUF_VISION, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test975_gguf_describe_image");
    eprintln!("[TEST041] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "image_input".to_string(),
        NodeData::Bytes(generate_test_png(64, 64, 100, 149, 237)), // blue image
    );
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_GGUF_VISION.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let description = extract_text(&outputs, "description");
    eprintln!("[TEST041] description: {}", &description[..description.len().min(300)]);
    assert!(!description.is_empty(), "Vision description output must not be empty");
}

// =============================================================================
// Scenario 29: PDF Thumbnail → ggufcartridge Vision Analysis (cross-cartridge chain)
// pdfcartridge → candlecartridge: thumbnail output feeds into gguf vision
// =============================================================================

// TEST042: Cross-cartridge chain: PDF thumbnail piped to GGUF vision analysis
#[tokio::test]
#[serial]
async fn test976_pdf_thumbnail_to_gguf_vision() {
    let dev_binaries =
        require_binaries(&["pdfcartridge", "ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_generate_thumbnail());
    registry.register(gguf_describe_image());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_VISION, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test976_pdf_thumbnail_to_gguf_vision");
    eprintln!("[TEST042] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed — thumbnail→vision media URN compatibility check");
    assert_eq!(graph.edges.len(), 3);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_GGUF_VISION.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    assert!(outputs.contains_key("thumbnail"), "Missing thumbnail output");
    assert!(outputs.contains_key("analysis"), "Missing analysis output");

    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );

    let analysis = extract_text(&outputs, "analysis");
    eprintln!("[TEST042] analysis: {}", &analysis[..analysis.len().min(300)]);
    assert!(!analysis.is_empty(), "Vision analysis output must not be empty");
}

// =============================================================================
// Scenario 30: All 4 ggufcartridge LLM Ops Fan-out (4 caps from same request)
// ggufcartridge: request → model_info + vocab + inference + inference_constrained
// =============================================================================

// TEST043: Fan-out from one LLM request to all 4 ggufcartridge LLM operations
#[tokio::test]
#[serial]
async fn test977_gguf_all_llm_ops() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(gguf_llm_model_info());
    registry.register(gguf_llm_vocab());
    registry.register(gguf_llm_inference());
    registry.register(gguf_llm_inference_constrained());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test977_gguf_all_llm_ops");
    eprintln!("[TEST043] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 4);
    assert_eq!(graph.nodes.len(), 5); // 1 input + 4 outputs

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    // Use constrained request so both inference caps receive valid input
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_constrained_request(
            MODEL_GGUF_LLM,
            "Name one color.",
        )),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let expected = ["model_info", "vocab", "generation", "constrained_generation"];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output node '{}'", node);
    }

    let info = extract_text(&outputs, "model_info");
    eprintln!("[TEST043] model_info: {}", &info[..info.len().min(200)]);
    assert!(!info.is_empty());

    let vocab = extract_text(&outputs, "vocab");
    eprintln!("[TEST043] vocab (first 100): {}", &vocab[..vocab.len().min(100)]);
    assert!(!vocab.is_empty());

    let gen = extract_text(&outputs, "generation");
    eprintln!("[TEST043] generation: {}", &gen[..gen.len().min(200)]);
    assert!(!gen.is_empty());

    let cgen = extract_text(&outputs, "constrained_generation");
    eprintln!(
        "[TEST043] constrained_generation: {}",
        &cgen[..cgen.len().min(200)]
    );
    assert!(!cgen.is_empty());

    eprintln!("[TEST043] All 4 ggufcartridge LLM ops fan-out complete");
}

// =============================================================================
// TEST044-047: MLX Cartridge Tests (macOS only, Swift binary)
// =============================================================================

/// TEST044: MLX text generation
/// Flow: single cap
/// Tests: mlxcartridge generate_text cap
#[tokio::test]
#[ignore] // MLX cartridge requires macOS with Apple Silicon
async fn test978_mlx_generate_text() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(mlx_generate_text());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_LLM, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test978_mlx_generate_text");
    eprintln!("[TEST044] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 1);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_MLX_LLM.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let generated = extract_text(&outputs, "generated_text");
    eprintln!("[TEST044] generated_text: {}", &generated[..generated.len().min(300)]);
    assert!(!generated.is_empty(), "Generated text must not be empty");
}

/// TEST045: MLX describe image
/// Flow: single cap
/// Tests: mlxcartridge describe_image cap (vision)
#[tokio::test]
#[ignore] // MLX cartridge requires macOS with Apple Silicon
async fn test979_mlx_describe_image() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(mlx_describe_image());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_VISION, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test979_mlx_describe_image");
    eprintln!("[TEST045] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 1);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "image_input".to_string(),
        NodeData::Bytes(generate_test_png(100, 100, 255, 0, 0)),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let description = extract_text(&outputs, "description");
    eprintln!("[TEST045] description: {}", &description[..description.len().min(300)]);
    assert!(!description.is_empty(), "Image description must not be empty");
}

/// TEST046: MLX generate embeddings
/// Flow: single cap
/// Tests: mlxcartridge generate_embeddings cap
#[tokio::test]
#[ignore] // MLX cartridge requires macOS with Apple Silicon
async fn test980_mlx_generate_embeddings() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(mlx_generate_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_EMBED, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test980_mlx_generate_embeddings");
    eprintln!("[TEST046] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 1);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "text_input".to_string(),
        NodeData::Bytes(b"Hello, world!".to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let embedding = extract_text(&outputs, "embedding");
    eprintln!("[TEST046] embedding (first 200): {}", &embedding[..embedding.len().min(200)]);
    assert!(!embedding.is_empty(), "Embedding must not be empty");
}

/// TEST047: MLX embeddings dimensions
/// Flow: single cap
/// Tests: mlxcartridge embeddings_dimensions cap
#[tokio::test]
#[ignore] // MLX cartridge requires macOS with Apple Silicon
async fn test981_mlx_embeddings_dimensions() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(mlx_embeddings_dimensions());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_EMBED, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test981_mlx_embeddings_dimensions");
    eprintln!("[TEST047] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 1);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_MLX_EMBED.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let dimensions = extract_text(&outputs, "dimensions");
    eprintln!("[TEST047] dimensions: {}", dimensions);
    assert!(!dimensions.is_empty(), "Dimensions must not be empty");
}

// =============================================================================
// TEST048: Model Download Test
// =============================================================================

/// TEST048: Model download
/// Flow: single cap
/// Tests: modelcartridge download-model cap
#[tokio::test]
#[serial]
async fn test982_model_download() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_download());

    let dot = load_scenario_dot("test982_model_download");
    eprintln!("[TEST048] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 1);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_BERT.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let result = extract_text(&outputs, "download_result");
    eprintln!("[TEST048] download_result: {}", &result[..result.len().min(300)]);
    assert!(!result.is_empty(), "Download result must not be empty");
}

// =============================================================================
// TEST049-056: Complex Flow Pattern Tests (Chains, Fan-in, Fan-out, Parallel)
// =============================================================================

/// TEST049: 3-step chain: PDF → thumbnail → candle describe → text embeddings
/// Flow: CHAIN (3 steps across 2 cartridges + ML inference)
/// Tests: Sequential data transformation across multiple cartridges
#[tokio::test]
#[serial]
async fn test983_pdf_to_thumbnail_to_describe_to_embed() {
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_generate_thumbnail());
    registry.register(candle_describe_image());
    registry.register(candle_text_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BLIP, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test983_pdf_to_thumbnail_to_describe_to_embed");
    eprintln!("[TEST049] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 3, "3-step chain");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    // Verify all intermediate and final outputs
    assert!(outputs.contains_key("thumbnail"), "Missing thumbnail");
    assert!(outputs.contains_key("description"), "Missing description");
    assert!(outputs.contains_key("embedding"), "Missing embedding");

    let embedding = extract_text(&outputs, "embedding");
    eprintln!("[TEST049] embedding (first 200): {}", &embedding[..embedding.len().min(200)]);
    assert!(!embedding.is_empty());

    eprintln!("[TEST049] 3-step chain complete: PDF → thumbnail → describe → embed");
}

/// TEST050: PDF thumbnail to GGUF describe with model_spec fan-in
/// Flow: CHAIN + FAN-IN (thumbnail and model_spec both feed into description)
/// Tests: Multiple inputs converging on single output node
#[tokio::test]
#[serial]
async fn test984_pdf_thumbnail_to_gguf_describe_fanin() {
    let dev_binaries = require_binaries(&["pdfcartridge", "ggufcartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_generate_thumbnail());
    registry.register(gguf_describe_image());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_VISION, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test984_pdf_thumbnail_to_gguf_describe_fanin");
    eprintln!("[TEST050] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    // 3 edges: pdf→thumbnail, thumbnail→description, model_spec→description
    assert_eq!(graph.edges.len(), 3, "Chain + fan-in pattern");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_GGUF_VISION.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let description = extract_text(&outputs, "description");
    eprintln!("[TEST050] description: {}", &description[..description.len().min(300)]);
    assert!(!description.is_empty());

    eprintln!("[TEST050] Chain + fan-in complete");
}

/// TEST051: Audio transcription (single cap test for whisper)
/// Flow: single cap
/// Tests: candlecartridge transcribe cap
#[tokio::test]
#[serial]
async fn test985_audio_transcribe_to_embed() {
    let dev_binaries = require_binaries(&["candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(candle_transcribe());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_WHISPER, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test985_audio_transcribe_to_embed");
    eprintln!("[TEST051] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 1);

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("audio_input".to_string(), NodeData::Bytes(generate_test_wav()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    assert!(outputs.contains_key("transcription"), "Missing transcription");
    let transcription = extract_text(&outputs, "transcription");
    eprintln!("[TEST051] transcription: {}", &transcription[..transcription.len().min(300)]);

    eprintln!("[TEST051] Audio transcription complete");
}

/// TEST052: PDF fan-out with chain: metadata + outline + thumbnail → image embedding
/// Flow: FAN-OUT (3 outputs) + CHAIN (thumbnail → embedding)
/// Tests: Single input fanning out with one branch continuing to ML
#[tokio::test]
#[serial]
async fn test986_pdf_fanout_with_chain() {
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_extract_metadata());
    registry.register(pdf_extract_outline());
    registry.register(pdf_generate_thumbnail());
    registry.register(candle_image_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test986_pdf_fanout_with_chain");
    eprintln!("[TEST052] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 4, "3 fan-out + 1 chain");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let expected = ["metadata", "outline", "thumbnail", "img_embedding"];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    let embedding = extract_text(&outputs, "img_embedding");
    eprintln!("[TEST052] img_embedding (first 200): {}", &embedding[..embedding.len().min(200)]);
    assert!(!embedding.is_empty());

    eprintln!("[TEST052] Fan-out with chain complete");
}

/// TEST053: Multi-format parallel chains: PDF + MD both get thumbnails and embeddings
/// Flow: PARALLEL CHAINS (2 independent chains running in parallel)
/// Tests: Parallel processing of different input formats
#[tokio::test]
#[serial]
async fn test987_multi_format_parallel_chains() {
    let dev_binaries = require_binaries(&["pdfcartridge", "txtcartridge", "candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_generate_thumbnail());
    registry.register(md_generate_thumbnail());
    registry.register(candle_image_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test987_multi_format_parallel_chains");
    eprintln!("[TEST053] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 4, "2 parallel chains × 2 steps");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(b"# Test Document\n\nHello, world!".to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let expected = ["pdf_thumbnail", "pdf_img_embed", "md_thumbnail", "md_img_embed"];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    eprintln!("[TEST053] Multi-format parallel chains complete");
}

/// TEST054: Deep chain with parallel branches from intermediate node
/// Flow: FAN-OUT from input + FAN-OUT from intermediate + CHAIN
/// Tests: Complex graph with branching at multiple levels
#[tokio::test]
#[serial]
async fn test988_deep_chain_with_parallel() {
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(pdf_extract_metadata());
    registry.register(pdf_generate_thumbnail());
    registry.register(candle_describe_image());
    registry.register(candle_text_embeddings());
    registry.register(candle_image_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BLIP, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test988_deep_chain_with_parallel");
    eprintln!("[TEST054] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 5, "Complex 5-edge graph");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let expected = ["metadata", "thumbnail", "description", "desc_embedding", "img_embedding"];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    let desc_embed = extract_text(&outputs, "desc_embedding");
    let img_embed = extract_text(&outputs, "img_embedding");
    eprintln!("[TEST054] desc_embedding (first 100): {}", &desc_embed[..desc_embed.len().min(100)]);
    eprintln!("[TEST054] img_embedding (first 100): {}", &img_embed[..img_embed.len().min(100)]);
    assert!(!desc_embed.is_empty() && !img_embed.is_empty());

    eprintln!("[TEST054] Deep chain with parallel branches complete");
}

/// TEST055: Multi-cartridge stress test with parallel independent paths
/// Flow: Two independent FAN-OUT paths (model management + PDF processing)
/// Tests: 3 cartridges working in parallel on independent data
#[tokio::test]
#[serial]
async fn test989_five_cartridge_chain() {
    let dev_binaries = require_binaries(&["modelcartridge", "pdfcartridge", "candlecartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(model_availability());
    registry.register(model_status());
    registry.register(pdf_generate_thumbnail());
    registry.register(candle_describe_image());
    registry.register(candle_text_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BLIP, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test989_five_cartridge_chain");
    eprintln!("[TEST055] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 5, "5 edges in stress test");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_BERT.as_bytes().to_vec()),
    );
    inputs.insert("pdf_input".to_string(), NodeData::Bytes(generate_test_pdf()));

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let expected = ["availability", "status", "thumbnail", "description", "embedding"];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    eprintln!("[TEST055] Multi-cartridge stress test complete");
}

/// TEST056: All text formats → thumbnails → parallel image embeddings (8 edges)
/// Flow: 4 PARALLEL CHAINS (one for each text format)
/// Tests: Maximum parallelism with 4 independent chains
#[tokio::test]
#[serial]
async fn test990_all_text_formats_to_image_embeds() {
    let dev_binaries = require_binaries(&["txtcartridge", "candlecartridge", "modelcartridge"]);

    let mut registry = CartridgeRegistry::new();
    registry.register(txt_generate_thumbnail());
    registry.register(md_generate_thumbnail());
    registry.register(rst_generate_thumbnail());
    registry.register(log_generate_thumbnail());
    registry.register(candle_image_embeddings());

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let dot = load_scenario_dot("test990_all_text_formats_to_image_embeds");
    eprintln!("[TEST056] DOT:\n{}", dot);

    let graph = parse_dot_to_cap_dag(&dot, &registry)
        .await
        .expect("Parse failed");
    assert_eq!(graph.edges.len(), 8, "4 formats × 2 steps = 8 edges");

    let (_temp, plugin_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "txt_input".to_string(),
        NodeData::Bytes(b"Plain text content for testing".to_vec()),
    );
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(b"# Markdown\n\nContent here".to_vec()),
    );
    inputs.insert(
        "rst_input".to_string(),
        NodeData::Bytes(b"Title\n=====\n\nRST content".to_vec()),
    );
    inputs.insert(
        "log_input".to_string(),
        NodeData::Bytes(b"2024-01-01 INFO: Log entry for testing".to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        plugin_dir,
        "https://machinefabric.com/api/plugins".to_string(),
        inputs,
        dev_bins,
    )
    .await
    .expect("Execution failed");

    let expected = [
        "txt_thumbnail", "txt_img_embed",
        "md_thumbnail", "md_img_embed",
        "rst_thumbnail", "rst_img_embed",
        "log_thumbnail", "log_img_embed",
    ];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    eprintln!("[TEST056] All text formats → image embeddings (8 parallel chains) complete");
}
