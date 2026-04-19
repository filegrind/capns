//! Real-world multi-cartridge chain tests for capdag orchestrator
//!
//! Unlike the testcartridge integration tests (which use synthetic test caps),
//! these tests exercise real cartridges (pdfcartridge, txtcartridge, modelcartridge,
//! candlecartridge, ggufcartridge) through multi-step pipelines with real input data.
//!
//! Prerequisites:
//! - Cartridge binaries will be auto-built if missing or outdated
//! - ML-dependent tests require pre-downloaded models

use capdag::orchestrator::{execute_dag, parse_machine_to_cap_dag, NodeData};
use capdag::{CapProgressFn, CapRegistry};
use serial_test::serial;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, LazyLock};
use tempfile::TempDir;

/// Write directly to /dev/tty, bypassing cargo test's stderr capture.
/// Returns None if no TTY is available (e.g., CI).
fn tty_writer() -> Option<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/tty")
        .ok()
}

/// Initialize tracing subscriber that writes to /dev/tty (bypasses cargo capture).
/// Safe to call multiple times — subsequent calls silently no-op.
fn init_tracing() {
    if let Some(tty) = tty_writer() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("warn")
            .with_target(false)
            .with_writer(std::sync::Mutex::new(tty))
            .try_init();
    }
}

/// Per-cap progress entry.
struct CapProgress {
    /// Local progress within this cap [0.0, 1.0]
    pct: f32,
    /// Short label (the op= value from the cap URN)
    label: String,
    /// Last status message from the cartridge
    last_msg: String,
    /// When this cap started executing
    start: std::time::Instant,
    /// Global DAG percentage where this cap's range starts
    global_base: f32,
    /// Global DAG percentage where this cap's range ends (set on completion)
    global_end: f32,
}

/// Multi-cap progress display state.
struct ProgressState {
    caps: Vec<CapProgress>,
    done: bool,
    /// Last seen global percentage — used to detect new cap boundaries
    last_global_pct: f32,
}

impl ProgressState {
    fn new() -> Self {
        Self {
            caps: Vec::new(),
            done: false,
            last_global_pct: 0.0,
        }
    }

    /// Extract a short label from a cap URN (the op= value).
    fn cap_label(cap_urn: &str) -> String {
        if let Some(pos) = cap_urn.find(";op=") {
            let after = &cap_urn[pos + 4..];
            let end = after.find(';').unwrap_or(after.len());
            return after[..end].to_string();
        }
        if let Some(pos) = cap_urn.find("op=") {
            let after = &cap_urn[pos + 3..];
            let end = after.find(';').unwrap_or(after.len());
            return after[..end].to_string();
        }
        if cap_urn.len() > 24 {
            cap_urn[..24].to_string()
        } else {
            cap_urn.to_string()
        }
    }

    fn update(&mut self, global_pct: f32, cap_urn: &str, msg: &str) {
        let label = Self::cap_label(cap_urn);

        if msg == "Completed" {
            // Group finished — mark this cap as done
            if let Some(entry) = self
                .caps
                .iter_mut()
                .find(|c| c.label == label && c.pct < 1.0)
            {
                entry.pct = 1.0;
                entry.global_end = global_pct;
                entry.last_msg = "done".to_string();
            } else {
                self.caps.push(CapProgress {
                    pct: 1.0,
                    label,
                    last_msg: "done".to_string(),
                    start: std::time::Instant::now(),
                    global_base: self.last_global_pct,
                    global_end: global_pct,
                });
            }
            self.last_global_pct = global_pct;
            return;
        }

        // Sub-progress — find or create entry for this cap
        if let Some(entry) = self
            .caps
            .iter_mut()
            .find(|c| c.label == label && c.pct < 1.0)
        {
            let range = entry.global_end - entry.global_base;
            if range > 0.0 {
                entry.pct = ((global_pct - entry.global_base) / range).clamp(0.0, 0.999);
            }
            entry.last_msg.clear();
            entry.last_msg.push_str(msg);
        } else {
            self.caps.push(CapProgress {
                pct: 0.0,
                label,
                last_msg: msg.to_string(),
                start: std::time::Instant::now(),
                global_base: self.last_global_pct,
                global_end: 1.0, // provisional — corrected on "Completed"
            });
        }
    }
}

/// Render all cap progress bars to the bottom N rows of the terminal.
fn render_progress(f: &mut std::fs::File, state: &ProgressState) {
    use std::io::Write;
    let _ = write!(f, "\x1b7"); // save cursor

    if state.done {
        // Erase all progress lines
        for i in 0..state.caps.len() {
            let row_offset = state.caps.len() - i;
            let _ = write!(f, "\x1b[999;1H"); // jump to bottom
            if row_offset > 1 {
                let _ = write!(f, "\x1b[{}A", row_offset - 1); // go up
            }
            let _ = write!(f, "\x1b[K"); // erase line
        }
    } else {
        // Draw each cap on its own row, starting from bottom
        let n = state.caps.len();
        for (i, cap) in state.caps.iter().enumerate() {
            let rows_from_bottom = n - 1 - i;
            let _ = write!(f, "\x1b[999;1H"); // jump to bottom
            if rows_from_bottom > 0 {
                let _ = write!(f, "\x1b[{}A", rows_from_bottom);
            }
            let _ = write!(f, "\x1b[K"); // erase line

            let elapsed = cap.start.elapsed().as_secs_f64();
            let bar_w: usize = 16;
            let label = if cap.label.len() > 24 {
                &cap.label[..24]
            } else {
                &cap.label
            };

            if cap.pct >= 1.0 {
                let _ = write!(
                    f,
                    "  \x1b[32m{label:<24}\x1b[0m [\x1b[32m{}\x1b[0m] {elapsed:5.1}s  \x1b[32m✓\x1b[0m",
                    "█".repeat(bar_w),
                );
            } else {
                let pct = cap.pct * 100.0;
                let filled = (cap.pct * bar_w as f32) as usize;
                let empty = bar_w.saturating_sub(filled + 1);
                let m = if cap.last_msg.len() > 36 {
                    &cap.last_msg[..36]
                } else {
                    &cap.last_msg
                };
                let _ = write!(
                    f,
                    "  \x1b[36m{label:<24}\x1b[0m [\x1b[32m{}\x1b[0m▸{}] \x1b[36m{pct:>5.1}%\x1b[0m {elapsed:5.1}s  {m}",
                    "█".repeat(filled),
                    "·".repeat(empty),
                );
            }
        }
    }

    let _ = write!(f, "\x1b8"); // restore cursor
    let _ = f.flush();
}

/// Build a progress callback with per-cap progress bars at the terminal bottom.
///
/// A background thread refreshes the display every 500ms so elapsed
/// times keep ticking between progress events.
fn test_progress_fn() -> CapProgressFn {
    init_tracing();
    let state = Arc::new(std::sync::Mutex::new(ProgressState::new()));

    // Refresh thread — keeps elapsed timers ticking
    if let Some(mut tty) = tty_writer() {
        let bg_state = Arc::clone(&state);
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(500));
            let s = bg_state.lock().unwrap();
            render_progress(&mut tty, &s);
            if s.done {
                break;
            }
        });
    }

    let tty = tty_writer();
    Arc::new(move |p: f32, cap_urn: &str, msg: &str| {
        {
            let mut s = state.lock().unwrap();
            s.update(p, cap_urn, msg);
            if p >= 1.0 && msg == "Completed" {
                s.done = true;
            }
        }
        if let Some(ref tty) = tty {
            if let Ok(mut f) = tty.try_clone() {
                let s = state.lock().unwrap();
                render_progress(&mut f, &s);
            }
        }
    })
}

// =============================================================================
// Standard Cap Registry — loaded from bundled capgraph definitions
// =============================================================================

/// Shared registry loaded with all standard cap definitions from capgraph.
/// Created once, shared across all tests. This is the same registry that
/// production uses — no hand-built test doubles.
static STANDARD_REGISTRY: LazyLock<Arc<CapRegistry>> = LazyLock::new(|| {
    let registry = CapRegistry::new_for_test();
    let standard_caps = registry
        .get_standard_caps()
        .expect("Failed to load standard caps from bundled definitions");
    registry.add_caps_to_cache(standard_caps);
    Arc::new(registry)
});

fn standard_registry() -> Arc<CapRegistry> {
    STANDARD_REGISTRY.clone()
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
    let output = Command::new("otool").arg("-L").arg(binary_path).output();

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
                eprintln!(
                    "[CartridgeTest] {} binary missing Metal GPU support, will rebuild",
                    name
                );
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
        std::env::var("CAPDAG_GPU_FEATURE")
            .ok()
            .and_then(|v| if v == "cuda" { Some("cuda") } else { None })
            .or(None)
    } else {
        None
    }
}

/// Build a cartridge in debug mode with appropriate features
fn build_cartridge(name: &str) -> Result<(), String> {
    let cart_dir =
        cartridge_dir(name).ok_or_else(|| format!("Cartridge directory not found for {}", name))?;

    // Determine if this cartridge supports GPU and what feature to use
    // Only use the feature if the cartridge actually defines it in Cargo.toml
    let gpu_feature = if GPU_CARTRIDGES.contains(&name) {
        gpu_feature_for_platform().filter(|&feature| cartridge_has_feature(name, feature))
    } else {
        None
    };

    let mut cmd = Command::new("cargo");
    cmd.arg("build").current_dir(&cart_dir);

    if let Some(feature) = gpu_feature {
        cmd.arg("--features").arg(feature);
        eprintln!(
            "[CartridgeTest] Building {} in debug mode with {} GPU acceleration...",
            name, feature
        );
        eprintln!(
            "[CartridgeTest]   Running: cargo build --features {}",
            feature
        );
    } else {
        eprintln!("[CartridgeTest] Building {} in debug mode...", name);
        eprintln!("[CartridgeTest]   Running: cargo build");
    }
    eprintln!("[CartridgeTest]   Directory: {:?}", cart_dir);

    let output = cmd
        .output()
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
        return Err(format!(
            "Failed to build {} (exit code: {:?})",
            name,
            output.status.code()
        ));
    }

    eprintln!("[CartridgeTest] Successfully built {}", name);
    Ok(())
}

/// Find the most recent debug binary for a cartridge.
/// Looks for both unversioned (e.g., `pdfcartridge`) and versioned (e.g., `pdfcartridge-0.93.6217`)
/// names in the cartridge's `target/debug/` directory.
fn find_cartridge_binary(name: &str) -> Option<PathBuf> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").ok()?;
    let manifest_path = PathBuf::from(&manifest_dir);

    // testcartridge is inside capdag/, others are at workspace root
    let bin_dir = if name == "testcartridge" {
        manifest_path.join(name).join("target").join("debug")
    } else {
        manifest_path
            .parent()?
            .join(name)
            .join("target")
            .join("debug")
    };

    if !bin_dir.exists() {
        return None;
    }

    // Try exact name first
    let exact = bin_dir.join(name);
    if exact.is_file() {
        return Some(exact);
    }

    // Try versioned names: find most recent file matching <name>-*
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&bin_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.file_name().and_then(|f| f.to_str()).map_or(false, |f| {
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
    find_cartridge_binary(name).ok_or_else(|| format!("{} binary not found after build", name))
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
// Machine Notation Helpers
// =============================================================================

/// Path to test scenario route files
fn scenarios_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("scenarios")
}

/// Load a machine notation file and parse it into a resolved graph.
/// Also generates a PNG diagram if mmdc is available.
async fn load_and_parse_scenario(name: &str) -> (String, capdag::orchestrator::ResolvedGraph) {
    let route_path = scenarios_dir().join(format!("{}.machine", name));
    let route = std::fs::read_to_string(&route_path)
        .unwrap_or_else(|e| panic!("Failed to read route file {}: {}", route_path.display(), e));
    let graph = parse_machine_to_cap_dag(&route, &*standard_registry())
        .await
        .unwrap_or_else(|e| panic!("Parse failed for {}: {}", name, e));
    generate_diagram(name, &graph);
    (route, graph)
}

/// Generate a PNG diagram from a resolved graph if mmdc is available.
/// Writes to scenarios/{name}.png alongside the .machine file.
fn generate_diagram(name: &str, graph: &capdag::orchestrator::ResolvedGraph) {
    static MMDC_AVAILABLE: LazyLock<bool> =
        LazyLock::new(|| Command::new("mmdc").arg("--version").output().is_ok());

    if !*MMDC_AVAILABLE {
        return;
    }

    let mermaid = graph.to_mermaid();
    let rendered_dir = scenarios_dir().join("rendered");
    Command::new("mkdir")
        .args(["-p", rendered_dir.to_str().unwrap()])
        .output()
        .ok();
    let png_path = rendered_dir.join(format!("{}.png", name));

    let mut child = match Command::new("mmdc")
        .args(["-i", "-", "-o"])
        .arg(&png_path)
        .args(["-s", "2", "-w", "2000"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => return,
    };

    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        let _ = stdin.write_all(mermaid.as_bytes());
    }

    match child.wait() {
        Ok(status) if status.success() => {
            eprintln!("[Diagram] Generated {}.png", name);
        }
        Ok(status) => {
            eprintln!("[Diagram] mmdc failed for {} (exit {})", name, status);
        }
        Err(e) => {
            eprintln!("[Diagram] mmdc error for {}: {}", name, e);
        }
    }
}

// =============================================================================
// ML Model Specs (matching candlecartridge defaults)
// =============================================================================

const MODEL_BERT: &str = "hf:sentence-transformers/all-MiniLM-L6-v2?include=*.json,*.safetensors";
const MODEL_CLIP: &str =
    "hf:openai/clip-vit-base-patch32?include=*.json,*.safetensors,pytorch_model.bin";
const MODEL_BLIP: &str = "hf:Salesforce/blip-image-captioning-large?include=*.json,*.safetensors";
const MODEL_WHISPER: &str = "hf:openai/whisper-base?include=*.json,*.safetensors";

// =============================================================================
// MLX Model Specs (matching mlxcartridge defaults)
// =============================================================================

const MODEL_MLX_LLM: &str = "hf:mlx-community/Llama-3.2-3B-Instruct-4bit";
const MODEL_MLX_VISION: &str = "hf:mlx-community/SmolVLM-Instruct-4bit";
const MODEL_MLX_EMBED: &str = "hf:mlx-community/all-MiniLM-L6-v2-4bit";

// =============================================================================
// Model Pre-Download
// =============================================================================

/// Pre-download ML models via modelcartridge before running ML tests.
/// This prevents ML cartridges from hanging on peer model-download requests
/// during DAG execution. Runs a single-edge DAG: model_spec → download-model.
async fn ensure_model_downloaded(model_spec: &str, modelcartridge_bin: &PathBuf) {
    eprintln!("[PreDownload] Ensuring model is available: {}", model_spec);

    let route = concat!(
        r#"[download cap:in="media:model-spec;textable";op=download-model;out="media:download-result;textable;record"]"#,
        "\n[model_spec -> download -> result]"
    );

    let graph = parse_machine_to_cap_dag(route, &*standard_registry())
        .await
        .expect("Pre-download DAG parse failed");

    let temp = TempDir::new().expect("temp dir");
    let cartridge_dir = temp.path().join("cartridges");
    std::fs::create_dir_all(&cartridge_dir).expect("cartridge dir");

    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(model_spec.to_string()),
    );

    match execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        vec![modelcartridge_bin.clone()],
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    {
        Ok(outputs) => {
            if let Some((node, NodeData::Bytes(b))) = outputs.iter().next() {
                let result = String::from_utf8_lossy(b);
                eprintln!(
                    "[PreDownload] {}: {}",
                    node,
                    &result[..result.len().min(200)]
                );
            }
        }
        Err(e) => {
            panic!(
                "[PreDownload] Model download failed for '{}': {}",
                model_spec, e
            );
        }
    }
}

// =============================================================================
// Test Setup
// =============================================================================

fn setup_test_env(dev_binaries: Vec<PathBuf>) -> (TempDir, PathBuf, Vec<PathBuf>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cartridge_dir = temp_dir.path().join("cartridges");
    std::fs::create_dir_all(&cartridge_dir).expect("Failed to create cartridge dir");
    (temp_dir, cartridge_dir, dev_binaries)
}

fn extract_bytes(outputs: &HashMap<String, NodeData>, node: &str) -> Vec<u8> {
    match outputs
        .get(node)
        .unwrap_or_else(|| panic!("Missing node '{}'", node))
    {
        NodeData::Bytes(b) => b.clone(),
        other => panic!("Expected Bytes at node '{}', got {:?}", node, other),
    }
}

fn extract_text(outputs: &HashMap<String, NodeData>, node: &str) -> String {
    let bytes = extract_bytes(outputs, node);
    String::from_utf8(bytes).unwrap_or_else(|_| panic!("Invalid UTF-8 at node '{}'", node))
}

// =============================================================================
// Scenario 1: PDF Document Intelligence (1 cap, render_page_image)
// pdfcartridge: render_page_image
// =============================================================================

// TEST1069: PDF render_page_image produces a thumbnail from a single PDF input
#[tokio::test]
#[serial]
async fn test1069_pdf_document_intelligence() {
    let dev_binaries = require_binaries(&["pdfcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test948_pdf_document_intelligence").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    // Verify thumbnail is PNG (starts with PNG signature)
    let thumbnail_bytes = extract_bytes(&outputs, "thumbnail");
    eprintln!("[TEST014] thumbnail: {} bytes", thumbnail_bytes.len());
    assert!(
        thumbnail_bytes.len() >= 8
            && thumbnail_bytes[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail should be valid PNG (signature check)"
    );
}

// =============================================================================
// Scenario 2: PDF Thumbnail to Image Embedding (2 caps, linear chain)
// pdfcartridge → candlecartridge (requires parser fix for media URN compatibility)
// =============================================================================

// TEST1070: Cross-cartridge chain: PDF thumbnail piped to CLIP image embedding
#[tokio::test]
#[serial]
async fn test1070_pdf_thumbnail_to_image_embedding() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    // Pre-download CLIP model needed for image embeddings
    let modelcartridge_bin = &dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test949_pdf_thumbnail_to_image_embedding").await;
    assert_eq!(graph.edges.len(), 2);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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
// Scenario 3: PDF Full Intelligence Pipeline (2 caps, chain)
// pdfcartridge + candlecartridge: render_page_image → generate_image_embeddings
// =============================================================================

// TEST881: PDF thumbnail to image embedding pipeline
#[tokio::test]
#[serial]
async fn test881_pdf_full_intelligence_pipeline() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    // Pre-download CLIP model needed for image embeddings
    let modelcartridge_bin = &dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test950_pdf_full_intelligence_pipeline").await;
    assert_eq!(graph.edges.len(), 2);
    assert_eq!(graph.nodes.len(), 3); // pdf_input, thumbnail, img_embedding

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    // Both output nodes must exist
    assert!(
        outputs.contains_key("thumbnail"),
        "Missing thumbnail output"
    );
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

    // Verify embedding has data
    let emb = extract_text(&outputs, "img_embedding");
    assert!(!emb.is_empty(), "Image embedding must not be empty");
}

// =============================================================================
// Scenario 4: Text Document Intelligence (1 cap, render_page_image on markdown)
// txtcartridge: render_page_image
// =============================================================================

// TEST1071: Markdown render_page_image produces thumbnail
#[tokio::test]
#[serial]
async fn test1071_text_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test951_text_document_intelligence").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    // Verify thumbnail is PNG
    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );
}

// =============================================================================
// Scenario 5: Multi-Format Document Processing (2 caps, parallel render_page_image)
// pdfcartridge + txtcartridge: PDF and markdown each get a thumbnail
// =============================================================================

// TEST1072: Parallel processing of PDF and markdown through independent render_page_image
#[tokio::test]
#[serial]
async fn test1072_multi_format_document_processing() {
    let dev_binaries = require_binaries(&["pdfcartridge", "txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test952_multi_format_document_processing").await;
    assert_eq!(graph.edges.len(), 2);
    assert_eq!(graph.nodes.len(), 4); // 2 inputs + 2 outputs

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    for node in &["pdf_thumbnail", "md_thumbnail"] {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
        let thumb = extract_bytes(&outputs, node);
        assert!(
            thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "{} must be valid PNG",
            node
        );
    }
}

// =============================================================================
// Scenario 6: Model + Dimensions (2 caps, fan-out)
// modelcartridge + candlecartridge: model-spec → availability + candle_dimensions
// =============================================================================

// TEST885: Fan-out from model spec to availability check and embedding dimensions
#[tokio::test]
#[serial]
async fn test885_model_plus_dimensions() {
    let dev_binaries = require_binaries(&["modelcartridge", "candlecartridge"]);

    // Pre-download BERT model needed for embeddings dimensions
    let modelcartridge_bin = &dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BERT, modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test953_model_plus_dimensions").await;
    assert_eq!(graph.edges.len(), 2);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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

// TEST884: Model spec fan-out to availability and status checks
#[tokio::test]
#[serial]
async fn test884_model_availability_plus_status() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test954_model_availability_plus_status").await;
    assert_eq!(graph.edges.len(), 2);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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

// TEST883: Generate text embedding with BERT via candlecartridge
#[tokio::test]
#[serial]
async fn test883_text_embedding() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["candlecartridge", "modelcartridge"]);

    // Pre-download BERT model needed for text embeddings
    let modelcartridge_bin = &dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BERT, modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test955_text_embedding").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "text_input".to_string(),
        NodeData::Text("The quick brown fox jumps over the lazy dog.".to_string()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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

// TEST882: Generate image description with BLIP via candlecartridge
#[tokio::test]
#[serial]
async fn test882_candle_describe_image() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["candlecartridge", "modelcartridge"]);

    // Pre-download BLIP model needed for image description
    let modelcartridge_bin = &dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BLIP, modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test956_candle_describe_image").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "image_input".to_string(),
        NodeData::Bytes(generate_test_png(32, 32, 255, 0, 0)), // 32x32 red image
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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

// TEST1032: Transcribe audio with Whisper via candlecartridge
#[tokio::test]
#[serial]
async fn test1032_audio_transcription() {
    // modelcartridge required: candlecartridge sends peer requests for model downloading
    let dev_binaries = require_binaries(&["candlecartridge", "modelcartridge"]);

    // Pre-download Whisper model needed for audio transcription
    let modelcartridge_bin = &dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_WHISPER, modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test957_audio_transcription").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "audio_input".to_string(),
        NodeData::Bytes(generate_test_wav()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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
// Scenario 11: PDF Complete Analysis (2 caps, pdfcartridge ops)
// pdfcartridge: render_page_image + disbind
// =============================================================================

// TEST1034: pdfcartridge ops on a single PDF — thumbnail + disbind pipeline
#[tokio::test]
#[serial]
async fn test1034_pdf_complete_analysis() {
    let dev_binaries = require_binaries(&["pdfcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test958_pdf_complete_analysis").await;
    assert_eq!(graph.edges.len(), 2, "2 edges expected");
    assert_eq!(graph.nodes.len(), 3, "1 input + 2 outputs");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    // Both output nodes must exist
    for node in &["thumbnail", "pages"] {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
    }

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

// TEST1035: All 4 modelcartridge inspection ops on a single model spec
#[tokio::test]
#[serial]
async fn test1035_model_full_inspection() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test959_model_full_inspection").await;
    assert_eq!(graph.edges.len(), 4);
    assert_eq!(graph.nodes.len(), 5);

    // model-path requires the model to be locally cached — download first
    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    for node in &["availability", "status", "contents", "path"] {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
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
    eprintln!(
        "[TEST025] contents: {}",
        &contents[..contents.len().min(300)]
    );
    assert!(!contents.is_empty());

    // Path should contain filesystem path
    let path = extract_text(&outputs, "path");
    eprintln!("[TEST025] path: {}", &path[..path.len().min(200)]);
    assert!(!path.is_empty());
}

// =============================================================================
// Scenario 13: Two-Format Full Analysis (3 caps, pdf ×2 + md ×1)
// pdfcartridge: render_page_image + disbind
// txtcartridge: render_page_image
// =============================================================================

// TEST1037: 3-cap parallel analysis — pdf thumbnail + disbind + md thumbnail
#[tokio::test]
#[serial]
async fn test1037_two_format_full_analysis() {
    let dev_binaries = require_binaries(&["pdfcartridge", "txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test960_two_format_full_analysis").await;
    assert_eq!(graph.edges.len(), 3, "3 edges expected");
    assert_eq!(graph.nodes.len(), 5, "2 inputs + 3 outputs");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    // 3 output nodes must exist: pdf thumbnail, pdf pages, md thumbnail
    for node in &["pdf_thumbnail", "pdf_pages", "md_thumbnail"] {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
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

    let pages = extract_text(&outputs, "pdf_pages");
    assert!(!pages.is_empty(), "pdf_pages must not be empty");

    eprintln!(
        "[TEST026] All 3 outputs verified: {} nodes with data",
        outputs.len()
    );
}

// =============================================================================
// Scenario 14: Model + PDF Combined Pipeline (3 caps, 2 sources)
// modelcartridge ×2 + pdfcartridge ×1: model availability/status + PDF thumbnail
// =============================================================================

// TEST1038: 3-cap cross-domain pipeline — model inspection + PDF thumbnail
#[tokio::test]
#[serial]
async fn test1038_model_plus_pdf_combined() {
    let dev_binaries = require_binaries(&["modelcartridge", "pdfcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test961_model_plus_pdf_combined").await;
    assert_eq!(graph.edges.len(), 3);
    assert_eq!(graph.nodes.len(), 5); // 2 inputs + 3 outputs

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    for node in &["availability", "status", "thumbnail"] {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
    }

    // Model outputs
    let avail = extract_text(&outputs, "availability");
    eprintln!("[TEST027] availability: {}", &avail[..avail.len().min(200)]);
    assert!(!avail.is_empty());

    let status = extract_text(&outputs, "status");
    eprintln!("[TEST027] status: {}", &status[..status.len().min(200)]);
    assert!(!status.is_empty());

    // PDF thumbnail
    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );

    eprintln!("[TEST027] 3-cap cross-domain pipeline complete");
}

// =============================================================================
// Scenario 15: Three-Cartridge 4-Cap Pipeline (model + pdf + txt)
// modelcartridge ×2 + pdfcartridge ×1 + txtcartridge ×1: 3 sources, 4 caps
// =============================================================================

// TEST1040: 4-cap three-cartridge pipeline — model availability/status + PDF + md thumbnails
#[tokio::test]
#[serial]
async fn test1040_three_cartridge_pipeline() {
    let dev_binaries = require_binaries(&["modelcartridge", "pdfcartridge", "txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test962_three_cartridge_pipeline").await;
    assert_eq!(graph.edges.len(), 4);
    assert_eq!(graph.nodes.len(), 7); // 3 inputs + 4 outputs

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_BERT.to_string()),
    );
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let expected = ["availability", "status", "pdf_thumbnail", "md_thumbnail"];
    for node in &expected {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
    }

    // Both thumbnails are PNG
    for node in &["pdf_thumbnail", "md_thumbnail"] {
        let thumb = extract_bytes(&outputs, node);
        eprintln!("[TEST028] {}: {} bytes, first 8: {:?}", node, thumb.len(), &thumb[..thumb.len().min(8)]);
        assert!(
            thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "{} must be valid PNG (got {} bytes, first 8: {:?})",
            node,
            thumb.len(),
            &thumb[..thumb.len().min(8)]
        );
    }

    // Model outputs non-empty
    for node in &["availability", "status"] {
        assert!(
            !extract_text(&outputs, node).is_empty(),
            "{} must not be empty",
            node
        );
    }

    eprintln!(
        "[TEST028] 4-cap three-cartridge pipeline complete: {} outputs",
        outputs.len()
    );
}

// =============================================================================
// GGUF Model Constants
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
// Scenario 16: txtcartridge Plain Text Format (1 cap, render_page_image on .txt)
// txtcartridge: render_page_image
// =============================================================================

// TEST1041: Plain text render_page_image produces thumbnail from txt input
#[tokio::test]
#[serial]
async fn test1041_txt_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test963_txt_document_intelligence").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "txt_input".to_string(),
        NodeData::Bytes(generate_test_txt()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let thumb = extract_bytes(&outputs, "thumbnail");
    eprintln!("[TEST029] thumbnail: {} bytes", thumb.len());
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "txt thumbnail must be valid PNG"
    );
}

// =============================================================================
// Scenario 17: txtcartridge RST Format (1 cap, render_page_image on .rst)
// txtcartridge: render_page_image
// =============================================================================

// TEST1042: RST document render_page_image produces thumbnail
#[tokio::test]
#[serial]
async fn test1042_rst_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test964_rst_document_intelligence").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "rst_input".to_string(),
        NodeData::Bytes(generate_test_rst()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "rst thumbnail must be valid PNG"
    );
}

// =============================================================================
// Scenario 18: txtcartridge Log Format (1 cap, render_page_image on .log)
// txtcartridge: render_page_image
// =============================================================================

// TEST1043: Log file render_page_image produces thumbnail from log input
#[tokio::test]
#[serial]
async fn test1043_log_document_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test965_log_document_intelligence").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "log_input".to_string(),
        NodeData::Bytes(generate_test_log()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "log thumbnail must be valid PNG"
    );
    eprintln!("[TEST031] thumbnail: {} bytes", thumb.len());
}

// =============================================================================
// Scenario 19: All Four Text Formats in One DAG (4 caps, 4 parallel render_page_image)
// txtcartridge: txt + rst + log + md each → thumbnail
// =============================================================================

// TEST1044: 4-cap DAG processing all four text formats simultaneously
#[tokio::test]
#[serial]
async fn test1044_all_text_formats_intelligence() {
    let dev_binaries = require_binaries(&["txtcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test966_all_text_formats_intelligence").await;
    // md: render_page_image, txt: render_page_image, rst: render_page_image, log: render_page_image = 4
    assert_eq!(graph.edges.len(), 4, "4 edges expected");
    // 4 inputs + 4 outputs = 8 nodes
    assert_eq!(graph.nodes.len(), 8, "4 inputs + 4 outputs");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "txt_input".to_string(),
        NodeData::Bytes(generate_test_txt()),
    );
    inputs.insert(
        "rst_input".to_string(),
        NodeData::Bytes(generate_test_rst()),
    );
    inputs.insert(
        "log_input".to_string(),
        NodeData::Bytes(generate_test_log()),
    );
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(generate_test_markdown()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    // 4 output nodes (one thumbnail per format)
    let expected_nodes = [
        "txt_thumbnail",
        "rst_thumbnail",
        "log_thumbnail",
        "md_thumbnail",
    ];
    for node in &expected_nodes {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
        let thumb = extract_bytes(&outputs, node);
        assert!(
            thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "{} must be valid PNG",
            node
        );
    }

    eprintln!("[TEST032] All 4 thumbnails verified across 4 text formats");
}

// =============================================================================
// Scenario 20: modelcartridge list-models (1 cap)
// modelcartridge: model-repo → model-list
// =============================================================================

// TEST1046: List all locally cached models via modelcartridge
#[tokio::test]
#[serial]
async fn test1046_model_list_models() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test967_model_list_models").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    // Empty JSON object: list all models without filtering
    inputs.insert("repo_input".to_string(), NodeData::Bytes(b"{}".to_vec()));

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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

// TEST1048: Query GGUF embedding model dimensions via ggufcartridge
#[tokio::test]
#[serial]
async fn test1048_gguf_embeddings_dimensions() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_EMBED, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test968_gguf_embeddings_dimensions").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_GGUF_EMBED.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let dim_text = extract_text(&outputs, "dimensions");
    eprintln!("[TEST034] dimensions: {}", dim_text);
    // Should be a positive integer (embedding dim for nomic-embed is 768)
    let dim: usize = dim_text
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("Dimensions output must be a number, got: {}", dim_text));
    assert!(
        dim > 0,
        "Embedding dimensions must be positive, got: {}",
        dim
    );
}

// =============================================================================
// Scenario 22: ggufcartridge LLM Model Info (1 cap)
// ggufcartridge: llm-generation-request → llm-model-info
// =============================================================================

// TEST1049: Query GGUF model metadata via llm_model_info cap
#[tokio::test]
#[serial]
async fn test1049_gguf_llm_model_info() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test969_gguf_llm_model_info").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_request(MODEL_GGUF_LLM, " ")),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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

// TEST1050: Extract vocabulary tokens from a GGUF model via llm_vocab cap
#[tokio::test]
#[serial]
async fn test1050_gguf_llm_vocab() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test970_gguf_llm_vocab").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_request(MODEL_GGUF_LLM, " ")),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let vocab = extract_text(&outputs, "vocab");
    eprintln!(
        "[TEST036] vocab (first 300): {}",
        &vocab[..vocab.len().min(300)]
    );
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

// TEST1051: Fan-out from one LLM request to both model_info and vocab outputs
#[tokio::test]
#[serial]
async fn test1051_gguf_model_info_plus_vocab() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test971_gguf_model_info_plus_vocab").await;
    assert_eq!(graph.edges.len(), 2);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "request_input".to_string(),
        NodeData::Bytes(build_llm_request(MODEL_GGUF_LLM, " ")),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    assert!(
        outputs.contains_key("model_info"),
        "Missing model_info output"
    );
    assert!(outputs.contains_key("vocab"), "Missing vocab output");

    let info = extract_text(&outputs, "model_info");
    eprintln!("[TEST037] model_info: {}", &info[..info.len().min(200)]);
    assert!(!info.is_empty());

    let vocab = extract_text(&outputs, "vocab");
    eprintln!(
        "[TEST037] vocab (first 200): {}",
        &vocab[..vocab.len().min(200)]
    );
    assert!(!vocab.is_empty());
}

// =============================================================================
// Scenario 25: ggufcartridge LLM Text Generation (1 cap, streaming)
// ggufcartridge: llm-generation-request → llm-text-stream
// =============================================================================

// TEST1052: Generate text with a small GGUF LLM via llm_inference cap
#[tokio::test]
#[serial]
async fn test1052_gguf_llm_inference() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test972_gguf_llm_inference").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
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
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let generation = extract_text(&outputs, "generation");
    eprintln!(
        "[TEST038] generation: {}",
        &generation[..generation.len().min(300)]
    );
    assert!(
        !generation.is_empty(),
        "Generation output must not be empty"
    );
}

// =============================================================================
// Scenario 26: ggufcartridge Constrained LLM Generation (1 cap)
// ggufcartridge: llm-generation-request (with JSON schema) → llm-text-stream
// =============================================================================

// TEST1053: Generate JSON-constrained output with GGUF LLM via llm_inference_constrained cap
#[tokio::test]
#[serial]
async fn test1053_gguf_llm_inference_constrained() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test973_gguf_llm_inference_constrained").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
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
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let generation = extract_text(&outputs, "generation");
    eprintln!(
        "[TEST039] constrained generation: {}",
        &generation[..generation.len().min(300)]
    );
    assert!(
        !generation.is_empty(),
        "Constrained generation output must not be empty"
    );
}

// =============================================================================
// Scenario 27: ggufcartridge Text Embeddings (fan-in: text + model-spec)
// ggufcartridge: text_input + model_spec → embedding vector
// The generate_embeddings cap requires both the text stream (media:textable)
// and the model-spec stream (media:model-spec;textable) simultaneously.
// Fan-in via two edges with the same cap URN to the same output node.
// =============================================================================

// TEST1054: Generate GGUF text embeddings with fan-in of text and model-spec inputs
#[tokio::test]
#[serial]
async fn test1054_gguf_generate_embeddings() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_EMBED, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test974_gguf_generate_embeddings").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
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
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let embedding = extract_text(&outputs, "embedding");
    eprintln!(
        "[TEST040] embedding: {}",
        &embedding[..embedding.len().min(200)]
    );
    assert!(
        !embedding.is_empty(),
        "GGUF embedding output must not be empty"
    );
    assert!(
        embedding.contains("embeddings") || embedding.contains("embedding"),
        "Output should contain embedding vector data"
    );
}

// =============================================================================
// Scenario 28: ggufcartridge Vision Analysis (fan-in: image + model-spec)
// ggufcartridge: image_input + model_spec → llm-text-stream (image analysis)
// =============================================================================

// TEST1057: Describe image with GGUF vision model via fan-in of image and model-spec
#[tokio::test]
#[serial]
async fn test1057_gguf_describe_image() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    // Vision model is large (~1.8GB) — pre-download; test proceeds regardless of download outcome
    ensure_model_downloaded(MODEL_GGUF_VISION, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test975_gguf_describe_image").await;

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
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
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let description = extract_text(&outputs, "description");
    eprintln!(
        "[TEST041] description: {}",
        &description[..description.len().min(300)]
    );
    assert!(
        !description.is_empty(),
        "Vision description output must not be empty"
    );
}

// =============================================================================
// Scenario 29: PDF Thumbnail → ggufcartridge Vision Analysis (cross-cartridge chain)
// pdfcartridge → candlecartridge: thumbnail output feeds into gguf vision
// =============================================================================

// TEST1058: Cross-cartridge chain: PDF thumbnail piped to GGUF vision analysis
#[tokio::test]
#[serial]
async fn test1058_pdf_thumbnail_to_gguf_vision() {
    let dev_binaries = require_binaries(&["pdfcartridge", "ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_VISION, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test976_pdf_thumbnail_to_gguf_vision").await;
    assert_eq!(graph.edges.len(), 3);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Text(MODEL_GGUF_VISION.to_string()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    assert!(
        outputs.contains_key("thumbnail"),
        "Missing thumbnail output"
    );
    assert!(outputs.contains_key("analysis"), "Missing analysis output");

    let thumb = extract_bytes(&outputs, "thumbnail");
    assert!(
        thumb.len() >= 8 && thumb[..8] == [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        "Thumbnail must be valid PNG"
    );

    let analysis = extract_text(&outputs, "analysis");
    eprintln!(
        "[TEST042] analysis: {}",
        &analysis[..analysis.len().min(300)]
    );
    assert!(
        !analysis.is_empty(),
        "Vision analysis output must not be empty"
    );
}

// =============================================================================
// Scenario 30: All 4 ggufcartridge LLM Ops Fan-out (4 caps from same request)
// ggufcartridge: request → model_info + vocab + inference + inference_constrained
// =============================================================================

// TEST1059: Fan-out from one LLM request to all 4 ggufcartridge LLM operations
#[tokio::test]
#[serial]
async fn test1059_gguf_all_llm_ops() {
    let dev_binaries = require_binaries(&["ggufcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_LLM, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test977_gguf_all_llm_ops").await;
    assert_eq!(graph.edges.len(), 4);
    assert_eq!(graph.nodes.len(), 5); // 1 input + 4 outputs

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
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
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let expected = [
        "model_info",
        "vocab",
        "generation",
        "constrained_generation",
    ];
    for node in &expected {
        assert!(
            outputs.contains_key(*node),
            "Missing output node '{}'",
            node
        );
    }

    let info = extract_text(&outputs, "model_info");
    eprintln!("[TEST043] model_info: {}", &info[..info.len().min(200)]);
    assert!(!info.is_empty());

    let vocab = extract_text(&outputs, "vocab");
    eprintln!(
        "[TEST043] vocab (first 100): {}",
        &vocab[..vocab.len().min(100)]
    );
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
async fn test1060_mlx_generate_text() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_LLM, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test978_mlx_generate_text").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_MLX_LLM.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let generated = extract_text(&outputs, "generated_text");
    eprintln!(
        "[TEST044] generated_text: {}",
        &generated[..generated.len().min(300)]
    );
    assert!(!generated.is_empty(), "Generated text must not be empty");
}

/// TEST045: MLX describe image
/// Flow: single cap
/// Tests: mlxcartridge describe_image cap (vision)
#[tokio::test]
async fn test1061_mlx_describe_image() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_VISION, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test979_mlx_describe_image").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "image_input".to_string(),
        NodeData::Bytes(generate_test_png(100, 100, 255, 0, 0)),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let description = extract_text(&outputs, "description");
    eprintln!(
        "[TEST045] description: {}",
        &description[..description.len().min(300)]
    );
    assert!(
        !description.is_empty(),
        "Image description must not be empty"
    );
}

/// TEST046: MLX generate embeddings
/// Flow: single cap
/// Tests: mlxcartridge generate_embeddings cap
#[tokio::test]
async fn test1062_mlx_generate_embeddings() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_EMBED, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test980_mlx_generate_embeddings").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "text_input".to_string(),
        NodeData::Bytes(b"Hello, world!".to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let embedding = extract_text(&outputs, "embedding");
    eprintln!(
        "[TEST046] embedding (first 200): {}",
        &embedding[..embedding.len().min(200)]
    );
    assert!(!embedding.is_empty(), "Embedding must not be empty");
}

/// TEST047: MLX embeddings dimensions
/// Flow: single cap
/// Tests: mlxcartridge embeddings_dimensions cap
#[tokio::test]
async fn test1063_mlx_embeddings_dimensions() {
    let dev_binaries = require_binaries(&["mlxcartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_MLX_EMBED, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test981_mlx_embeddings_dimensions").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_MLX_EMBED.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
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
async fn test1064_model_download() {
    let dev_binaries = require_binaries(&["modelcartridge"]);

    let (_route, graph) = load_and_parse_scenario("test982_model_download").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_BERT.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let result = extract_text(&outputs, "download_result");
    eprintln!(
        "[TEST048] download_result: {}",
        &result[..result.len().min(300)]
    );
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
async fn test1066_pdf_to_thumbnail_to_describe_to_embed() {
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BLIP, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;

    let (_route, graph) =
        load_and_parse_scenario("test983_pdf_to_thumbnail_to_describe_to_embed").await;
    assert_eq!(graph.edges.len(), 3, "3-step chain");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    // Verify all intermediate and final outputs
    assert!(outputs.contains_key("thumbnail"), "Missing thumbnail");
    assert!(outputs.contains_key("description"), "Missing description");
    assert!(outputs.contains_key("embedding"), "Missing embedding");

    let embedding = extract_text(&outputs, "embedding");
    eprintln!(
        "[TEST049] embedding (first 200): {}",
        &embedding[..embedding.len().min(200)]
    );
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

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_GGUF_VISION, &modelcartridge_bin).await;

    let (_route, graph) =
        load_and_parse_scenario("test984_pdf_thumbnail_to_gguf_describe_fanin").await;
    // 3 edges: pdf→thumbnail, thumbnail→description, model_spec→description
    assert_eq!(graph.edges.len(), 3, "Chain + fan-in pattern");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_GGUF_VISION.as_bytes().to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let description = extract_text(&outputs, "description");
    eprintln!(
        "[TEST050] description: {}",
        &description[..description.len().min(300)]
    );
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

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_WHISPER, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test985_audio_transcribe_to_embed").await;
    assert_eq!(graph.edges.len(), 1);

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "audio_input".to_string(),
        NodeData::Bytes(generate_test_wav()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    assert!(
        outputs.contains_key("transcription"),
        "Missing transcription"
    );
    let transcription = extract_text(&outputs, "transcription");
    eprintln!(
        "[TEST051] transcription: {}",
        &transcription[..transcription.len().min(300)]
    );

    eprintln!("[TEST051] Audio transcription complete");
}

/// TEST052: PDF render_page_image chained to image embedding
/// Flow: CHAIN (thumbnail → img_embedding)
/// Tests: pdfcartridge thumbnail piped to candlecartridge image embedding
#[tokio::test]
#[serial]
async fn test986_pdf_fanout_with_chain() {
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test986_pdf_fanout_with_chain").await;
    assert_eq!(graph.edges.len(), 2, "fan-out + chain");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let expected = ["thumbnail", "img_embedding"];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    let embedding = extract_text(&outputs, "img_embedding");
    eprintln!(
        "[TEST052] img_embedding (first 200): {}",
        &embedding[..embedding.len().min(200)]
    );
    assert!(!embedding.is_empty());

    eprintln!("[TEST052] PDF thumbnail to image embedding complete");
}

/// TEST053: Multi-format parallel chains: PDF + MD both get thumbnails and embeddings
/// Flow: PARALLEL CHAINS (2 independent chains running in parallel)
/// Tests: Parallel processing of different input formats
#[tokio::test]
#[serial]
async fn test987_multi_format_parallel_chains() {
    let dev_binaries = require_binaries(&[
        "pdfcartridge",
        "txtcartridge",
        "candlecartridge",
        "modelcartridge",
    ]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test987_multi_format_parallel_chains").await;
    assert_eq!(graph.edges.len(), 4, "2 parallel chains × 2 steps");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );
    inputs.insert(
        "md_input".to_string(),
        NodeData::Bytes(b"# Test Document\n\nHello, world!".to_vec()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let expected = [
        "pdf_thumbnail",
        "pdf_img_embed",
        "md_thumbnail",
        "md_img_embed",
    ];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    eprintln!("[TEST053] Multi-format parallel chains complete");
}

/// TEST054: Deep chain with parallel branches from intermediate node
/// Flow: thumbnail → FAN-OUT (describe_image + img_embedding) + CHAIN (description → desc_embedding)
/// Tests: Complex graph with branching at multiple levels
#[tokio::test]
#[serial]
async fn test988_deep_chain_with_parallel() {
    let dev_binaries = require_binaries(&["pdfcartridge", "candlecartridge", "modelcartridge"]);

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BLIP, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test988_deep_chain_with_parallel").await;
    assert_eq!(graph.edges.len(), 4, "Complex 4-edge graph");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let expected = ["thumbnail", "description", "desc_embedding", "img_embedding"];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    let desc_embed = extract_text(&outputs, "desc_embedding");
    let img_embed = extract_text(&outputs, "img_embedding");
    eprintln!(
        "[TEST054] desc_embedding (first 100): {}",
        &desc_embed[..desc_embed.len().min(100)]
    );
    eprintln!(
        "[TEST054] img_embedding (first 100): {}",
        &img_embed[..img_embed.len().min(100)]
    );
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

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_BLIP, &modelcartridge_bin).await;
    ensure_model_downloaded(MODEL_BERT, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test989_five_cartridge_chain").await;
    assert_eq!(graph.edges.len(), 5, "5 edges in stress test");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
    let mut inputs = HashMap::new();
    inputs.insert(
        "model_spec".to_string(),
        NodeData::Bytes(MODEL_BERT.as_bytes().to_vec()),
    );
    inputs.insert(
        "pdf_input".to_string(),
        NodeData::Bytes(generate_test_pdf()),
    );

    let outputs = execute_dag(
        &graph,
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let expected = [
        "availability",
        "status",
        "thumbnail",
        "description",
        "embedding",
    ];
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

    let modelcartridge_bin = dev_binaries
        .iter()
        .find(|p| p.to_str().map_or(false, |s| s.contains("modelcartridge")))
        .expect("modelcartridge binary required")
        .clone();
    ensure_model_downloaded(MODEL_CLIP, &modelcartridge_bin).await;

    let (_route, graph) = load_and_parse_scenario("test990_all_text_formats_to_image_embeds").await;
    assert_eq!(graph.edges.len(), 8, "4 formats × 2 steps = 8 edges");

    let (_temp, cartridge_dir, dev_bins) = setup_test_env(dev_binaries);
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
        cartridge_dir,
        "https://machinefabric.com/api/cartridges".to_string(),
        inputs,
        dev_bins,
        standard_registry(),
        Some(&test_progress_fn()),
    )
    .await
    .expect("Execution failed");

    let expected = [
        "txt_thumbnail",
        "txt_img_embed",
        "md_thumbnail",
        "md_img_embed",
        "rst_thumbnail",
        "rst_img_embed",
        "log_thumbnail",
        "log_img_embed",
    ];
    for node in &expected {
        assert!(outputs.contains_key(*node), "Missing output '{}'", node);
    }

    eprintln!("[TEST056] All text formats → image embeddings (8 parallel chains) complete");
}
