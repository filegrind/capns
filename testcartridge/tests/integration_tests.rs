//! Integration tests for testcartridge
//!
//! These tests verify the full stack: file-path auto-conversion, stream multiplexing,
//! large payload chunking, and PeerInvoker protocol.

use std::process::Command;
use std::fs;
use tempfile::TempDir;

/// Get path to the testcartridge binary
fn testcartridge_bin() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{}/target/release/testcartridge", manifest_dir)
}

// TEST400: File-path conversion with test-edge1 (scalar file input)
#[test]
fn test400_filepath_conversion_scalar() {
    // Create test file with known content
    let temp = TempDir::new().unwrap();
    let test_file = temp.path().join("input.txt");
    fs::write(&test_file, "TEST CONTENT").unwrap();

    // Run testcartridge with test-edge1 cap
    let output = Command::new(testcartridge_bin())
        .args(&[
            "test-edge1",
            "--prefix", "PREFIX:",
            test_file.to_str().unwrap()
        ])
        .output()
        .expect("Failed to execute testcartridge");

    assert!(output.status.success(), "Command failed: {:?}", output);

    // Verify file was read and processed (not just the path string)
    let result = String::from_utf8(output.stdout).unwrap();
    assert_eq!(result.trim(), "PREFIX:TEST CONTENT");

    // Verify it was NOT just the path string
    assert!(!result.contains(test_file.to_str().unwrap()));
}

// TEST401: File-path array with glob expansion (test-edge3)
#[test]
#[ignore] // Glob expansion not yet implemented
fn test401_filepath_array_glob() {
    let temp = TempDir::new().unwrap();
    fs::write(temp.path().join("file1.txt"), "CONTENT1").unwrap();
    fs::write(temp.path().join("file2.txt"), "CONTENT2").unwrap();

    let glob_pattern = temp.path().join("*.txt").to_str().unwrap().to_string();

    let output = Command::new(testcartridge_bin())
        .args(&["test-edge3", &glob_pattern])
        .output()
        .expect("Failed to execute");

    assert!(output.status.success());

    let result = String::from_utf8(output.stdout).unwrap();
    // Should receive array of processed files
    assert!(result.contains("CONTENT1"));
    assert!(result.contains("CONTENT2"));
}

// TEST402: Large payload auto-chunking (1MB response)
#[test]
fn test402_large_payload_1mb() {
    let output = Command::new(testcartridge_bin())
        .args(&["test-large", "--size", "1048576"])
        .output()
        .expect("Failed to execute");

    assert!(output.status.success(), "Command failed");

    // Verify we received full 1MB
    assert_eq!(output.stdout.len(), 1_048_576);

    // Verify pattern is correct (data preserved across chunks)
    for (i, &byte) in output.stdout.iter().enumerate() {
        assert_eq!(byte, (i % 256) as u8, "Mismatch at byte {}", i);
    }
}

// TEST403: Plugin chain via PeerInvoker (requires CBOR mode, skip for now)
#[test]
#[ignore] // Requires running in CBOR mode with host
fn test403_peer_invoke_chain() {
    // This would require AsyncPluginHost to orchestrate
    // Skip for CLI-only integration tests
}

// TEST404: Multi-argument cap (test-edge5)
// TODO: Multi-input caps (in2, in3) not fully supported yet
// Each file-path arg maps to the primary in_spec instead of its corresponding input
#[test]
#[ignore]
fn test404_multi_argument() {
    let temp = TempDir::new().unwrap();
    let file1 = temp.path().join("arg1.txt");
    let file2 = temp.path().join("arg2.txt");
    fs::write(&file1, "ARG1").unwrap();
    fs::write(&file2, "ARG2").unwrap();

    let output = Command::new(testcartridge_bin())
        .args(&[
            "test-edge5",
            "--separator", "+",
            file1.to_str().unwrap(),
            "--second-input", file2.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to execute");

    assert!(output.status.success());

    let result = String::from_utf8(output.stdout).unwrap();
    assert_eq!(result.trim(), "ARG1+ARG2");
}

// TEST405: Piped stdin input (no file-path conversion)
#[test]
fn test405_piped_stdin() {
    let mut child = Command::new(testcartridge_bin())
        .args(&["test-edge1", "--prefix", ">>>"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("Failed to spawn");

    // Write to stdin
    use std::io::Write;
    let stdin = child.stdin.as_mut().unwrap();
    stdin.write_all(b"PIPED DATA").unwrap();
    drop(stdin); // Close stdin to signal EOF

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());

    let result = String::from_utf8(output.stdout).unwrap();
    assert_eq!(result.trim(), ">>>PIPED DATA");
}

// TEST406: Empty file handling
#[test]
fn test406_empty_file() {
    let temp = TempDir::new().unwrap();
    let test_file = temp.path().join("empty.txt");
    fs::write(&test_file, "").unwrap();

    let output = Command::new(testcartridge_bin())
        .args(&[
            "test-edge1",
            "--prefix", "EMPTY:",
            test_file.to_str().unwrap()
        ])
        .output()
        .expect("Failed to execute");

    assert!(output.status.success());

    let result = String::from_utf8(output.stdout).unwrap();
    assert_eq!(result.trim(), "EMPTY:");
}

// TEST407: UTF-8 file handling (textable constraint)
#[test]
fn test407_utf8_file() {
    let temp = TempDir::new().unwrap();
    let test_file = temp.path().join("utf8.txt");
    let utf8_data = "Hello 世界 🌍"; // Mix of ASCII, CJK, emoji
    fs::write(&test_file, utf8_data).unwrap();

    let output = Command::new(testcartridge_bin())
        .args(&[
            "test-edge1",
            "--prefix", ">>>",
            test_file.to_str().unwrap()
        ])
        .output()
        .expect("Failed to execute");

    assert!(output.status.success());

    let result = String::from_utf8(output.stdout).unwrap();
    assert_eq!(result.trim(), format!(">>>{}", utf8_data));
}

// TEST408: Missing file error handling
#[test]
fn test408_missing_file() {
    let output = Command::new(testcartridge_bin())
        .args(&[
            "test-edge1",
            "/nonexistent/file.txt"
        ])
        .output()
        .expect("Failed to execute");

    // Should fail with error
    assert!(!output.status.success());

    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("Failed to read file") || stderr.contains("No such file"));
}
