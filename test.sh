#!/bin/bash

# run_all_tests.sh - Run all tests in capns and subprojects
# Usage: ./run_all_tests.sh [--release]

set -e  # Exit on first failure

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
BUILD_MODE="--lib"
if [ "$1" = "--release" ]; then
    BUILD_MODE="--release"
    echo -e "${BLUE}Running tests in release mode${NC}"
fi

# Track results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_PROJECTS=()

# Function to run tests for a project
run_tests() {
    local project_dir=$1
    local project_name=$2
    local test_type=$3  # "lib", "bin", "integration", or "all"

    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Testing: ${project_name}${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    cd "$project_dir"

    local test_cmd=""
    case "$test_type" in
        lib)
            test_cmd="cargo test --lib"
            ;;
        bin)
            test_cmd="cargo test --bins"
            ;;
        integration)
            test_cmd="cargo test --tests"
            ;;
        all)
            test_cmd="cargo test"
            ;;
    esac

    if [ "$BUILD_MODE" = "--release" ]; then
        test_cmd="$test_cmd --release"
    fi

    echo -e "${YELLOW}Running: $test_cmd${NC}"

    # Run tests and capture output
    if $test_cmd 2>&1 | tee /tmp/test_output_$$.txt; then
        # Extract test counts from output
        # Sum up all "test result:" lines (excluding doc-tests with 0 passed)
        local total_passed=0
        local total_failed=0

        while IFS= read -r result; do
            local passed=$(echo "$result" | grep -oE "[0-9]+ passed" | grep -oE "[0-9]+")
            local failed=$(echo "$result" | grep -oE "[0-9]+ failed" | grep -oE "[0-9]+")

            if [ -z "$passed" ]; then passed=0; fi
            if [ -z "$failed" ]; then failed=0; fi

            # Skip doc-test results with 0 passed and 0 failed (no real tests)
            if [ "$passed" -gt 0 ] || [ "$failed" -gt 0 ]; then
                total_passed=$((total_passed + passed))
                total_failed=$((total_failed + failed))
            fi
        done < <(grep "test result:" /tmp/test_output_$$.txt)

        TOTAL_TESTS=$((TOTAL_TESTS + total_passed + total_failed))
        PASSED_TESTS=$((PASSED_TESTS + total_passed))

        if [ "$total_failed" -eq 0 ] && [ "$total_passed" -gt 0 ]; then
            echo -e "${GREEN}✓ $project_name: $total_passed tests passed${NC}"
        elif [ "$total_failed" -gt 0 ]; then
            echo -e "${RED}✗ $project_name: $total_failed tests FAILED${NC}"
            FAILED_PROJECTS+=("$project_name")
        else
            echo -e "${YELLOW}⊘ $project_name: No tests found${NC}"
        fi
    else
        echo -e "${RED}✗ $project_name: Tests failed to run${NC}"
        FAILED_PROJECTS+=("$project_name")
    fi

    rm -f /tmp/test_output_$$.txt
}

# Store original directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║          CAPNS Full Test Suite Runner                 ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"

# 1. capns library tests
run_tests "$SCRIPT_DIR" "capns (library)" "lib"

# 2. capns integration tests (if any)
if [ -d "$SCRIPT_DIR/tests" ] && [ "$(ls -A $SCRIPT_DIR/tests/*.rs 2>/dev/null)" ]; then
    run_tests "$SCRIPT_DIR" "capns (integration tests)" "integration"
fi

# 3. testcartridge
if [ -d "$SCRIPT_DIR/testcartridge" ]; then
    run_tests "$SCRIPT_DIR/testcartridge" "testcartridge" "all"
fi

# 4. macino
if [ -d "$SCRIPT_DIR/macino" ]; then
    run_tests "$SCRIPT_DIR/macino" "macino" "all"
fi

# 5. Check for other potential test directories
for dir in "$SCRIPT_DIR"/*/; do
    if [ -f "$dir/Cargo.toml" ]; then
        dir_name=$(basename "$dir")
        # Skip already tested projects
        if [ "$dir_name" != "testcartridge" ] && [ "$dir_name" != "macino" ] && [ "$dir_name" != "target" ]; then
            echo -e "${YELLOW}Found additional project: $dir_name${NC}"
            run_tests "$dir" "$dir_name" "all"
        fi
    fi
done

# Print summary
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    TEST SUMMARY                        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ ${#FAILED_PROJECTS[@]} -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    echo -e "${GREEN}  Total: $PASSED_TESTS / $TOTAL_TESTS tests passed${NC}"
    exit 0
else
    echo -e "${RED}✗ Some tests failed:${NC}"
    for project in "${FAILED_PROJECTS[@]}"; do
        echo -e "${RED}  - $project${NC}"
    done
    echo ""
    echo -e "${YELLOW}  Total: $PASSED_TESTS / $TOTAL_TESTS tests passed${NC}"
    exit 1
fi
