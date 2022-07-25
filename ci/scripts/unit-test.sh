#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "+++ Run unit tests with coverage"
# disable fuzzing tests (risingwave_sqlsmith)
# use tee to disable progress bar
NEXTEST_PROFILE=ci cargo llvm-cov nextest \
    --lcov --output-path lcov.info \
    --features failpoints \
    --workspace --exclude risingwave_sqlsmith \
    2> >(tee)

echo "--- Codecov upload coverage reports"
curl -Os https://uploader.codecov.io/latest/linux/codecov && chmod +x codecov
./codecov -t "$CODECOV_TOKEN" -s . -F rust
