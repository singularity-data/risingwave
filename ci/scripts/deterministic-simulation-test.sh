#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "+++ Running deterministic simulation test"
echo "$(tput setaf 3)Some tests may not pass. Tracking issue: https://github.com/singularity-data/risingwave/issues/3467$(tput sgr0)"

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Run unit tests in deterministic simulation mode"
timeout 7m cargo make stest --no-fail-fast
