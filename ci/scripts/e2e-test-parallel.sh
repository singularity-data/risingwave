#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an arguemnt" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

echo "--- Download artifacts"
mkdir -p target/debug
buildkite-agent artifact download risingwave-"$profile" target/debug/
buildkite-agent artifact download risedev-playground-"$profile" target/debug/
mv target/debug/risingwave-"$profile" target/debug/risingwave
mv target/debug/risedev-playground-"$profile" target/debug/risedev-playground

echo "--- Adjust permission"
chmod +x ./target/debug/risingwave
chmod +x ./target/debug/risedev-playground

echo "--- Generate RiseDev CI config"
cp ci/risedev-components.ci.env risedev-components.user.env

echo "--- Prepare RiseDev playground"
cargo make pre-start-playground
cargo make link-all-in-one-binaries

echo "--- Start cluster"
cargo make ci-start ci-3cn-1fe

echo "--- e2e, ci-3cn-1fe, streaming"
timeout 5m sqllogictest -p 4566 -d dev  './e2e_test/streaming/**/*.slt' -j 16 --junit "parallel-streaming-${profile}"

echo "--- e2e, ci-3cn-1fe, delta join"
timeout 3m sqllogictest -p 4566 -d dev  './e2e_test/streaming_delta_join/**/*.slt' --junit "parallel-streaming-delta-join-${profile}"

echo "--- e2e, ci-3cn-1fe, batch distributed"
timeout 2m sqllogictest -p 4566 -d dev  './e2e_test/batch/**/*.slt' -j 16 --junit "parallel-batch-${profile}"

echo "--- Kill cluster"
cargo make ci-kill
