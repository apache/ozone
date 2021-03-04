#!/usr/bin/env bash

# Fail if required variables are not set.
set -u
: "${OZONE_VOLUME}"
: "${TEST_DIR}"
set +u

source "$TEST_DIR/testlib.sh"

export COMPOSE_FILE="$TEST_DIR/compose/ha/docker-compose.yaml"
create_data_dirs "${OZONE_VOLUME}"/{om1,om2,om3,dn1,dn2,dn3,recon,s3g,scm}
