#!/bin/bash

set -euo pipefail
docker-compose exec postgres psql  -h localhost -p 5432 -U spoke spokedev "$@"

