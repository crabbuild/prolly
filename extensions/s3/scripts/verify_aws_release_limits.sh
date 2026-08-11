#!/usr/bin/env bash
set -euo pipefail

qualification_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
limits="${PROLLY_S3_PRODUCTION_LIMITS:-$qualification_root/qualification/production-limits-v1.json}"

required_variables=(
  PROLLY_S3_AWS_COST_LOG
  PROLLY_S3_AWS_CONTENTION_LOG
  PROLLY_S3_AWS_LOAD_LOG
  PROLLY_S3_AWS_SCALE_LOG
  PROLLY_S3_AWS_REQUEST_PRICES
)

for variable in "${required_variables[@]}"; do
  value="${!variable:-}"
  if [[ -z "$value" || ! -f "$value" ]]; then
    echo "$variable must name a regular release-evidence file" >&2
    exit 2
  fi
done

python3 "$qualification_root/scripts/verify_production_limits.py" \
  --limits "$limits" \
  --profile aws-release \
  --cost-log "$PROLLY_S3_AWS_COST_LOG" \
  --contention-log "$PROLLY_S3_AWS_CONTENTION_LOG" \
  --expected-writers 1,8,32 \
  --load-log "$PROLLY_S3_AWS_LOAD_LOG" \
  --scale-log "$PROLLY_S3_AWS_SCALE_LOG" \
  --request-prices "$PROLLY_S3_AWS_REQUEST_PRICES"

echo "AWS_RELEASE_LIMITS_COMPLETE profile=aws-release limits=$limits"
