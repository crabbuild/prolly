#!/usr/bin/env bash
set -Eeuo pipefail

endpoint="${PROLLY_RUSTFS_ENDPOINT:-http://127.0.0.1:9000}"
access_key="${PROLLY_RUSTFS_ACCESS_KEY:-prollyadmin}"
secret_key="${PROLLY_RUSTFS_SECRET_KEY:-prolly-local-secret-change-me}"
region="${PROLLY_RUSTFS_REGION:-us-east-1}"
bucket="${PROLLY_RUSTFS_BUCKET:-prolly-versioned-s3-demo}"

for command_name in aws curl cmp mktemp; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "required command is unavailable: $command_name" >&2
    exit 1
  fi
done

aws_s3api() {
  AWS_ACCESS_KEY_ID="$access_key" \
    AWS_SECRET_ACCESS_KEY="$secret_key" \
    AWS_DEFAULT_REGION="$region" \
    aws --no-cli-pager --region "$region" --endpoint-url "$endpoint" s3api "$@"
}

temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/prolly-rustfs-aws-cli.XXXXXX")"
payload_file="$temporary_directory/payload.txt"
download_file="$temporary_directory/download.txt"
probe_key="manual-verification/aws-cli-$(date -u +%Y%m%dT%H%M%SZ)-$$.txt"
probe_version_id=""
probe_written=0

delete_probe() {
  if [[ "$probe_written" -ne 1 ]]; then
    return
  fi

  if [[ -n "$probe_version_id" && "$probe_version_id" != "None" && "$probe_version_id" != "null" ]]; then
    aws_s3api delete-object \
      --bucket "$bucket" \
      --key "$probe_key" \
      --version-id "$probe_version_id" >/dev/null
  else
    aws_s3api delete-object --bucket "$bucket" --key "$probe_key" >/dev/null
  fi
  probe_written=0
}

cleanup() {
  set +e
  delete_probe
  rm -f -- "$payload_file" "$download_file"
  rmdir "$temporary_directory" 2>/dev/null
}
trap cleanup EXIT

echo "Checking RustFS health at ${endpoint%/}/health"
curl --fail --silent --show-error "${endpoint%/}/health"
echo

echo "Authenticating as $access_key and listing buckets"
aws_s3api list-buckets --query 'Buckets[].Name' --output table

echo "Checking bucket $bucket"
aws_s3api head-bucket --bucket "$bucket"

echo "Physical bucket versioning configuration"
if ! aws_s3api get-bucket-versioning --bucket "$bucket" --output json; then
  echo "The provider did not expose GetBucketVersioning for $bucket" >&2
fi

printf 'prolly RustFS AWS CLI verification\n' >"$payload_file"

echo "Writing probe s3://$bucket/$probe_key"
probe_version_id="$(
  aws_s3api put-object \
    --bucket "$bucket" \
    --key "$probe_key" \
    --body "$payload_file" \
    --content-type text/plain \
    --query VersionId \
    --output text
)"
probe_written=1

echo "Reading probe metadata"
aws_s3api head-object \
  --bucket "$bucket" \
  --key "$probe_key" \
  --query '{ContentLength:ContentLength,ContentType:ContentType,ETag:ETag,VersionId:VersionId}' \
  --output json

echo "Downloading and comparing probe bytes"
aws_s3api get-object --bucket "$bucket" --key "$probe_key" "$download_file" >/dev/null
if ! cmp -s "$payload_file" "$download_file"; then
  echo "downloaded probe does not match the uploaded bytes" >&2
  exit 1
fi

echo "Deleting the exact probe version when the provider returned one"
delete_probe

if aws_s3api head-object --bucket "$bucket" --key "$probe_key" >/dev/null 2>&1; then
  echo "probe is still visible after deletion" >&2
  exit 1
fi

echo "RUSTFS_AWS_CLI_VERIFICATION_COMPLETE bucket=$bucket endpoint=$endpoint"
