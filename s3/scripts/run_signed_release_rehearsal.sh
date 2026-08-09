#!/usr/bin/env bash
set -euo pipefail

release_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repository_root="$(cd "$release_root/.." && pwd)"
container="${PROLLY_RUSTFS_CONTAINER:-prolly-rustfs}"
data_dir="${PROLLY_RUSTFS_DATA_DIR:-/Volumes/Workspace/prolly-data}"
cargo_toolchain="${PROLLY_S3_CARGO_TOOLCHAIN:-+1.94.1}"
package_target="${PROLLY_S3_RELEASE_PACKAGE_TARGET:-/Volumes/Workspace/prolly-build/versioned-s3}"
release_id="${PROLLY_S3_RELEASE_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
evidence_root="${PROLLY_S3_RELEASE_EVIDENCE_DIR:-$package_target/release-evidence/$release_id}"
rolling_build_root="${PROLLY_S3_RELEASE_ROLLING_BUILD_ROOT:-$package_target/release-rolling-build/$release_id}"
rolling_target_root="${PROLLY_S3_RELEASE_ROLLING_TARGET_ROOT:-$package_target/release-rolling-target}"
signing_scratch="$(mktemp -d)"

cleanup() {
  rm -f -- "$signing_scratch/ephemeral-private.pem"
  rmdir "$signing_scratch" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ "${PROLLY_S3_RUSTFS:-}" != "1" ]]; then
  echo "set PROLLY_S3_RUSTFS=1 and the RustFS endpoint credentials" >&2
  exit 2
fi
if [[ ! -d "$data_dir" ]]; then
  echo "RustFS data directory does not exist: $data_dir" >&2
  exit 2
fi
if ! docker inspect "$container" >/dev/null 2>&1; then
  echo "RustFS container $container does not exist" >&2
  exit 2
fi
rustfs_health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container")"
if [[ "$rustfs_health" != "healthy" ]]; then
  echo "RustFS is not healthy: $rustfs_health" >&2
  exit 2
fi
rustfs_mount="$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}:{{.Destination}}{{end}}{{end}}' "$container")"
if [[ "$rustfs_mount" != "$data_dir:/data" ]]; then
  echo "RustFS /data mount mismatch: expected=$data_dir:/data actual=$rustfs_mount" >&2
  exit 2
fi
if [[ -e "$evidence_root" ]]; then
  echo "release evidence directory already exists: $evidence_root" >&2
  exit 2
fi

source_revision="$(git -C "$repository_root" rev-parse HEAD 2>/dev/null || true)"
if [[ -z "$source_revision" ]]; then
  source_revision="unversioned"
fi
if [[ -n "$(git -C "$repository_root" status --porcelain --untracked-files=normal)" ]]; then
  source_state="dirty"
  if [[ "${PROLLY_S3_RELEASE_ALLOW_DIRTY:-}" != "1" ]]; then
    echo "release rehearsal requires a clean source tree; set PROLLY_S3_RELEASE_ALLOW_DIRTY=1 only for local rehearsal" >&2
    exit 2
  fi
  package_dirty=(--allow-dirty)
else
  source_state="clean"
  package_dirty=()
fi

mkdir -p "$evidence_root/artifacts" "$package_target"
export RUSTC_WRAPPER=""
dependency_security_log="$evidence_root/dependency-security.log"
set +e
"$release_root/scripts/check_dependency_security.sh" \
  2>&1 | tee "$dependency_security_log"
dependency_security_status="${PIPESTATUS[0]}"
set -e
if [[ "$dependency_security_status" != "0" ]]; then
  exit "$dependency_security_status"
fi

package_verification_log="$evidence_root/package-verification.log"
# The client depends on the core archive from the same release. Before the core
# is published, ordinary Cargo verification would silently select an older
# registry release with the same version or fail to resolve a new version.
# Build both archives as one workspace release without registry-based
# verification, then verify the two exact extracted archives together below.
set +e
CARGO_TARGET_DIR="$package_target" cargo "$cargo_toolchain" package \
  --manifest-path "$release_root/Cargo.toml" \
  --workspace --locked --no-verify "${package_dirty[@]}" \
  2>&1 | tee "$package_verification_log"
workspace_package_status="${PIPESTATUS[0]}"
set -e
if [[ "$workspace_package_status" != "0" ]]; then
  exit "$workspace_package_status"
fi

core_archive="$package_target/package/prolly-s3-core-0.1.0.crate"
client_archive="$package_target/package/prolly-s3-client-0.1.0.crate"
if [[ ! -f "$core_archive" || ! -f "$client_archive" ]]; then
  echo "Cargo did not produce both expected package archives" >&2
  exit 1
fi

package_verify_source="$rolling_build_root/package-verification-source"
package_verify_target="$rolling_target_root/package-verification"
if [[ -e "$package_verify_source" ]]; then
  echo "package verification source already exists: $package_verify_source" >&2
  exit 2
fi
mkdir -p "$package_verify_source/core" "$package_verify_source/client"
tar -xzf "$core_archive" -C "$package_verify_source/core" --strip-components=1
tar -xzf "$client_archive" -C "$package_verify_source/client" --strip-components=1
set +e
CARGO_TARGET_DIR="$package_verify_target" cargo "$cargo_toolchain" check \
  --manifest-path "$package_verify_source/client/Cargo.toml" --offline \
  --config "patch.crates-io.prolly-s3-core.path='$package_verify_source/core'" \
  2>&1 | tee -a "$package_verification_log"
archive_check_status="${PIPESTATUS[0]}"
set -e
if [[ "$archive_check_status" != "0" ]]; then
  exit "$archive_check_status"
fi
echo "EXACT_PACKAGE_PAIR_VERIFIED core=$core_archive client=$client_archive" \
  | tee -a "$package_verification_log"

cp "$core_archive" "$client_archive" "$evidence_root/artifacts/"
cp "$release_root/Cargo.lock" "$evidence_root/Cargo.lock"
cp "$release_root/deny.toml" "$evidence_root/deny.toml"
cp "$release_root/compatibility-v1.json" "$evidence_root/compatibility-v1.json"
cp "$release_root/fixtures/canonical-v1.json" "$evidence_root/canonical-v1.json"
tar -tzf "$evidence_root/artifacts/prolly-s3-core-0.1.0.crate" \
  | LC_ALL=C sort >"$evidence_root/prolly-s3-core-0.1.0.contents.txt"
tar -tzf "$evidence_root/artifacts/prolly-s3-client-0.1.0.crate" \
  | LC_ALL=C sort >"$evidence_root/prolly-s3-client-0.1.0.contents.txt"

rolling_bucket="${PROLLY_S3_ROLLING_BUCKET:-prolly-versioned-s3-release-rehearsal}"
rolling_prefix="${PROLLY_S3_ROLLING_PREFIX:-integration/signed-release/$release_id}"
set +e
PROLLY_S3_ROLLING_BUCKET="$rolling_bucket" \
PROLLY_S3_ROLLING_PREFIX="$rolling_prefix" \
PROLLY_S3_ROLLING_BUILD_ROOT="$rolling_build_root" \
PROLLY_S3_ROLLING_TARGET_ROOT="$rolling_target_root" \
PROLLY_S3_ROLLING_CORE_PACKAGE="$evidence_root/artifacts/prolly-s3-core-0.1.0.crate" \
PROLLY_S3_ROLLING_CLIENT_PACKAGE="$evidence_root/artifacts/prolly-s3-client-0.1.0.crate" \
CARGO_TARGET_DIR="$package_target" \
  "$release_root/scripts/run_rustfs_rolling_upgrade.sh" \
  2>&1 | tee "$evidence_root/rolling-upgrade.log"
rolling_status="${PIPESTATUS[0]}"
set -e
if [[ "$rolling_status" != "0" ]]; then
  exit "$rolling_status"
fi

if [[ -n "${PROLLY_S3_RELEASE_SIGNING_KEY:-}" ]]; then
  signing_key="$PROLLY_S3_RELEASE_SIGNING_KEY"
  if [[ ! -f "$signing_key" ]]; then
    echo "release signing key does not exist: $signing_key" >&2
    exit 2
  fi
  signer_mode="operator-supplied"
else
  if [[ "${PROLLY_S3_ALLOW_EPHEMERAL_SIGNING:-}" != "1" ]]; then
    echo "set PROLLY_S3_RELEASE_SIGNING_KEY; ephemeral signing requires explicit PROLLY_S3_ALLOW_EPHEMERAL_SIGNING=1" >&2
    exit 2
  fi
  signing_key="$signing_scratch/ephemeral-private.pem"
  openssl genpkey -algorithm ED25519 -out "$signing_key"
  chmod 600 "$signing_key"
  signer_mode="ephemeral-local-rehearsal"
fi
openssl pkey -in "$signing_key" -pubout -out "$evidence_root/release-public-key.pem"
signer_fingerprint="$(openssl pkey -pubin -in "$evidence_root/release-public-key.pem" -outform DER | shasum -a 256 | awk '{print $1}')"
created_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cargo_version="$(cargo "$cargo_toolchain" --version)"
rustc_version="$(rustc "$cargo_toolchain" --version)"
rustfs_image="$(docker inspect --format '{{.Config.Image}}@{{.Image}}' "$container")"

python3 "$release_root/scripts/release_evidence.py" create \
  --root "$evidence_root" \
  --artifact artifacts/prolly-s3-core-0.1.0.crate \
  --artifact artifacts/prolly-s3-client-0.1.0.crate \
  --artifact Cargo.lock \
  --artifact deny.toml \
  --artifact compatibility-v1.json \
  --artifact canonical-v1.json \
  --artifact prolly-s3-core-0.1.0.contents.txt \
  --artifact prolly-s3-client-0.1.0.contents.txt \
  --artifact package-verification.log \
  --artifact dependency-security.log \
  --artifact rolling-upgrade.log \
  --artifact release-public-key.pem \
  --metadata "created_at=$created_at" \
  --metadata "source_revision=$source_revision" \
  --metadata "source_state=$source_state" \
  --metadata "cargo_version=$cargo_version" \
  --metadata "rustc_version=$rustc_version" \
  --metadata "rustfs_container=$container" \
  --metadata "rustfs_health=$rustfs_health" \
  --metadata "rustfs_image=$rustfs_image" \
  --metadata "rustfs_mount=$rustfs_mount" \
  --metadata "rolling_bucket=$rolling_bucket" \
  --metadata "rolling_prefix=$rolling_prefix" \
  --metadata "signer_mode=$signer_mode" \
  --metadata "signer_fingerprint_sha256=$signer_fingerprint"

openssl pkeyutl -sign -rawin -inkey "$signing_key" \
  -in "$evidence_root/release-evidence.json" \
  -out "$evidence_root/release-evidence.sig"
openssl pkeyutl -verify -rawin -pubin \
  -inkey "$evidence_root/release-public-key.pem" \
  -in "$evidence_root/release-evidence.json" \
  -sigfile "$evidence_root/release-evidence.sig"
python3 "$release_root/scripts/release_evidence.py" verify \
  --root "$evidence_root"

manifest_sha256="$(shasum -a 256 "$evidence_root/release-evidence.json" | awk '{print $1}')"
signature_sha256="$(shasum -a 256 "$evidence_root/release-evidence.sig" | awk '{print $1}')"
echo "SIGNED_RELEASE_REHEARSAL_COMPLETE evidence=$evidence_root manifest_sha256=$manifest_sha256 signature_sha256=$signature_sha256 signer_fingerprint_sha256=$signer_fingerprint signer_mode=$signer_mode source_state=$source_state"
