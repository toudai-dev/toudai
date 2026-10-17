#!/usr/bin/env bash
# Object-store helpers shared by the cloud benchmark scripts.
#
# The store is chosen from the URI scheme so the same script can drive either
# cloud: `gs://` uses the Google Cloud CLI and `s3://` uses the AWS CLI. Callers
# pass whole URIs and never a bare path, so no caller has to know which cloud it
# is talking to, and a run is repointed by changing one bucket variable.
#
# Source this file; it is not executable on its own.
#
# S3 credentials and region come from the normal AWS resolution chain, which on
# a benchmark node is the instance profile plus instance metadata. Set
# TOUDAI_AWS_PROFILE and TOUDAI_AWS_REGION to override them.

_object_store_aws() {
  local args=(aws)
  [[ -n "${TOUDAI_AWS_PROFILE:-}" ]] && args+=(--profile "${TOUDAI_AWS_PROFILE}")
  [[ -n "${TOUDAI_AWS_REGION:-}" ]] && args+=(--region "${TOUDAI_AWS_REGION}")
  "${args[@]}" "${@}"
}

# Print the bucket and key of an s3:// URI, tab-separated, on one line.
_object_store_s3_parts() {
  local uri="${1#s3://}"
  local bucket="${uri%%/*}"
  local key=""
  [[ "${uri}" == */* ]] && key="${uri#*/}"
  printf '%s\t%s' "${bucket}" "${key}"
}

# Total bytes for a bucket and optional key prefix. JMESPath `sum()` rejects a
# null projection, so the sizes are summed locally instead.
#
# `printf "%.0f"` rather than `print`: Debian ships mawk, whose `print` renders a
# large integer in scientific notation, and a byte total of 1.13284e+11 silently
# fails every comparison against the exact inventory.
_object_store_s3_bytes() {
  local bucket="$1" key="$2"
  _object_store_aws s3api list-objects-v2 --bucket "${bucket}" --prefix "${key}" \
    --query 'Contents[].Size' --output text |
    tr '\t' '\n' |
    awk '{ total += ($1 == "None" ? 0 : $1) } END { printf "%.0f\n", total }'
}

# `gcloud storage du --summarize` prints the byte count immediately before the
# URL for a whole bucket and with a space for a prefix, so take the leading
# integer rather than splitting on whitespace.
_object_store_leading_int() {
  awk 'match($0, /^[[:space:]]*[0-9]+/) { printf "%.0f\n", substr($0, RSTART, RLENGTH) }'
}

# Fail early, and with the right CLI named, when the store cannot be reached.
object_store_check() {
  case "$1" in
    gs://*)
      command -v gcloud >/dev/null 2>&1 || { echo "gcloud is required for ${1}" >&2; return 1; }
      ;;
    s3://*)
      command -v aws >/dev/null 2>&1 || { echo "aws is required for ${1}" >&2; return 1; }
      ;;
    *)
      echo "unsupported object store URI: ${1}" >&2
      return 1
      ;;
  esac
}

# Object URIs under a prefix, one per line and sorted.
object_store_list() {
  local prefix="${1%/}"
  case "${prefix}" in
    gs://*)
      gcloud storage ls "${prefix}/**"
      ;;
    s3://*)
      local bucket key
      IFS=$'\t' read -r bucket key <<< "$(_object_store_s3_parts "${prefix}/")"
      # The CLI emits list elements tab-separated, so converting tabs to
      # newlines keeps a key that contains a space intact.
      _object_store_aws s3api list-objects-v2 --bucket "${bucket}" --prefix "${key}" \
        --query 'Contents[].Key' --output text |
        tr '\t' '\n' |
        awk -v base="s3://${bucket}/" 'NF && $0 != "None" { print base $0 }'
      ;;
    *)
      return 1
      ;;
  esac | LC_ALL=C sort
}

# Total bytes stored under a prefix.
object_store_bytes() {
  local prefix="${1%/}"
  case "${prefix}" in
    gs://*)
      gcloud storage du --summarize "${prefix}" | _object_store_leading_int
      ;;
    s3://*)
      local bucket key
      IFS=$'\t' read -r bucket key <<< "$(_object_store_s3_parts "${prefix}/")"
      _object_store_s3_bytes "${bucket}" "${key}"
      ;;
    *)
      return 1
      ;;
  esac
}

# Total bytes in a whole bucket.
object_store_bucket_bytes() {
  local bucket="${1%/}"
  case "${bucket}" in
    gs://*)
      gcloud storage du --summarize "${bucket}" | _object_store_leading_int
      ;;
    s3://*)
      local name key
      IFS=$'\t' read -r name key <<< "$(_object_store_s3_parts "${bucket}")"
      _object_store_s3_bytes "${name}" "${key}"
      ;;
    *)
      return 1
      ;;
  esac
}

# Download one object to a local path.
object_store_get() {
  local object="$1" destination="$2"
  case "${object}" in
    gs://*)
      gcloud storage cp "${object}" "${destination}"
      ;;
    s3://*)
      _object_store_aws s3 cp "${object}" "${destination}" --only-show-errors --no-progress
      ;;
    *)
      return 1
      ;;
  esac
}

# Upload one local path to an object.
object_store_put() {
  local source="$1" object="$2"
  case "${object}" in
    gs://*)
      gcloud storage cp "${source}" "${object}" --cache-control=no-store
      ;;
    s3://*)
      _object_store_aws s3 cp "${source}" "${object}" --cache-control=no-store \
        --only-show-errors --no-progress
      ;;
    *)
      return 1
      ;;
  esac
}

# Succeed only when the object already exists.
object_store_exists() {
  local object="$1"
  case "${object}" in
    gs://*)
      gcloud storage ls "${object}" >/dev/null 2>&1
      ;;
    s3://*)
      # `aws s3 ls` succeeds on a missing key, so this needs HeadObject.
      local bucket key
      IFS=$'\t' read -r bucket key <<< "$(_object_store_s3_parts "${object}")"
      _object_store_aws s3api head-object --bucket "${bucket}" --key "${key}" >/dev/null 2>&1
      ;;
    *)
      return 1
      ;;
  esac
}
