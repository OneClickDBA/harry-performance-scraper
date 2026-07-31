#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
readonly REPO_ROOT
readonly OUTPUT_FILE="${REPO_ROOT}/THIRD_PARTY_LICENSES.txt"
readonly HEADER_FILE="${REPO_ROOT}/thirdparty/NOTICE_HEADER.txt"
readonly TEMPLATE_FILE="${REPO_ROOT}/thirdparty/licenses.tpl"
readonly MODULE_PATH="github.com/OneClickDBA/harry-performance-scraper"
readonly GO_LICENSES_BIN="${GO_LICENSES_BIN:-go-licenses}"
readonly GO_LICENSES_VERSION="v2.0.1"

usage() {
  cat <<'EOF'
Usage: scripts/generate-third-party-licenses.sh [--check]

Without arguments, regenerate THIRD_PARTY_LICENSES.txt.
With --check, fail when the committed file is not current.
EOF
}

mode="write"
case "${1:-}" in
  "") ;;
  --check) mode="check" ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

if ! command -v "${GO_LICENSES_BIN}" >/dev/null 2>&1; then
  cat >&2 <<EOF
${GO_LICENSES_BIN} was not found in PATH.
Install the pinned generator with:
  go install github.com/google/go-licenses/v2@${GO_LICENSES_VERSION}
EOF
  exit 1
fi

for source_file in "${HEADER_FILE}" "${TEMPLATE_FILE}"; do
  if [[ ! -f "${source_file}" ]]; then
    echo "Required source file not found: ${source_file}" >&2
    exit 1
  fi
done

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
candidate="${tmp_dir}/THIRD_PARTY_LICENSES.txt"

generate_variant() {
  local output_file="$1"
  local tags="$2"
  local cgo_enabled="$3"
  local goos="$4"
  local goarch="$5"

  GOFLAGS="${tags:+-tags=${tags}}" \
    CGO_ENABLED="${cgo_enabled}" \
    GOOS="${goos}" \
    GOARCH="${goarch}" \
    "${GO_LICENSES_BIN}" report . \
      --ignore "${MODULE_PATH}" \
      --template "${TEMPLATE_FILE}" >"${output_file}"
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "Neither sha256sum nor shasum is available." >&2
    return 1
  fi
}

cd "${REPO_ROOT}"
cat "${HEADER_FILE}" >"${candidate}"
cat >>"${candidate}" <<EOF

Generator: github.com/google/go-licenses/v2@${GO_LICENSES_VERSION}
go.mod SHA-256: $(sha256_file go.mod)
go.sum SHA-256: $(sha256_file go.sum)
EOF

# Architectures have identical external dependency inventories for each of
# these driver/OS combinations, so one representative architecture is enough.
generate_variant "${tmp_dir}/godror-linux" "" 1 linux amd64
generate_variant "${tmp_dir}/goora-linux" goora 0 linux amd64
generate_variant "${tmp_dir}/goora-windows" goora 0 windows amd64
generate_variant "${tmp_dir}/goora-darwin" goora 0 darwin amd64

cat >>"${candidate}" <<'EOF'

===============================================================================
DEPENDENCY INVENTORY: ALL RELEASE VARIANTS
===============================================================================

Covered builds:
- godror: Linux amd64 and arm64
- go-ora: Linux amd64 and arm64
- go-ora: Windows amd64
- go-ora: macOS amd64 and arm64
EOF

# The template separates records with ASCII Record Separator. Keep the first
# occurrence of each complete dependency/license record across all variants.
awk '
  BEGIN { RS = sprintf("%c", 30); ORS = "" }
  NF {
    sub(/^[\r\n]+/, "")
    if (!seen[$0]++) {
      print "\n" $0
    }
  }
' "${tmp_dir}/godror-linux" \
  "${tmp_dir}/goora-linux" \
  "${tmp_dir}/goora-windows" \
  "${tmp_dir}/goora-darwin" >>"${candidate}"

# Preserve license text while normalizing platform line endings and EOF.
perl -pi -e 's/\r$//' "${candidate}"
perl -0pi -e 's/\n+\z/\n/' "${candidate}"
chmod 0644 "${candidate}"

if grep -Eq '^License: Unknown$|^Source: Unknown$' "${candidate}"; then
  echo "The generated inventory contains unresolved license metadata:" >&2
  grep -En '^License: Unknown$|^Source: Unknown$' "${candidate}" >&2
  exit 1
fi

if [[ "${mode}" == check ]]; then
  if ! cmp -s "${candidate}" "${OUTPUT_FILE}"; then
    echo "${OUTPUT_FILE} is out of date." >&2
    echo "Run 'make licenses' and commit the result." >&2
    diff -u "${OUTPUT_FILE}" "${candidate}" || true
    exit 1
  fi
  echo "THIRD_PARTY_LICENSES.txt is current."
  exit 0
fi

mv "${candidate}" "${OUTPUT_FILE}"
chmod 0644 "${OUTPUT_FILE}"
echo "Generated ${OUTPUT_FILE}."
