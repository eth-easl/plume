#!/bin/bash
# Sets up a venv with the libraries needed for benchmarks/ubench/python's
# --plot chart (matplotlib). The trace summary itself is stdlib-only; this is
# only needed for rendering.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR"/setup_common.sh

REPO_ROOT="$(git -C "$DIR" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# configuration
venv_dir="build/venv-ubench" # relative to repository root; already under .gitignore's /build*/
while getopts 'd:' flag; do
  case "${flag}" in
    d) venv_dir="${OPTARG}" ;;
    *) printf "Usage: %s [-d venv_dir]\n" "$0"
       exit 1 ;;
  esac
done

if ! has_command python3; then
  msg "python3 not found on PATH." "$color_red"
  exit 1
fi

if [ ! -d "${venv_dir}" ]; then
  # `python3 -m venv` needs ensurepip, which Debian/Ubuntu splits into a
  # separate python3.X-venv package; without it venv creation fails deep
  # inside CPython with a much less obvious error. Check up front.
  if ! python3 -c "import ensurepip" &>/dev/null; then
    py_ver="$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
    msg "python3's venv support (ensurepip) is missing. Install it first:" "$color_red"
    echo "  sudo apt install python${py_ver}-venv"
    exit 1
  fi
  msg "Creating venv in '${venv_dir}'..."
  python3 -m venv "${venv_dir}"
else
  msg "Reusing existing venv in '${venv_dir}'."
fi

msg "Installing plotting dependencies..."
"${venv_dir}/bin/pip" install --quiet --upgrade pip
"${venv_dir}/bin/pip" install --quiet matplotlib

msg "Done. Use it with:"
echo "  ${venv_dir}/bin/python3 benchmarks/ubench/python/summarize_trace.py TRACE.trace --plot out.png"
echo "or activate it first:"
echo "  source ${venv_dir}/bin/activate"
