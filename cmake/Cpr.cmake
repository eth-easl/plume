# ---------------------------------------------------------------------------
#  libcpr (C++ Requests) — pinned, fetched via FetchContent
#   A thin libcurl wrapper exposed as the imported target `cpr::cpr`. Used by the
#   `plume` runner to POST query invocations to a dandelion instance, and by the
#   client's RemoteFetcher to range-fetch a remote table's header/footer (schema +
#   statistics) at compile time. cpr builds its own libcurl.
# ---------------------------------------------------------------------------
message(STATUS "Fetching libcpr...")

# The runner POSTs to the dandelion instance over plain HTTP, but the client also
# fetches the `https://` *data* URLs of remote tables itself (to resolve their
# schema/stats), so HTTPS support is needed for remote sources to bind.
#
# cpr/curl need a TLS backend at build time. We use OpenSSL when its development
# files are present and build without SSL otherwise (https remote tables then fail
# to resolve, but plain-HTTP dandelion calls and local/http sources still work).
# NOTE: a system may ship the OpenSSL runtime (libssl.so) without the headers —
# install the dev package (e.g. `libssl-dev` on Debian/Ubuntu) and re-run cmake to
# turn HTTPS on. To use a non-system OpenSSL, pass -DOPENSSL_ROOT_DIR=/path.
#
# Prefer OpenSSL's static archives (libssl.a/libcrypto.a, shipped alongside the
# .so by libssl-dev) over the shared runtime, so binaries that bundle plume_client
# (e.g. plume_bench, distributed as a standalone downloadable asset) don't pick up
# a dynamic dependency on the build machine's libssl/libcrypto version. If only
# the shared libs are found, OpenSSL_FOUND stays false and we fall through to the
# no-SSL branch below rather than silently linking a .so.
set(OPENSSL_USE_STATIC_LIBS ON)
find_package(OpenSSL QUIET)
if(OpenSSL_FOUND)
  message(STATUS "OpenSSL ${OPENSSL_VERSION} found; building libcpr with HTTPS support.")
  set(CPR_ENABLE_SSL ON CACHE BOOL "" FORCE)
  set(CPR_FORCE_OPENSSL_BACKEND ON CACHE BOOL "" FORCE)
else()
  message(WARNING
    "OpenSSL development files not found; building libcpr WITHOUT SSL. "
    "https:// remote-table URLs will NOT resolve at compile time. Install the "
    "OpenSSL dev package (e.g. libssl-dev) and re-run cmake to enable HTTPS.")
  set(CPR_ENABLE_SSL OFF CACHE BOOL "" FORCE)
endif()

# cpr and the curl it fetches in turn both build shared libraries by default;
# curl's own CMakeLists is what actually defines the BUILD_SHARED_LIBS cache entry
# (with a default of ON) the first time it runs, so pin it to OFF ourselves before
# that happens. This makes cpr and curl both build as static archives, so binaries
# that bundle plume_client (e.g. plume_bench, distributed as a standalone
# downloadable asset) don't carry a runtime dependency on libcpr.so/libcurl.so —
# libcurl is instead baked into the plume_client/plume_bench binary itself.
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)

# curl also optionally links zlib (for compressed transfer encoding) via a plain
# find_package(ZLIB); prefer its static archive for the same reason as OpenSSL
# above. Ubuntu's zlib1g-dev ships libz.a alongside libz.so. If no static archive
# is found, ZLIB support is left off rather than falling back to libz.so.
set(ZLIB_USE_STATIC_LIBS ON)

include(FetchContent)
FetchContent_Declare(
  cpr
  GIT_REPOSITORY https://github.com/libcpr/cpr.git
  GIT_TAG 1.11.2
)

FetchContent_MakeAvailable(cpr)
