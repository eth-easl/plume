# ---------------------------------------------------------------------------
#  Protocol Buffers (statically linked)
#   Used only by the client-side compile layer (core/src/dandelion/api.cpp) to
#   encode/decode the protobuf metadata header of dandelion requests/responses.
#   Exposes PLUME_PROTOBUF_TARGET (the libprotobuf-lite target to link) and the
#   protobuf_generate() function. dandelion.proto uses `optimize_for = LITE_RUNTIME`,
#   which only needs the much smaller lite runtime (~1MB vs ~2.5MB per binary).
#
#   Linked statically so binaries that bundle the client (e.g. plume_bench,
#   distributed as a standalone downloadable asset) don't pick up a runtime
#   dependency on the build machine's libprotobuf version.
#
#   Resolution order:
#    1. An upstream CMake package (protobuf >= 22 installs one together with its
#       abseil dependency), e.g. a source install under CMAKE_PREFIX_PATH.
#    2. CMake's FindProtobuf module (e.g. distro `libprotobuf-dev` +
#       `protobuf-compiler`), preferring the static libprotobuf.a.
#   The protoc used for code generation must match the linked library version;
#   both modes pick the protoc that belongs to the found package.
# ---------------------------------------------------------------------------

find_package(Protobuf CONFIG QUIET)
if(Protobuf_FOUND)
  message(STATUS "Found protobuf ${Protobuf_VERSION} (CMake package: ${Protobuf_DIR})")
else()
  set(Protobuf_USE_STATIC_LIBS ON)
  find_package(Protobuf REQUIRED MODULE)
  message(STATUS "Found protobuf ${Protobuf_VERSION} (FindProtobuf: ${Protobuf_LITE_LIBRARY})")
  # FindProtobuf only defines protobuf_generate() since CMake 3.13; older distro
  # protoc (< 3.15) additionally needs a flag for proto3 `optional` fields.
  if(Protobuf_VERSION VERSION_LESS 3.15)
    set(PLUME_PROTOC_EXTRA_ARGS --experimental_allow_proto3_optional)
  endif()
endif()
set(PLUME_PROTOBUF_TARGET protobuf::libprotobuf-lite)

if(NOT COMMAND protobuf_generate)
  message(FATAL_ERROR "protobuf_generate() is not available; use a newer CMake or protobuf install.")
endif()
