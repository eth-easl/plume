# ---------------------------------------------------------------------------
#  nlohmann/json v3.12.0 (pinned, header-only)
#   Fetched via FetchContent and exposed as the imported target
#   `nlohmann_json::nlohmann_json`.  Used by the client (dandelion API).
# ---------------------------------------------------------------------------
message(STATUS "Fetching nlohmann/json...")

include(FetchContent)
FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.12.0
)

FetchContent_MakeAvailable(nlohmann_json)
