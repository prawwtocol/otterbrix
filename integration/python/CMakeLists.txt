if(POLICY CMP0148) # Disallow FindPythonLibs
    cmake_policy(SET CMP0148 NEW)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# ── Libraries to link against ────────────────────────────────────────────────
# Local targets from otterbrix (NOT the external Conan package otterbrix::otterbrix)
set(external_LIBS
    cpp_otterbrix
    otterbrix_string_util
    otterbrix_vector
    otterbrix_table
    magic_enum::magic_enum
    utf8proc::utf8proc
)

# ── Collect object files from subdirectories ─────────────────────────────────
set(ALL_OBJECT_FILES)

add_subdirectory(components)
add_subdirectory(pybind11)
add_subdirectory(common)
add_subdirectory(connection_environment)
add_subdirectory(native)
add_subdirectory(numpy)
add_subdirectory(pandas)
add_subdirectory(scan)
# add_subdirectory(arrow)
add_subdirectory(otterbrix_wrapper)
add_subdirectory(pyconnection)
add_subdirectory(util)

# ── Old SQL-based bindings (kept for backwards compatibility) ────────────────
set(otterbrix_sql_sources
    sql/convert.cpp
    sql/spaces.cpp
    sql/wrapper_client.cpp
    sql/wrapper_connection.cpp
    sql/wrapper_cursor.cpp
)

# ── Build the Python module ──────────────────────────────────────────────────
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} main.cpp ${otterbrix_sql_sources})
pybind11_add_module(otterbrix_python ${ALL_OBJECT_FILES})

target_link_libraries(otterbrix_python PUBLIC
    ${external_LIBS}
    pybind11::pybind11
    spdlog::spdlog
    msgpackc-cxx
    absl::crc32c
    absl::int128
    actor-zeta::actor-zeta
    Boost::boost
    fmt::fmt
    ZLIB::ZLIB
    BZip2::BZip2
)

set_target_properties(otterbrix_python PROPERTIES
    OUTPUT_NAME "otterbrix"
    CXX_VISIBILITY_PRESET "hidden"
)

pybind11_extension(otterbrix_python)

if(NOT MSVC AND NOT ${CMAKE_BUILD_TYPE} MATCHES Debug|RelWithDebInfo)
    pybind11_strip(otterbrix_python)
endif()

# ── Install rules ────────────────────────────────────────────────────────────
if(SKBUILD)
    install(DIRECTORY otterbrix DESTINATION .)
    install(TARGETS otterbrix_python DESTINATION ./otterbrix)
endif()

# ── Copy test scripts in dev mode ────────────────────────────────────────────
if(DEV_MODE)
    file(GLOB PY_TESTS "tests/*.py")
    file(COPY ${PY_TESTS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif()
