cmake_minimum_required(VERSION 3.16)
project(SibenchDuckDbJdbc LANGUAGES C CXX)
find_package(JNI REQUIRED)
set(CMAKE_CXX_STANDARD 17)
file(GLOB JNI_SOURCES CONFIGURE_DEPENDS "${JDBC_SOURCE}/src/jni/*.cpp")
add_library(duckdb_java SHARED ${JNI_SOURCES}
  "${CORE_BUILD}/src/duckdb_static_extension_loader.c"
  "${CORE_BUILD}/source/extension/loader/static_extension_autoregister.cpp")
target_include_directories(duckdb_java PRIVATE ${JNI_INCLUDE_DIRS}
  "${CORE_BUILD}/source/src/include" "${CORE_BUILD}/source/third_party/fmt/include"
  "${CORE_BUILD}/source/third_party/utf8proc/include"
  "${CMAKE_CURRENT_SOURCE_DIR}/headers")
target_compile_definitions(duckdb_java PRIVATE DUCKDB_STATIC_BUILD)
target_compile_options(duckdb_java PRIVATE -fvisibility=hidden
  $<$<COMPILE_LANGUAGE:CXX>:-fvisibility-inlines-hidden>)
target_link_options(duckdb_java PRIVATE -Wl,--no-undefined -Wl,-Bsymbolic
  "-Wl,--version-script=${JDBC_SOURCE}/duckdb_java.map")
target_link_libraries(duckdb_java PRIVATE -Wl,--start-group
  "${CORE_BUILD}/src/libduckdb_static.a"
  "${CORE_BUILD}/src/main/http/libduckdb_httplib.a"
  "${CORE_BUILD}/src/main/extension/libduckdb_loadable_extensions.a"
  "${CORE_BUILD}/src/main/extension/local_extension_repository/libduckdb_local_extension_repository.a"
  "${CORE_BUILD}/extension/core_functions/libcore_functions_extension.a"
  "${CORE_BUILD}/third_party/utf8proc/libduckdb_utf8proc.a"
  -Wl,--end-group dl pthread)
set_target_properties(duckdb_java PROPERTIES SUFFIX ".so_linux_amd64")
