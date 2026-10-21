# Injected via CMAKE_PROJECT_INCLUDE when configuring Axiom: the installed folly
# CMake config references ZLIB::ZLIB without finding ZLIB itself.
find_package(ZLIB REQUIRED)
