#
# Copyright(c) 2020 ADLINK Technology Limited and others
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License v. 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0, or the Eclipse Distribution License
# v. 1.0 which is available at
# http://www.eclipse.org/org/documents/edl-v10.php.
#
# SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
#
project(helloworld LANGUAGES C CXX)
cmake_minimum_required(VERSION 3.10)

if(NOT TARGET CycloneDDS-CXX::ddscxx)
  find_package(CycloneDDS-CXX REQUIRED)
endif()

idlcxx_generate(TARGET thousandtopics_types FILES types.idl)

add_executable(thousandtopics thousandtopics.cpp thousandtopics.h)
target_include_directories(thousandtopics PRIVATE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(thousandtopics CycloneDDS-CXX::ddscxx thousandtopics_types)

# Disable the static analyzer in GCC to avoid crashing the GNU C++ compiler
# on Azure Pipelines
if(DEFINED ENV{SYSTEM_TEAMFOUNDATIONSERVERURI})
  if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND ANALYZER STREQUAL "on")
    target_compile_options(thousandtopics PRIVATE -fno-analyzer)
  endif()
endif()

set_property(TARGET thousandtopics PROPERTY CXX_STANDARD 17)
