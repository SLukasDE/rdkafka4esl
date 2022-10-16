cmake_minimum_required(VERSION 3.23)

project(rdkafka4esl VERSION 1.5.0
            LANGUAGES CXX)
            
#include(FetchContent)
#FetchContent_Declare(
#  catch
#  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
#  GIT_TAG        v2.13.6
#)
#FetchContent_MakeAvailable(catch)


# The compiled library code is here
#add_subdirectory(src/main)
# STATIC, SHARED, MODULE, INTERFACE
add_library(rdkafka4esl STATIC
      ./src/main/rdkafka4esl/Plugin.cpp
      ./src/main/rdkafka4esl/Plugin.h
      ./src/main/rdkafka4esl/Logger.h
      ./src/main/rdkafka4esl/com/basic/server/RequestContext.cpp
      ./src/main/rdkafka4esl/com/basic/server/Socket.cpp
      ./src/main/rdkafka4esl/com/basic/server/ObjectContext.h
      ./src/main/rdkafka4esl/com/basic/server/ObjectContext.cpp
      ./src/main/rdkafka4esl/com/basic/server/Request.cpp
      ./src/main/rdkafka4esl/com/basic/server/Connection.h
      ./src/main/rdkafka4esl/com/basic/server/Connection.cpp
      ./src/main/rdkafka4esl/com/basic/server/RequestContext.h
      ./src/main/rdkafka4esl/com/basic/server/Request.h
      ./src/main/rdkafka4esl/com/basic/server/Socket.h
      ./src/main/rdkafka4esl/com/basic/broker/Client.h
      ./src/main/rdkafka4esl/com/basic/broker/Client.cpp
      ./src/main/rdkafka4esl/com/basic/client/SharedConnectionFactory.h
      ./src/main/rdkafka4esl/com/basic/client/SharedConnectionFactory.cpp
      ./src/main/rdkafka4esl/com/basic/client/ConnectionFactory.h
      ./src/main/rdkafka4esl/com/basic/client/SharedConnection.cpp
      ./src/main/rdkafka4esl/com/basic/client/Connection.h
      ./src/main/rdkafka4esl/com/basic/client/Connection.cpp
      ./src/main/rdkafka4esl/com/basic/client/ConnectionFactory.cpp
      ./src/main/rdkafka4esl/com/basic/client/SharedConnection.h
)

target_include_directories(rdkafka4esl PUBLIC ./src/main)
target_compile_features(rdkafka4esl PUBLIC cxx_std_11)
set_property(TARGET rdkafka4esl PROPERTY POSITION_INDEPENDENT_CODE ON)

find_package(esl REQUIRED)
target_link_libraries(rdkafka4esl PRIVATE esl::esl)

target_link_libraries(rdkafka4esl PRIVATE rdkafka)

install(TARGETS rdkafka4esl
        EXPORT rdkafka4esl-export
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

install(EXPORT      rdkafka4esl-export
        FILE        rdkafka4eslTargets.cmake
        NAMESPACE   rdkafka4esl::
        DESTINATION lib/cmake
)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/rdkafka4eslConfig.cmake
        DESTINATION "lib/cmake"
)




# The compiled test code is here
#add_subdirectory(src/test)
