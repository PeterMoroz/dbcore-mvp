include(FetchContent)
FetchContent_Declare(
	googletest
	GIT_REPOSITORY https://github.com/google/googletest.git
	GIT_TAG release-1.11.0
)

FetchContent_MakeAvailable(googletest)
add_library(GTest::GTest INTERFACE IMPORTED)
target_link_libraries(GTest::GTest INTERFACE gtest_main)

add_executable(tuple_test tuple_test.cpp utils.cpp)
add_executable(rwlatch_test rwlatch_test.cpp)
add_executable(pages_manager_test pages_manager_test.cpp)
add_executable(page_guard_test page_guard_test.cpp)
add_executable(extendible_htable_page_test extendible_htable_page_test.cpp)
add_executable(b_plus_tree_insert_test b_plus_tree_insert_test.cpp)
add_executable(b_plus_tree_delete_test b_plus_tree_delete_test.cpp)
add_executable(b_plus_tree_sequential_scale_test b_plus_tree_sequential_scale_test.cpp)
add_executable(b_plus_tree_concurrent_test b_plus_tree_concurrent_test.cpp)

target_link_libraries(tuple_test PRIVATE GTest::GTest dbcore)
target_link_libraries(rwlatch_test PRIVATE GTest::GTest dbcore)
target_link_libraries(pages_manager_test PRIVATE GTest::GTest dbcore)
target_link_libraries(page_guard_test PRIVATE GTest::GTest dbcore)
target_link_libraries(extendible_htable_page_test PRIVATE GTest::GTest dbcore)
target_link_libraries(b_plus_tree_insert_test PRIVATE GTest::GTest dbcore)
target_link_libraries(b_plus_tree_delete_test PRIVATE GTest::GTest dbcore)
target_link_libraries(b_plus_tree_sequential_scale_test PRIVATE GTest::GTest dbcore)
target_link_libraries(b_plus_tree_concurrent_test PRIVATE GTest::GTest dbcore)


add_test(dbcore_gtests tuple_test rwlatch_test pages_manager_test page_guard_test extendible_htable_page_test
		b_plus_tree_insert_test b_plus_tree_delete_test b_plus_tree_concurrent_test)
