set(DBCORE_SRC 
    src/column.cpp
    src/catalog.cpp
    src/index.cpp
    src/schema.cpp
    src/tuple.cpp
    src/value.cpp
    src/page.cpp
    src/index_info.cpp
    src/page_guard.cpp
    src/table_heap.cpp
    src/table_info.cpp
    src/table_page.cpp
    src/tuple_compare.cpp
    src/table_iterator.cpp
    src/pages_manager.cpp
    src/b_plus_tree_internal_page.cpp
    src/b_plus_tree_leaf_page.cpp
    src/b_plus_tree_page.cpp
    src/b_plus_tree.cpp
    src/extendible_htable_bucket_page.cpp
    src/extendible_htable_header_page.cpp
    src/extendible_htable_directory_page.cpp)

add_library(dbcore STATIC ${DBCORE_SRC})
target_include_directories(dbcore PUBLIC include)
