#include <dbcore/tuple.h>
#include <dbcore/pages_manager.h>
#include <dbcore/table_heap.h>
#include <gtest/gtest.h>

#include <vector>

#include "utils.h"

using namespace dbcore;
using namespace testutils;

TEST(TupleTest, TableHeapTest)
{
    Column col1{"a", TypeId::VARCHAR, 20};
    Column col2{"b", TypeId::SMALLINT};
    Column col3{"c", TypeId::BIGINT};
    Column col4{"d", TypeId::BOOLEAN};
    Column col5{"e", TypeId::VARCHAR, 16};

    Column cols[] = {col1, col2, col3, col4, col5};
    Schema schema{cols, 5};
    Tuple tuple = ConstructTuple(schema);

    constexpr uint32_t num_of_pages = 50;
    PagesManager pages_manager(num_of_pages);

    TableHeap table_heap(pages_manager);

    std::vector<RID> rids;
    constexpr int num_records = 5000;

    for (int i = 0; i < num_records; i++) {
        auto rid = table_heap.InsertTuple(TupleMeta{0, false}, tuple);
        ASSERT_FALSE(rid == RID());
        rids.push_back(rid);
    }

    int i = 0;
    TableIterator itr = table_heap.MakeIterator();
    while (!itr.IsEnd()) {
        ASSERT_EQ(itr.GetRID(), rids[i++]);
        // TO DO: implement tuple compare operation
        // ASSERT_EQ(itr.GetTuple(), tuple);
        itr.Next();
    }
}

// int main(int argc, char** argv)
// {
// 	::testing::InitGoogleTest(&argc, argv);
// 	return RUN_ALL_TESTS();
// }
