#include <dbcore/pages_manager.h>
#include <gtest/gtest.h>

#include <limits>
#include <random>
#include <algorithm>
#include <iterator>

#include <cstring>


using namespace dbcore;

TEST(PagesManagerTest, BasicTest)
{
    constexpr uint32_t num_of_pages = 5;
    PagesManager pages_manager(num_of_pages);

    std::random_device r;
    std::default_random_engine rng(r());

    constexpr int lower_bound = static_cast<int>(std::numeric_limits<char>::min());
    constexpr int upper_bound = static_cast<int>(std::numeric_limits<char>::max());
    static_assert(upper_bound - lower_bound == 255);

    std::uniform_int_distribution<int> uniform_distribution(lower_bound, upper_bound);
    char random_binary_data_0[PAGE_SIZE];

    std::generate(std::begin(random_binary_data_0), std::end(random_binary_data_0),
        [&uniform_distribution, &rng](){ return uniform_distribution(rng); });
    // insert null-terminator in the middle and at the end, 
    // to check that PagesManager handles all data in the same way
    random_binary_data_0[PAGE_SIZE / 2] = '\0';
    random_binary_data_0[PAGE_SIZE - 1] = '\0';

    page_id_t page_ids[num_of_pages] = { INVALID_PAGE_ID };
    // scenario: get a first free page
    Page* page0 = pages_manager.NextFreePage(&page_ids[0]);
    ASSERT_NE(nullptr, page0);
    EXPECT_EQ(0, page_ids[0]);

    // scenario: write and read content of the obtained page
    std::memcpy(page0->GetData(), random_binary_data_0, PAGE_SIZE);
    EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data_0, PAGE_SIZE));

    // scenario: consume all free pages, write/read data
    for (uint32_t n = 1; n < num_of_pages; n++) {
        Page* page = pages_manager.NextFreePage(&page_ids[n]);
        EXPECT_NE(nullptr, page);
        EXPECT_NE(INVALID_PAGE_ID, page_ids[n]);

        char random_binary_data[PAGE_SIZE];
        std::generate(std::begin(random_binary_data), std::end(random_binary_data),
            [&uniform_distribution, &rng](){ return uniform_distribution(rng); });

        std::memcpy(page->GetData(), random_binary_data, PAGE_SIZE);
        EXPECT_EQ(0, std::memcmp(page->GetData(), random_binary_data, PAGE_SIZE));
    }

    // scenario: check that no more free pages available
    for (uint32_t n = num_of_pages; n < num_of_pages * 2; n++) {
        page_id_t page_id;
        Page* page = pages_manager.NextFreePage(&page_id);
        EXPECT_EQ(nullptr, page);
        EXPECT_EQ(INVALID_PAGE_ID, page_id);
    }

    // scenario: get the pages by their ids
    for (uint32_t i = 0; i < num_of_pages; i++) {
        Page* page = pages_manager.GetPage(page_ids[i]);
        EXPECT_NE(nullptr, page);
    }

    // scenario: the page have to preserve data writeen before
    page0 = nullptr;
    page0 = pages_manager.GetPage(0);
    EXPECT_NE(nullptr, page0);

    EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data_0, PAGE_SIZE));

    
    for (uint32_t i = 0; i < num_of_pages; i++) {
        pages_manager.UnpinPage(page_ids[i], false);
    }
}
