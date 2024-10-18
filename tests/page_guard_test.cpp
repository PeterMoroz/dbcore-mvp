#include <dbcore/pages_manager.h>
#include <dbcore/page_guard.h>
#include <gtest/gtest.h>

using namespace dbcore;

TEST(PageGuardTest, BasicTest)
{
    constexpr uint32_t num_of_pages = 5;
    PagesManager pages_manager(num_of_pages);

    page_id_t page_id_tmp = INVALID_PAGE_ID;
    Page* page = pages_manager.NextFreePage(&page_id_tmp);
    PageGuard pg = PageGuard(&pages_manager, page);

    EXPECT_EQ(page->GetData(), pg.GetData());
    EXPECT_EQ(page->GetPageId(), pg.PageId());
    EXPECT_EQ(1, page->GetPinCount());

    pg.Drop();

    EXPECT_EQ(0, page->GetPinCount());

    {
        page = pages_manager.NextFreePage(&page_id_tmp);
        PageGuard pg = PageGuard(&pages_manager, page);
        EXPECT_EQ(1, page->GetPinCount());
    }

    EXPECT_EQ(0, page->GetPinCount());
}

