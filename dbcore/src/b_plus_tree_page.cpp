#include <dbcore/b_plus_tree_page.h>

using namespace dbcore;

void BPlusTreePage::Init(BPlusTreePageType page_type, uint32_t max_size, uint32_t key_size)
{
    _page_type = page_type;
    _max_size = max_size;
    _key_size = key_size;
}
