#include <dbcore/page.h>

#include <cstring>

using namespace dbcore;

Page::Page()
{
    ResetData();
}

void Page::ResetData()
{
    std::memset(_data, 0, PAGE_SIZE);
}
