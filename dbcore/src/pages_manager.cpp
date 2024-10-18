#include <dbcore/pages_manager.h>

#include <cassert>
#include <cstdlib>
#include <memory>

using namespace dbcore;

#define UNLIKELY(expr) __builtin_expect((expr), false)
#define LIKELY(expr) __builtin_expect((expr), true)

PagesManager::PagesManager(uint32_t num_of_pages)
    : _num_of_pages(num_of_pages)
{
    page_id_t page_id = 0;
    _pages = static_cast<Page *>(::calloc(num_of_pages, sizeof(Page)));

    for (uint32_t idx = 0; idx < num_of_pages; idx++) {
        new(_pages + idx)Page();
        _free_pages.insert(page_id);
        _free_pages_list.push_back(page_id);
        page_id++;
    }
}

PagesManager::~PagesManager()
{
    for (uint32_t idx = 0; idx < _num_of_pages; idx++) {
        Page *ptr = &_pages[idx];
        assert(ptr->_pin_count == 0);
        ptr->~Page();
    }

    ::free(_pages);
    _pages = nullptr;
}

Page* PagesManager::NextFreePage(page_id_t *page_id)
{
    assert(page_id != nullptr);
    std::lock_guard lg(_mutex);
    if (UNLIKELY(_free_pages.empty())) {
        *page_id = INVALID_PAGE_ID;
        return nullptr;        
    }

    page_id_t tmp_id = _free_pages_list.front();
    _free_pages_list.pop_front();
    _free_pages.erase(tmp_id);

    Page* page = &_pages[tmp_id];
    assert(page->_pin_count == 0);
    page->_pin_count = 1;
    page->_page_id = tmp_id;
    *page_id = tmp_id;

    return page;
}

Page* PagesManager::GetPage(page_id_t page_id)
{
    if (UNLIKELY(page_id >= _num_of_pages)) {
        return nullptr;
    }
    
    std::lock_guard lg(_mutex);
    if (UNLIKELY(_free_pages.find(page_id) != _free_pages.cend())) {
        return nullptr;
    }

    Page* page = &_pages[page_id];
    return page;
}

Page* PagesManager::GetPagePinned(page_id_t page_id)
{
    Page* page = GetPage(page_id);
    if (page != nullptr)
        page->_pin_count++;
    return page;
}

bool PagesManager::UnpinPage(page_id_t page_id, bool is_dirty)
{
    Page* page = GetPage(page_id);
    if (UNLIKELY(page == nullptr))
        return false;

    if (LIKELY(page->_pin_count > 0)) {
        page->_pin_count--;
        page->_is_dirty = is_dirty;
        return true;
    }
    return false;
}


PageGuard PagesManager::NextFreePageGuarded(page_id_t *page_id)
{
    return PageGuard(this, NextFreePage(page_id));
}

PageGuard PagesManager::GetPageGuarded(page_id_t page_id)
{
    return PageGuard(this, GetPagePinned(page_id));
}

ReadPageGuard PagesManager::GetPageRead(page_id_t page_id)
{
    return ReadPageGuard(this, GetPagePinned(page_id));   
}

WritePageGuard PagesManager::GetPageWrite(page_id_t page_id)
{
    return WritePageGuard(this, GetPagePinned(page_id));   
}
