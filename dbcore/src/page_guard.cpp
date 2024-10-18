#include <dbcore/page_guard.h>
#include <dbcore/pages_manager.h>

// #include <iostream>
// #include <thread>

using namespace dbcore;

// PageGuard::PageGuard(PageGuard && other)
//     : _pages_manager(other._pages_manager)
//     , _page(other._page)
//     , _is_dirty(other._is_dirty)
// {
//     other._page = nullptr;
//     other._pages_manager = nullptr;
//     other._is_dirty = false;
// }

PageGuard::PageGuard(PageGuard && other)
{
    std::swap(_pages_manager, other._pages_manager);
    std::swap(_page, other._page);
    std::swap(_is_dirty, other._is_dirty);
}

PageGuard::PageGuard(PagesManager *pages_manager, Page *page)
    : _pages_manager(pages_manager)
    , _page(page)
{

}

void PageGuard::Drop()
{
    if (_pages_manager && _page) {
        _pages_manager->UnpinPage(_page->GetPageId(), _is_dirty);
    }
    _pages_manager = nullptr;
    _page = nullptr;
    _is_dirty = false;
}

PageGuard::~PageGuard()
{
    Drop();
}

PageGuard& PageGuard::operator=(PageGuard&& other)
{
    if (this != &other)
    {
        // _pages_manager = other._pages_manager;
        // _page = other._page;
        // _is_dirty = other._is_dirty;

        // other._pages_manager = nullptr;
        // other._page = nullptr;
        // other._is_dirty = false;

        std::swap(_pages_manager, other._pages_manager);
        std::swap(_page, other._page);
        std::swap(_is_dirty, other._is_dirty);
    }
    return *this;
}

ReadPageGuard PageGuard::UpgradeRead() 
{ 
    return ReadPageGuard(_pages_manager, _page); 
}

WritePageGuard PageGuard::UpgradeWrite() 
{ 
    return WritePageGuard(_pages_manager, _page); 
}


ReadPageGuard::ReadPageGuard(ReadPageGuard&& other)
    : _page_guard(std::move(other._page_guard))
{

}

ReadPageGuard::ReadPageGuard(PagesManager *pages_manager, Page *page)
    : _page_guard(pages_manager, page)
{
    if (_page_guard._page) {
        _page_guard._page->RLatch();
    }
}

void ReadPageGuard::Drop()
{
    if (_page_guard._page) {
        _page_guard._page->RUnlatch();
    }
    _page_guard.Drop();
}

ReadPageGuard::~ReadPageGuard()
{
    Drop();
}

ReadPageGuard& ReadPageGuard::operator=(ReadPageGuard&& other)
{
    if (this != &other) {
        Drop();
        _page_guard = std::move(other._page_guard);
    }
    return *this;
}


WritePageGuard::WritePageGuard(WritePageGuard&& other)
    : _page_guard(std::move(other._page_guard))
{

}

WritePageGuard::WritePageGuard(PagesManager *pages_manager, Page *page)
    : _page_guard(pages_manager, page)
{
    if (_page_guard._page) {
        _page_guard._page->WLatch();
        // std::cout << "Thread " << std::this_thread::get_id() 
        //     << " acquired wlatch on the page " << _page_guard._page->GetPageId()
        //     << std::endl;
    }
}

void WritePageGuard::Drop()
{
    if (_page_guard._page) {
        _page_guard._page->WUnlatch();
        // std::cout << "Thread " << std::this_thread::get_id() 
        //     << " released wlatch on the page " << _page_guard._page->GetPageId()
        //     << std::endl;
    }
    _page_guard.Drop();
}

WritePageGuard::~WritePageGuard()
{
    Drop();
}

WritePageGuard& WritePageGuard::operator=(WritePageGuard&& other)
{
    if (this != &other) {
        Drop();
        _page_guard = std::move(other._page_guard);
    }
    return *this;
}
