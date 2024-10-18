#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/page.h>

namespace dbcore
{

class PagesManager;
class Page;

class ReadPageGuard;
class WritePageGuard;

class PageGuard final
{
    PageGuard(const PageGuard&) = delete;
    PageGuard& operator=(const PageGuard&) = delete;

public:
    PageGuard() = default;

    PageGuard(PageGuard && other);
    PageGuard(PagesManager *pages_manager, Page *page);

    /**
     * @brief Drop guarded page
     * Dropping a page should clear all contents (so that
     * the page guard is no longer usefull), and it should
     * tell the PagesManager that we are done using the page.
    */
    void Drop();

    ~PageGuard();

    PageGuard& operator=(PageGuard&& other);

    page_id_t PageId() const { return _page != nullptr ? _page->GetPageId() : INVALID_PAGE_ID; }

    const char* GetData() const { return _page != nullptr ? _page->GetData() : nullptr; }

    char* GetDataMut() const { return _page != nullptr ? _page->GetData() : nullptr; }

    template <typename T>
    const T* As() const { 
        return reinterpret_cast<const T*>(GetData());
    }

    template <typename T>
    T* AsMut() {
        _is_dirty = true;
        return reinterpret_cast<T *>(GetDataMut());
    }

    ReadPageGuard UpgradeRead();
    WritePageGuard UpgradeWrite();

private:
    PagesManager *_pages_manager{nullptr};
    Page *_page{nullptr};
    bool _is_dirty{false};

    friend class ReadPageGuard;
    friend class WritePageGuard;
};


class ReadPageGuard final
{
    ReadPageGuard(const ReadPageGuard&) = delete;
    ReadPageGuard& operator=(const ReadPageGuard&) = delete;

public:
    ReadPageGuard() = default;

    ReadPageGuard(ReadPageGuard&& other);
    ReadPageGuard(PagesManager *pages_manager, Page *page);

    void Drop();

    ~ReadPageGuard();

    ReadPageGuard& operator=(ReadPageGuard&& other);

    page_id_t PageId() const { return _page_guard.PageId(); }

    const char* GetData() const { return _page_guard.GetData(); }

    template <typename T>
    const T* As() const { return _page_guard.As<T>(); }

private:
    PageGuard _page_guard;
    unsigned _counter{0};
};


class WritePageGuard final
{
    WritePageGuard(const WritePageGuard&) = delete;
    WritePageGuard& operator=(const WritePageGuard&) = delete;

public:
    WritePageGuard() = default;

    WritePageGuard(WritePageGuard&& other);
    WritePageGuard(PagesManager *pages_manager, Page *page);

    void Drop();

    ~WritePageGuard();

    WritePageGuard& operator=(WritePageGuard&& other);

    page_id_t PageId() const { return _page_guard.PageId(); }

    const char* GetData() const { return _page_guard.GetData(); }

    template <typename T>
    const T* As() const { return _page_guard.As<T>(); }

    char* GetDataMut() { return _page_guard.GetDataMut(); }

    template <typename T>
    T* AsMut() { return _page_guard.AsMut<T>(); }

private:
    PageGuard _page_guard;
    unsigned _counter{0};
};

}
