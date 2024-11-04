#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/page.h>
#include <dbcore/page_guard.h>

#include <mutex>
#include <list>
#include <unordered_set>

namespace dbcore
{

/**
 * The class provides the pages management: allocation, fetching, flushing etc. 
 * In fact all of the pages are kept in a pool of a fixed size. 
*/
class PagesManager final
{
    PagesManager(const PagesManager&) = delete;
    PagesManager& operator=(const PagesManager&) = delete;

public:
    /**
     * @brief Create a new PagesManager.
     * @param num_of_pages the number of pages kept in memory
    */
    explicit PagesManager(uint32_t num_of_pages);

    ~PagesManager();

    /**
     * @brief get the next free page (if any one).
     * @attention Dont't use this method directly!
     * Leave it for unit-test only. Use @ref NextFreePageGuarded instead.
     * @param[out] page_id id of returned page
     * @return pointer to the page or nullptr if no more pages available
    */
    Page* NextFreePage(page_id_t *page_id);

    /**
     * @brief get the requested page
     * @attention Dont't use this method directly!
     * Leave it for unit-test only. Insted use @ref GetPageGuarder, 
     * @ref GetPageRead or @ref GetPageWrite, depending on purpose.
     * @param page_id id of the page
     * @return pointer to the page or nullptr when the page_id is invalid or requested free page
    */
    Page* GetPage(page_id_t page_id);

    /**
     * @brief get the requested page and increment pin counter
     * @attention Dont't use this method directly!
     * Leave it for unit-test only. Insted use @ref GetPageGuarder, 
     * @ref GetPageRead or @ref GetPageWrite, depending on purpose.
     * @param page_id id of the page
     * @return pointer to the page or nullptr when the page_id is invalid or requested free page
    */
    Page* GetPagePinned(page_id_t page_id);


    /**
     * @brief Decrement the pin counter of a page. Set the dirty flag to indicate that page was modified.
     * @param page_id id of page to be unpinned
     * @param is_dirty true if page should be marked as dirty, false otherwise
     * @return false if the page pin count is already 0 before this call (or wrong page id is passed), true otherwise
    */
    bool UnpinPage(page_id_t page_id, bool is_dirty);



    /**
     * @brief get the next free page (if any one) wrapped into the PageGuard. 
     * @param[out] page_id id of returned page
     * @return PageGuard instance, which holds the requested page
    */
    PageGuard NextFreePageGuarded(page_id_t *page_id);

    /**
     * @brief get the requested page wrapped into the PageGuard
     * @param page_id id of the page
     * @return PageGuard instance, which holds the requested page
    */
    PageGuard GetPageGuarded(page_id_t page_id);

    /**
     * @brief get the requested page for reading. 
     * the page is wrapped into ReadPageGuard object, which keeps tracking the lifetime of the page.
     * @param page_id id of the page
     * @return ReadPageGuard instance, which holds the requested page
    */
    ReadPageGuard GetPageRead(page_id_t page_id);

    /**
     * @brief get the requested page for modify. 
     * the page is wrapped into WritePageGuard object, which keeps tracking
     * the lifetime of the page and set dirty flag when page is goes out of scope.
     * @param page_id id of the page
     * @return WritePageGuard instance, which holds the requested page
    */
    WritePageGuard GetPageWrite(page_id_t page_id);

    /**
     * @brief give back page to list of free pages. should be used when the page is not needed anymore.
     * the pin count of the page must be equal to 0, i.e. page isn't used by any thread.
     * @param page_id id of the page
     * @return false if the page page id is invalid, page is already freed or page still in use (pin count != 0),
     * true when page is freed successfully
    */
    bool GiveBackPage(page_id_t page_id);
    


private:
    /** The number of pages */
    const uint32_t _num_of_pages;
    /** Array of managed pages */
    Page *_pages{nullptr};
    /** The catalog of free pages */
    std::unordered_set<page_id_t> _free_pages;
    /** The free pages list */ 
    std::list<page_id_t> _free_pages_list;
    /** The mutex to ensure exclusive access to internal data */
    std::mutex _mutex;
};



}