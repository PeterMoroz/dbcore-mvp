#pragma once

#include <dbcore/coretypes.h>
#include <dbcore/rwlatch.h>

namespace dbcore
{

class PagesManager;

/**
 * Page is the basic unit of storage within the DBMS. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contain book-keeping information that is used by the pages manager. e.g.
 * pin count, dirty flag, page id, etc.
*/

class Page final
{
    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

public:
    /**
     * Default c-tor. Zeroes out the page data.
    */
    Page();

    ~Page() = default;


    /**
     * @return the actual data contained within the page
    */
   char* GetData() { return _data; }

   /**
    * @return the page id of the page
   */
   page_id_t GetPageId() const { return _page_id; }

   /**
    * @return the pin count of the page
   */
   uint32_t GetPinCount() const { return _pin_count; }


   /**
    * Acquire the page read latch.
   */
   void RLatch() { _latch.RLock(); }

   /**
    * Release the page read latch.
   */
   void RUnlatch() { _latch.RUnlock(); }

   /**
    * Acquire the page write latch.
   */
   void WLatch() { _latch.WLock(); }

   /**
    * Release the page write latch.
   */
   void WUnlatch() { _latch.WULock(); }

private:
    void ResetData();

    /** The actual data that is stored within a page. */
    char _data[PAGE_SIZE];
    /** The ID of the page. */
    page_id_t _page_id{INVALID_PAGE_ID};
    /** The pin count of the page. */
    uint32_t _pin_count{0}; // TO DO: it should be atomic<uint32_t> 
    /** True if the page is dirty, i.e. its content is different 
     * from its corresponding page on storage (disk or something else). */
    bool _is_dirty{false};
    /** Page latch */
    ReaderWriterLatch _latch;

    friend class PagesManager;
};

}
