#pragma once

#include <shared_mutex>

namespace dbcore
{

/**
 * Reader-Writer latch backed by the platform-specific mutex.
*/
class ReaderWriterLatch final
{
    ReaderWriterLatch(const ReaderWriterLatch&) = delete;
    ReaderWriterLatch& operator=(const ReaderWriterLatch&) = delete;

public:
    ReaderWriterLatch() = default;
    
    /**
     * Acquire a write latch
    */
   void WLock() { _mutex.lock(); }

   /**
    * 
    * Release a write latch
   */
  void WULock() { _mutex.unlock(); }


  /**
   * Acquire a read latch
  */
  void RLock() { _mutex.lock_shared(); }

  /**
   * Releae a read latch
  */
  void RUnlock() { _mutex.unlock_shared(); }

private:
    std::shared_mutex _mutex;
};

}
