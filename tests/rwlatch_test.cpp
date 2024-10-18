#include <dbcore/rwlatch.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

using namespace dbcore;

namespace 
{

class Counter
{
public:
    Counter() = default;

    void Inc(int v);
    int Read();

private:
    int _value{0};
    ReaderWriterLatch _latch;    
};

void Counter::Inc(int v)
{
    _latch.WLock();
    _value += v;
    _latch.WULock();
}

int Counter::Read()
{
    int v = -1;
    _latch.RLock();
    v = _value;
    _latch.RUnlock();
    return v;
}

}

TEST(RWLatchTest, BasicTest)
{
    const int num_threads = 100;
    Counter counter{};
    counter.Inc(5);

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        if (i % 2 == 0) {
            threads.emplace_back([&counter]() { counter.Read(); });
        } else {
            threads.emplace_back([&counter]() { counter.Inc(1); });
        }
    }

    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    EXPECT_EQ(counter.Read(), 55);
}
