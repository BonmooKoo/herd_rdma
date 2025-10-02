#pragma once
#include <cstdint>
#include <atomic>
#include <boost/coroutine/symmetric_coroutine.hpp>

extern std::atomic<bool> g_stop;
extern Route route_tbl[MAX_CORES];

// 샤드 소유권 테이블 구조체 (C++ 스타일)
struct Route {
    std::atomic<int> owner;
};

// Herd의 정보를 담도록 확장된 Request 구조체
struct Request {
    int type{0};
    uint64_t key{0};
    uint64_t start_time{0};

    // Herd 연동에 필요한 정보
    int req_buf_offset{0};
    int client_id{0};
};

class Scheduler;
using coro_t = boost::coroutines::symmetric_coroutine<Scheduler *>;
using CoroCall = coro_t::call_type;

// 논리적 ID를 포함하는 Task 구조체
struct Task
{
//Coroutine의 context를 감싸는 구조체.
    CoroCall *source = nullptr;
    int utask_id{0}; // 고유 id (확인용)
    int thread_id{0}; // thread id ~= 담당하는 shard id
    int task_type{0};
    uint64_t key{0};
    uint64_t value{0};

    Task() = default;
    Task(CoroCall *src, int tid, int uid, int type = 0)
        : source(src), utask_id(uid), thread_id(tid), task_type(type) {}

    Task(Task &&other) noexcept
        : source(other.source), utask_id(other.utask_id),
          thread_id(other.thread_id), task_type(other.task_type),
          key(other.key), value(other.value)
    {
        other.source = nullptr;
    }

    Task &operator=(Task &&other) noexcept
    {
        if (this != &other)
        {
            source = other.source;
            utask_id = other.utask_id;
            thread_id = other.thread_id;
            task_type = other.task_type;
            key = other.key;
            value = other.value;
            other.source = nullptr;
        }
        return *this;
    }

    ~Task()
    {
        delete source;
    }

    bool is_done() const
    {
        return source == nullptr || !(*source);
    }

    void resume(Scheduler *sched)
    {
        if (source && *source)
        {
            (*source)(sched); // resume coroutine with scheduler pointer
        }
    }
    void set_type(int type) { task_type = type; }
    int get_type() const { return task_type; }
};
