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

class Scheduler
{
//Scheduler는 Core위에서 coroutine을 관리하는 역할을 하고
//Task는 coroutine을 감싸는 class임. 
public:
    int thread_id;
    std::queue<Task> work_queue; // Work Queue (코루틴 스케줄)
    std::queue<Task> wait_list;  // Wait list (코루틴 대기)
    std::mutex mutex;            // Mutex for Wait list

    // 스레드 내부 요청 큐 (외부 g_rx에서 옮겨온 Request 소비)
    MPMCQueue rx_queue;

    int shard_count;
    // SLO & Time
    static constexpr int LAT_CAP = 4096;
    static constexpr uint64_t WINDOW_NS = 2ULL * 1000000ULL; // 2ms
    static constexpr uint64_t SLO_NS = (uint64_t)SLO_THRESHOLD_MS * 1'000'000ULL;

    Scheduler(int tid) : thread_id(tid)
    {
        schedulers[tid] = this;
        core_state[tid] = STARTED; // 시작시 STARTED 단계로 consolidation을 방지.
        shard_count = 1;
    }
    struct LatSample
    {
        uint64_t ts_ns;
        uint32_t lat_ns;
    };
    std::array<LatSample, LAT_CAP> lat_ring{};
    int lat_idx = 0; // next write pos
    int lat_cnt = 0; // valid count (<= LAT_CAP)

    // 요청 완료 직후 호출 (start_ns는 Request.start_time)
    inline void record_latency(uint64_t start_ns)
    {
        uint64_t end = now_ns();
        uint64_t lat = (end > start_ns) ? (end - start_ns) : 0;
        printf("[%d]%" PRIu64 " nsec\n", thread_id, lat);
	lat_ring[lat_idx] = LatSample{end, (uint32_t)std::min<uint64_t>(lat, UINT32_MAX)};
        lat_idx = (lat_idx + 1) & (LAT_CAP - 1);
        if (lat_cnt < LAT_CAP)
            ++lat_cnt;
    }

    // 최근 WINDOW_NS에서 p99(ns) 반환. 샘플 부족 시 0
    uint64_t p99_in_recent_window() const
    {
        if (lat_cnt == 0)
            return 0;
        uint64_t cutoff = now_ns() - WINDOW_NS;

        uint32_t tmp[LAT_CAP];
        int k = 0;

        // 최근부터 역순으로 모으면 cutoff 이전에서 빨리 중단 가능
        for (int i = 0; i < lat_cnt; ++i)
        {
            int pos = (lat_idx - 1 - i) & (LAT_CAP - 1);
            const LatSample &s = lat_ring[pos];
            if (s.ts_ns < cutoff)
                break;
            tmp[k++] = s.lat_ns;
        }
        if (k < 50)
            return 0; // 윈도우 내 샘플이 너무 적으면 스킵

        int idx = (int)std::floor(0.99 * (k - 1));
        std::nth_element(tmp, tmp + idx, tmp + k);
        return tmp[idx];
    }

    bool detect_SLO_violation()
    {
        if (rx_queue.size() > Q_B * MAX_Q)
        {
            return true;
        }
        else
            return false;
    }
    bool detect_SLO_violation_slice()
    {
        if (rx_queue.size() > Q_B * MAX_Q)
            return true; // 기존 조건 유지
        uint64_t p99ns = p99_in_recent_window();
        bool ret = (p99ns > 0 && p99ns > SLO_NS);
        if (ret)
        {
            printf("Latency Violate\n");
            return true;
        }
        else
        {
            return false;
        }
    }

    bool is_idle()
    {
        if (rx_queue.size() < Q_A * MAX_Q)
        {
            return true;
        }
        else
            return false;
    }
    void emplace(Task &&task)
    {
        work_queue.push(std::move(task));
    }

    void enqueue_to_wait_list(Task &&task)
    {
        std::lock_guard<std::mutex> lock(mutex);
        wait_list.push(std::move(task));
    }

    bool is_empty()
    {
        std::lock_guard<std::mutex> lock(mutex);
        return work_queue.empty() && wait_list.empty();
    }
    
    // 한 번에 wait_list -> work_queue로 옮기고, 한 개 코루틴을 실행
    void schedule(){
        // 1. Wait list -> Work Queue (대기중인 작업을 실행 큐로 옮김)
        if (!wait_list.empty()) {
            std::lock_guard<std::mutex> lock(mutex);
            while (!wait_list.empty()) {
                work_queue.push(std::move(wait_list.front()));
                wait_list.pop();
            }
        }

        // 2. Work Queue에 작업이 없으면 할 일이 없음
        if (work_queue.empty()) {
            return;
        }

        // 3. Work Queue에서 코루틴을 하나 꺼내 실행
        Task task = std::move(work_queue.front());
        work_queue.pop();

        if (task.source && (*task.source)) {
            (*task.source)(this); // 코루틴 실행
        }

        // 4. 코루틴이 아직 끝나지 않았다면 다시 큐에 넣어 다음 기회에 실행
        if (!task.is_done()) {
            work_queue.push(std::move(task));
        }
    }// void schedule()
};