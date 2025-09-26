// Boost coroutine version of the original cpp coroutine example
// export LANG=ko_KR.UTF-8
// Compile with:
// g++ -std=c++17 one_sided_consol.cpp -o one_sided_consol -I./ -I/usr/local/include   -L/usr/local/lib -Wl,-rpath,/usr/local/lib   -lboost_coroutine -lboost_context -lboost_system   -libverbs -lmemcached -lpthread
// 지금은 코루틴을 직접 옮기지만, 이 구현이 너무 무거우면 request만 옮기는 구현이 필요함.

#include <boost/coroutine/symmetric_coroutine.hpp>
#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cassert>
#include <sys/syscall.h>
#include <unistd.h>
#include <pthread.h>
#include <cmath>
// RDMA
#include "rdma_common.h"
#include "rdma_verb.h"
#include "keeper.h"

constexpr int MAX_Q = 64;   // Max Queue length
constexpr double Q_A = 0.2; // Queue Down threshold-> Core consolidation
constexpr double Q_B = 0.9; // Queue Up threshold -> Load Balancing
constexpr int MAX_THREADS = 4;
constexpr int SLO_THRESHOLD_MS = 5;
constexpr int SCHEDULING_TICK = 16;
// 실험 종료를 알리는 전역변수
std::atomic<bool> g_stop{false};

enum CoreState
{
    SLEEPING,
    ACTIVE,
    CONSOLIDATING,
    STARTED,
    CONSOLIDATED
};
/*
1) SLEEPING : Sleep flag = 1 인 상태
2) ACTIVE : 실행중
3) CONSOLIDATING : 현재 coroutine migration 진행중. 일종의 core간의 global lock 역할
4) STARTED : 방금 실행해서 당분간은 coroutine migration 진행하지 마셈
5) CONSOLIDATED : 방금 consolidation 수행했으니 당분간 하지마셈
*/
enum RequestType
{
    OP_PUT,
    OP_GET,
    OP_DELETE,
    OP_RANGE,
    OP_UPDATE
};

class Scheduler;
using coro_t = boost::coroutines::symmetric_coroutine<Scheduler *>;
using CoroCall = coro_t::call_type;   // caller
using CoroYield = coro_t::yield_type; // callee

pid_t gettid()
{
    return syscall(SYS_gettid);
}
void bind_cpu(int cpu_num)
{
    pthread_t this_thread = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_num, &cpuset);
    pthread_setaffinity_np(this_thread, sizeof(cpu_set_t), &cpuset);
}

Scheduler *schedulers[MAX_THREADS] = {nullptr};
std::condition_variable cvs[MAX_THREADS];
std::mutex cv_mutexes[MAX_THREADS];
std::atomic<bool> sleeping_flags[MAX_THREADS];
std::atomic<CoreState> core_state[MAX_THREADS];

//=====================
// Latency
//=====================
static inline uint64_t now_ns()
{
    using clock = std::chrono::steady_clock;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               clock::now().time_since_epoch())
        .count();
}

// =====================
// Request & MPMC Queue
// =====================
struct Request
{
    int type{0};
    uint64_t key{0};
    uint64_t value{0};
    // 필요하면 타임스탬프, ctx 포인터 등 확장
    uint64_t start_time;
};

// 간단 MPMC 스타일 큐
class MPMCQueue
{
    mutable std::mutex m_;
    std::condition_variable cv_;
    std::deque<Request> q_;

public:
    bool try_pop(Request &out)
    {
        std::lock_guard<std::mutex> lk(m_);
        if (q_.empty())
            return false;
        out = std::move(q_.front());
        q_.pop_front();
        return true;
    }
    void push(Request &&r)
    {
        {
            std::lock_guard<std::mutex> lk(m_);
            q_.push_back(std::move(r));
        }
        cv_.notify_one();
    }
    size_t size() const
    {
        // std::lock_guard<std::mutex> lk(m_);
        return q_.size();
    }
    void steal_all(std::deque<Request> &out)
    {
        std::lock_guard<std::mutex> lk(m_);
        while (!q_.empty())
        {
            out.push_back(std::move(q_.front()));
            q_.pop_front();
        }
    }
    void push_bulk(std::deque<Request> &in)
    {
        if (in.empty())
            return;
        {
            std::lock_guard<std::mutex> lk(m_);
            while (!in.empty())
            {
                q_.push_back(std::move(in.front()));
                in.pop_front();
            }
        }
        cv_.notify_all();
    }

} g_rx;

// =====================
// Task & Scheduler
// =====================
struct Task
{
    CoroCall *source = nullptr;
    int utask_id{0};
    int thread_id{0};
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
public:
    int thread_id;
    std::queue<Task> work_queue; // Work Queue (코루틴 스케줄)
    std::queue<Task> wait_list;  // Wait list (코루틴 대기)
    std::mutex mutex;            // Mutex for Wait list

    // 스레드 내부 요청 큐 (외부 g_rx에서 옮겨온 Request 소비)
    MPMCQueue rx_queue;

    // RDMA IO 대기중인 코루틴
    std::unordered_map<int, Task> blocked;
    int blocked_num{0};
    int block_hint{-1};

    // SLO & Time
    static constexpr int LAT_CAP = 4096;
    static constexpr uint64_t WINDOW_NS = 2ULL * 1000000ULL; // 2ms
    static constexpr uint64_t SLO_NS = (uint64_t)SLO_THRESHOLD_MS * 1'000'000ULL;

    Scheduler(int tid) : thread_id(tid)
    {
        schedulers[tid] = this;
        core_state[tid] = ACTIVE; // 시작시 STARTED 단계로 consolidation을 방지.
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
        printf("[%d]%llu nsec\n",thread_id,lat);
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
        // std::lock_guard<std::mutex> lock(mutex);
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

    // RDMA
    void block_self(int utask_id) { block_hint = utask_id; }
    // 코루틴을 blocked 리스트에 넣기 -- RDMA request 전송한 코루틴이 스스로
    void block_task(Task &&t)
    {
        blocked[t.utask_id] = std::move(t);
        blocked_num++;
    }

    // RDMA 완료 시 깨우기 -- Master가 코루틴 깨우
    bool wake_task(int uid)
    {
        auto it = blocked.find(uid);
        if (it != blocked.end())
        {
            work_queue.push(std::move(it->second));
            blocked.erase(it);
            blocked_num--;
            return true;
        }
        printf("[%d]wake_task wrong<%d>\n", thread_id, uid);
        return false;
    }

    // 한 번에 wait_list -> work_queue로 옮기고, 한 개 코루틴을 실행
    void schedule()
    {
        // 1) Wait list -> Work Queue
        if (!wait_list.empty())
        {
            printf("[%d]pulling wait_list\n", thread_id);
            std::lock_guard<std::mutex> lock(mutex);
            while (!wait_list.empty())
            {
                // work_queue.push(std::move(wait_list.front()));
                emplace(std::move(wait_list.front()));
                // printf("[%d]Enqueue<%d>\n",thread_id,wait_list.front().utask_id);
                wait_list.pop();
            }
            printf("[%d]pull fin <%d:%d>\n", thread_id, work_queue.size(), wait_list.size());
        }

        // 2) RDMA poll 확인
        int next_id = poll_coroutine(this->thread_id);
        //int next_id = -1;
        if (next_id < 0)
        { // no RDMA
            // 2-1) poll 실패 → 일반 코루틴 하나 실행
	    if (work_queue.empty())
                return;

            Task task = std::move(work_queue.front());
	    work_queue.pop();

            (*task.source)(this); 

	     //여기서 resume 됨
            if (!task.is_done() && !g_stop.load())
            {
                if (block_hint == task.utask_id)
                {
                    block_task(std::move(task));
                    block_hint = -1;
                    return;
                }
                else
                {
                    emplace(std::move(task));
                }
            }
        }
        else
        { // RDMA polled
            printf("[%d]polled<%d>\n",thread_id,next_id);
            wake_task(next_id);
        }
    } // void schedule()
};
// =====================
// Sleep / Wake helpers
// =====================

void sleep_thread(int tid)
{
    std::unique_lock<std::mutex> lock(cv_mutexes[tid]);
    sleeping_flags[tid] = true;
    cvs[tid].wait(lock, [&]
    { return !sleeping_flags[tid] || g_stop.load(); }); // 실험용
    // cvs[tid].wait(lock, [&]{ return !sleeping_flags[tid]; });
}

void wake_up_thread(int tid)
{
    {
        std::lock_guard<std::mutex> lk(cv_mutexes[tid]);
        sleeping_flags[tid] = false;
    }
    cvs[tid].notify_one();
}
void wake_all_threads(int num_thread)
{
    for (int i = 0; i < MAX_THREADS; ++i)
    {
        {
            std::lock_guard<std::mutex> lk(cv_mutexes[i]);
            sleeping_flags[i] = false;
        }
        cvs[i].notify_all();
    }
}

// ================
//   Migration 처리
// ================
// 오프로딩 시 코루틴 이동 (from -> to)
int post_mycoroutines_to(int from_tid, int to_tid)
{
    int count = 0;
    auto &to_sched = *schedulers[to_tid];
    auto &from_sched = *schedulers[from_tid];

    // std::scoped_lock lk(from_sched.mutex, to_sched.mutex);
    std::lock_guard<std::mutex> lk_to(to_sched.mutex);
    while (!from_sched.work_queue.empty())
    {
        count++;
        to_sched.wait_list.push(std::move(from_sched.work_queue.front()));
        printf("[%d>>%d]post_coroutine<%d>\n",from_tid,to_tid,from_sched.work_queue.front().utask_id);
        from_sched.work_queue.pop();
    }
    printf("[%d:%d]post_coroutineto<%d:%d:%d>\n", from_tid, from_sched.work_queue.size(), to_tid, to_sched.work_queue.size(), to_sched.wait_list.size());
    return count;
}

// global rx_queue → thread_local rx_queue
static inline void pump_external_requests_into(Scheduler &sched, int burst = 32)
{
    Request r;
    int cnt = 0;
    while (cnt < burst && g_rx.try_pop(r))
    {
        sched.rx_queue.push(std::move(r));
        cnt++;
    }
    // printf("[%d]Pulled%d\n",sched.thread_id,cnt);
}

int sched_load(int c)
{
    int wq = schedulers[c]->work_queue.size();
    int rx = schedulers[c]->rx_queue.size();
    return wq + rx;
}

// CAS(ACTIVE,CONSOL)
bool state_active_to_consol(int tid)
{
    CoreState expected = ACTIVE;
    return core_state[tid].compare_exchange_strong(expected, CONSOLIDATING);
}
bool state_sleep_to_consol(int tid)
{
    CoreState expected = SLEEPING;
    return core_state[tid].compare_exchange_strong(expected, CONSOLIDATING);
}

int pick_active_random(int self, int also_exclude = -1)
{
    std::array<int, MAX_THREADS> cand{};
    int n = 0;
    for (int c = 0; c < MAX_THREADS; ++c)
    {
        if (c == self || c == also_exclude)
            continue;
        if (!schedulers[c])
            continue;
        if (core_state[c].load() != ACTIVE)
            continue;
        cand[n++] = c;
    }
    if (n == 0)
        return -1;
    std::uniform_int_distribution<int> dist(0, n - 1);
    return cand[rand() % n];
}

int power_of_two_choices(int self)
{
    int a = pick_active_random(self);
    if (a < 0)
        return -1;
    int b = pick_active_random(self, a);
    if (b < 0)
        return a;

    size_t la = sched_load(a);
    size_t lb = sched_load(b);
    return (lb > la) ? a : b; // 더 가벼운 쪽(통합 대상으로 좋음)
}

int pick_and_lock_target_pow2(int self)
{

    int a = pick_active_random(self);
    int b = pick_active_random(self, a);

    if (a < 0 && b < 0)
        return -1;

    // 더 가벼운 쪽을 먼저 시도
    int first = a, second = b;
    if (a >= 0 && b >= 0 && sched_load(b) < sched_load(a))
        std::swap(first, second);

    for (int t : {first, second})
    {
        if (t < 0)
            continue;
        CoreState expected = ACTIVE;
        if (core_state[t].compare_exchange_strong(
                expected, CONSOLIDATING,
                std::memory_order_acq_rel, std::memory_order_relaxed))
        {
            return t; // 락 성공
        }
        else
        {
            //printf("CAS{%d}isNOTACTIVE\n", t);
        }
    }
    return -1; // 둘 다 실패 → 호출부에서 재시도
}

// 다른애한테 넘겨주고 나는 종료
int core_consolidation(Scheduler &sched, int tid)
{
    printf("[%d] 0\n", tid);
    // 0)
    if (!state_active_to_consol(tid))
    {
        printf("[%d] %d by someone\n", tid, core_state[tid].load());
        return -1; // Someone is giving me a job
    }
    printf("[%d]ACTIVE->CONSOL\n", tid);
    // 1)target
    int target = -1;
    // 5회 시도
    for (int att = 0; att < 5; ++att)
    {
        /*int cand = power_of_two_choices(tid);
        if (cand < 0) continue;
        if (state_active_to_consol(cand)) {
            printf("[%d>%d]CONSOL\n",tid,cand);
            target = cand;
            break;// target = CONSOLIDATING
        }*/
        target = pick_and_lock_target_pow2(tid);
        if (target >= 0)
            break;
    }
    if (target < 0)
    {
        printf("[%d]NO target\n", tid);
        // Consolidation Failed
        core_state[tid] = CONSOLIDATED; // Failed. Instead of consolidation, Just Empty my work queue and sleep
        return -2;
    }
    printf("[%d>%d]CONSOL\n", tid, target);

    // 2)move coroutine
    post_mycoroutines_to(tid, target);
    // 3)move request
    std::deque<Request> tmp;
    sched.rx_queue.steal_all(tmp);
    schedulers[target]->rx_queue.push_bulk(tmp);
    core_state[target] = CONSOLIDATED;
    return target;
}

// 자는 스레드를 깨울 때 코루틴 일부 전달
int load_balancing(int from_tid, int to_tid)
{
    Scheduler *to_sched = schedulers[to_tid];
    Scheduler *from_sched = schedulers[from_tid];
    printf("[%d>>%d]TryLoadBalancing<%d>\n", from_tid, to_tid, from_sched->work_queue.size());
    if (!state_sleep_to_consol(to_tid))
    {
        // 이미 SLEEPING이 아님
        printf("[%d>%d]Not Sleeping\n", from_tid, to_tid);
        return -2;
    }
    // std::scoped_lock lk(from_sched.mutex, to_sched.mutex);
    std::lock_guard<std::mutex> lk_to(to_sched->mutex);
    int half = 0;
    // from의 절반 정도만 넘기는 예시 (필요 시 정책 조정)
    std::queue<Task> tmp;
    int total = from_sched->work_queue.size();
    half = total / 2;
    for (int i = 0; i < half; ++i)
    {
        Task t = std::move(from_sched->work_queue.front());
        printf("[%d>%d]Load<%d>\n",from_tid,to_tid,t.utask_id);
        to_sched->wait_list.push(std::move(t));
        from_sched->work_queue.pop();
    }
    
    return half;
}

// ===============
// Request 처리부
// ===============
// request handler func

static void process_request_on_worker(const Request &r, int tid, int coroid)
{
    // RDMA 아닌 simple CPU work
    // printf("[Worker%d-%d]%d\n",tid,coroid,r.key);
}

// 워커 코루틴: 깨어날 때마다 rx_queue에서 Request를 소비
// yield_type은 void에서 Scheduler*로 변경
// call_type은 Scheduler*를 받도록 변경

void print_worker(Scheduler &sched, int tid, int coroid)
{
    auto *source = new CoroCall([tid, coroid](CoroYield &yield)
                                {
                                    Scheduler *current = yield.get(); // (*call)(this)로 전달된 포인터
                                    printf("[Coroutine%d-%d]started\n", gettid(), coroid);

                                    Request r;
                                    while (true)
                                    {
                                        if (!current->rx_queue.try_pop(r))
                                        {
                                            yield();
                                            current = yield.get();
                                            continue;
                                        }
					int thread_id=current->thread_id;
                                        switch (r.type)
                                        {
                                        case OP_PUT:
                                            printf("[%d-%d]WRITE\n",thread_id,coroid);rdma_write_nopoll(/*client addr*/reinterpret_cast<uint64_t>(&r.value),/*server_addr*/(r.key % (ALLOCSIZE / SIZEOFNODE)) * SIZEOFNODE, 8,0,thread_id,coroid);
                                            current->block_self(coroid);
                                            //process_request_on_worker(r, tid, coroid);
                                            yield();
                                            current = yield.get(); // RDMA poll됨.
					    current->record_latency(r.start_time);
                                            break;
                                        case OP_GET:
                                            rdma_read_nopoll((r.key % (ALLOCSIZE / SIZEOFNODE)) * SIZEOFNODE,8, 0, thread_id, coroid);
                                            current->block_self(coroid);
                                            //process_request_on_worker(r, tid, coroid);
                                            yield();
                                            current = yield.get(); // RDMA poll됨.
					    current->record_latency(r.start_time);
                                            break;
                                        default:
                                            // RDMA 필요 없는 경우는 바로 처리
                                            process_request_on_worker(r, thread_id, coroid);
                                            yield();
                                            current = yield.get();
					    current->record_latency(r.start_time);
                                            continue;
                                        }
                                    } });

    Task task(source, tid, coroid, 0);
    sched.emplace(std::move(task));
}

void master(Scheduler &sched, int tid, int coro_count)
{
    bind_cpu(tid);
    if (sleeping_flags[tid])
    {
        // printf("[%d]Sleep\n", tid);
        core_state[tid] = SLEEPING;
        sleep_thread(tid); // 미리 재움
        // printf("[%d]Wakeup\n", tid);
        core_state[tid] = STARTED;
    }
    for (int i = 0; i < coro_count; ++i)
    {
        print_worker(sched, tid, tid * coro_count + i);
    }
    pump_external_requests_into(sched, /*burst*/ 64);

    int sched_count = 0;
    while (!g_stop.load())
    {
        sched.schedule();//RDMA poll & start coroutine
        if (++sched_count >= SCHEDULING_TICK)
        {
            // printf("[%d]Status=%d\n",tid,core_state[tid].load());
            sched_count = 0;
            // 3-0) CONSOLIDATED/SLEEPING/STARTED -> ACTIVE
            if (core_state[tid] == SLEEPING || core_state[tid] == CONSOLIDATED || core_state[tid] == STARTED)
            {
                core_state[tid] = ACTIVE;
            }
            else if (core_state[tid] == CONSOLIDATING)
            {
                // Do nothing just keep go
            }
            else if (core_state[tid] == ACTIVE)
            {
                // 3-1) 저부하이면 코어 정리 (core 0은 제외)
                if (sched.is_idle() && tid != 0)
                {
                    // printf("Core[%d] idle\n", tid);
                    int cc = core_consolidation(sched, tid);
                    // printf("Core[%d] cc:%d\n", tid, cc);
                    if (cc >= 0)
                    {
                        // 현재 work_queue의 코루틴이랑 실행전 request 싹 넘겼음
                        // 자기전에 대기중인 RDMA request 다 처리함
                        while (sched.blocked_num > 0 | sched.rx_queue.size() > 0)
                        {
                            sched.schedule();
                        }
                        printf("[%d] sleep after CC\n", tid);
                        core_state[tid] = SLEEPING;
                        if (!g_stop.load())
                        {
                            sleep_thread(tid);         // 넘기고 잠자기
                            core_state[tid] = STARTED; // Wakeup 후 ACTIVE
                        }
                    }
                    else if (cc == -2)
                    {
                        // CC 실패: 타겟을 못 잡음. 내 CONSOLIDATING 해제(짧은 쿨다운 의미로 CONSOLIDATED).
                        core_state[tid] = CONSOLIDATED;

                        // 1) 남은 RDMA 완료를 너무 오래 돌지 않게 예산 한도 내에서만 처리
                        while (sched.blocked_num > 0 || sched.work_queue.empty()  || sched.rx_queue.size() > 0)
                        {
                            sched.schedule();
                        }
                        printf("[%d]Work n Sleep\n", tid);
                        core_state[tid] = SLEEPING;
                        if (!g_stop.load())
                        {
                            sleep_thread(tid);         // 넘기고 잠
                            core_state[tid] = STARTED; // 깨어난 뒤 다음 틱에 ACTIVE로 복원됨
                        }
                    }
                    else
                    { // cc ==-1 someone is giving me his coroutine
                    }
                }
                // 3-2) SLO 위반 시 잠자는 스레드 깨워 이관
                else if (!g_stop.load() && sched.detect_SLO_violation_slice())
                {
                    // printf("[%d]DetectSLOviolation\n",tid);
                    // 3-2-1) first, set my state to CONSOLIDATED to prevent consolidation
                    if (state_active_to_consol(tid))
                    {
                        // 3-2-2) try load balancing
                        int target = -1;
                        for (int i = 0; i < MAX_THREADS; i++)
                        {
                            if (i != tid && sleeping_flags[i])
                            {
                                target = i;
                                break;
                            }
                        }
                        if (target == -1)
                        {
                            // printf("[%d]No target to LoadBalance\n",tid);
                            core_state[tid] = CONSOLIDATED;
                        }
                        else
                        {
                            int lb = load_balancing(tid, target);
                            if (lb >= 0)
                            {
                                wake_up_thread(target); // 깨워
                                core_state[tid] = CONSOLIDATED;
                                printf("[%d>>%d]LoadBalancingEnd\n", tid, target);
                            }
                            else if (lb == -2)
                            {
                                // target is consolidated by some body
                                core_state[tid] = CONSOLIDATED;
                                // printf("[%d]load_balancing fail\n", tid);
                            }
                        }
                    }
                    else
                    { // CAS failed- someone is consolidating me
                        printf("[%d]LoadBalance:CASfailed\n", tid);
                    }
                }
            } // end else (core_state == ACTIVE)
            // 3-3) Pull request
            pump_external_requests_into(sched, /*burst*/ 64);
        } // end if (++sched_count >= SCHEDULING_TICK)
    } // end while (!g_stop.load())

    // drain
    pump_external_requests_into(sched, 1024);
    while (sched.blocked_num > 0 || sched.work_queue.empty()  || sched.rx_queue.size() > 0)
    {
        sched.schedule();
    }
    printf("[%d]Master ended\n",sched.thread_id);
}

void thread_func(int tid, int coro_count)
{
    bind_cpu(tid);
    Scheduler sched(tid);
    master(sched, tid, coro_count);
    printf("Thread%dfinished\n", tid);
}
void timed_producer(int num_thread, int qps, int durationSec);

int main()
{
    std::thread thread_list[MAX_THREADS];
    printf("RDMA Connection\n");
    for (int i = 0; i < MAX_THREADS; i++)
    {
        thread_list[i] = std::thread(client_connection, 0, MAX_THREADS, i);
    }
    for (int i = 0; i < MAX_THREADS; i++)
    {
        thread_list[i].join();
    }

    printf("Start\n");
    const int coro_count = 10;  // 워커 코루틴 수
    const int num_thread = 2;   // 워커 스레드 수
    const int durationSec = 10; // 실험 시간 (초)
    const int qps = 500000;     // 초당 요청 개수

    // sleeping_flags 초기화
    for (int i = 0; i < num_thread; i++)
        sleeping_flags[i] = false;
    for (int i = num_thread; i < MAX_THREADS; i++)
        sleeping_flags[i] = true;

    // 프로듀서 시작 (T초/QPS)
    std::thread producer(timed_producer, num_thread, qps, durationSec);
    for (int i = num_thread; i < MAX_THREADS; i++)
    {
        thread_list[i] = std::thread(thread_func, i, coro_count);
    }
    sleep(1);
    // 시간 측정
    uint64_t now = std::time(nullptr);
    printf("===============Start time : %lu ==============\n", now);
    // 워커 시작
    for (int i = 0; i < num_thread; i++)
    {
        thread_list[i] = std::thread(thread_func, i, coro_count);
    }

    // 프로듀서 종료 대기
    producer.join();
    now = std::time(nullptr);
    printf("===============End time : %lu ================\n", now);
    // 잠든 master 깨우기
    wake_all_threads(num_thread);
    // 워커 조인
    for (int i = 0; i < MAX_THREADS; i++)
    {
        if (thread_list[i].joinable())
            thread_list[i].join();
    }

    return 0;
}

void timed_producer(int num_thread, int qps, int durationSec)
{
    bind_cpu(num_thread);

    using clock = std::chrono::steady_clock;
    auto start = clock::now();
    auto deadline = start + std::chrono::seconds(durationSec);

    // 한 요청 간격
    auto period = std::chrono::nanoseconds(1'000'000'000LL / std::max(1, qps));
    auto next = start;

    uint64_t k = 1;
    while (!g_stop.load() && clock::now() < deadline)
    {
        Request r;
        r.type = OP_PUT; // OP_PUT/GET/DELETE/RANGE/UPDATE 중 하나
        r.key = k++;
        r.value = k * 10;
        r.start_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           clock::now().time_since_epoch())
                           .count();
        // [당신의 현재 g_rx는 mutex 기반이라 실패 반환이 없음]
        g_rx.push(std::move(r));

        // 다음 발사 시각까지 대기 (드리프트 최소화)
        next += period;
        std::this_thread::sleep_until(next);
    }

    // 실험 타임업 → 종료 신호
    g_stop.store(true);
}
