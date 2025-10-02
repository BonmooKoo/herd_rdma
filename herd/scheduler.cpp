//Config
#include "main.h"
#include "scheduler_defs.h"

constexpr int MAX_Q = 64;   // Max Queue length
constexpr double Q_A = 0.2; // Queue Down threshold-> Core consolidation
constexpr double Q_B = 0.9; // Queue Up threshold -> Load Balancing

constexpr int SLO_THRESHOLD_MS = 5;
constexpr int SCHEDULING_TICK = 16;
constexpr int NUM_SHARDS = MAX_CORES; // 최대 worker 갯수 = 전체 Shard 갯수
extern Route route_tbl[NUM_SHARDS];  // shard -> current owner tid

// 실험 종료를 알리는 전역변수
extern std::atomic<bool> g_stop;
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

Scheduler *schedulers[MAX_CORES] = {nullptr};
std::condition_variable cvs[MAX_CORES];
std::mutex cv_mutexes[MAX_CORES];
std::atomic<bool> sleeping_flags[MAX_CORES];
std::atomic<CoreState> core_state[MAX_CORES];

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

};

// =====================
// Task & Scheduler
// =====================



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
    for (int i = 0; i < MAX_CORES; ++i)
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
    // printf("[%d:%d]post_coroutineto<%d:%d:%d>\n", from_tid, from_sched.work_queue.size(), to_tid, to_sched.work_queue.size(), to_sched.wait_list.size());
    printf("[%d:%zu]post_coroutineto<%d:%zu:%zu>\n", from_tid, from_sched.work_queue.size(), to_tid, to_sched.work_queue.size(), to_sched.wait_list.size());

    return count;
}

// global rx_queue → thread_local rx_queue
// static inline void pump_external_requests_into(Scheduler &sched, int burst = 32)
// {
//     Request r;
//     int cnt = 0;
//     while (cnt < burst && g_rx.try_pop(r))
//     {
//         sched.rx_queue.push(std::move(r));
//         cnt++;
//     }
//     // printf("[%d]Pulled%d\n",sched.thread_id,cnt);
// }
// Herd의 요청을 폴링하여 스케줄러 큐에 넣는 함수
static inline void poll_owned_shards(Scheduler &sched, int my_tid, volatile struct mica_op* req_buf) {
    static int ws[NUM_CLIENTS] = {0};
    const int BURST_SIZE = 16;
    for (int shard_id = 0; shard_id < NUM_SHARDS; ++shard_id) {
        if (route_tbl[shard_id].owner.load(std::memory_order_acquire) == my_tid) {
            int clt_i = shard_id;
            for (int i = 0; i < BURST_SIZE; ++i) {
                int req_offset = OFFSET(shard_id, clt_i, ws[clt_i]);
                if (req_buf[req_offset].opcode >= HERD_OP_GET) {
                    // 요청이 있으면 큐에 추가
                    Request r;
                    r.type = req_buf[req_offset].opcode;
                    r.key = req_buf[req_offset].key.bkt;
                    r.req_buf_offset = req_offset;
                    r.client_id = clt_i;
                    r.start_time = now_ns();

                    sched.rx_queue.push(std::move(r));
                    HRD_MOD_ADD(ws[clt_i], WINDOW_SIZE); // 다음 슬롯으로 이동
                } else {
                    break;
                }
            }
        }
    }
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
    std::array<int, MAX_CORES> cand{};
    int n = 0;
    for (int c = 0; c < MAX_CORES; ++c)
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
    // 4)move shard ownership
    for (int s = 0; s < NUM_SHARDS; ++s) {
        if (route_tbl[s].owner.load() == tid) {
            route_tbl[s].owner.store(target, std::memory_order_release);
        }
    }
    std::vector<int> my_shards;
    for (int s = 0; s < NUM_SHARDS; ++s) {
        if (route_tbl[s].owner.load() == tid) {
            my_shards.push_back(s);
        }
    }
    assert(my_shards.size()!=1);
    int shards_to_move = my_shards.size();
    for (int i = 0; i < shards_to_move; ++i) {
        route_tbl[my_shards[i]].owner.store(target, std::memory_order_release);
    }

    core_state[target] = CONSOLIDATED;
    return target;
}

// 자는 스레드를 깨울 때 코루틴 일부 전달
int load_balancing(int from_tid, int to_tid)
{
    Scheduler *to_sched = schedulers[to_tid];
    Scheduler *from_sched = schedulers[from_tid];
    // printf("[%d>>%d]TryLoadBalancing<%d>\n", from_tid, to_tid, from_sched->work_queue.size());
    printf("[%d>>%d]TryLoadBalancing<%zu>\n", from_tid, to_tid, from_sched->work_queue.size());
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
    std::vector<int> my_shards;
    for (int s = 0; s < NUM_SHARDS; ++s) {
        if (route_tbl[s].owner.load() == from_tid) {
            my_shards.push_back(s);
        }
    }
    assert(my_shards.size()!=1);
    int shards_to_move = my_shards.size() / 2;
    for (int i = 0; i < shards_to_move; ++i) {
        route_tbl[my_shards[i]].owner.store(to_tid, std::memory_order_release);
    }

    return half;
}

// ===============
// Request 처리부
// ===============
// request handler func

// 워커 코루틴: 깨어날 때마다 rx_queue에서 Request를 소비
// yield_type은 void에서 Scheduler*로 변경
// call_type은 Scheduler*를 받도록 변경

void herd_worker_coroutine(Scheduler &sched, int lwid, int coroid,
                           struct mica_kv* kv_ptr, volatile struct mica_op* req_buf,
                           int num_server_ports, struct hrd_ctrl_blk** cb,
                           struct ibv_ah** ah, struct hrd_qp_attr** clt_qp) {
    auto* source = new CoroCall([=](CoroYield &yield) {
        Scheduler *current = yield.get();
        Request r;
        struct ibv_send_wr wr, *bad_send_wr = NULL;
        struct ibv_sge sgl;
        struct mica_op* op_ptr_arr[1];
        struct mica_resp resp_arr[1];

        while (true) {
            if (!current->rx_queue.try_pop(r)) {
                yield();
                current = yield.get();
                continue;
            }

            volatile struct mica_op* req = &req_buf[r.req_buf_offset];
            req->opcode -= HERD_MICA_OFFSET;
            op_ptr_arr[0] = (struct mica_op*)req;

            mica_batch_op(kv_ptr, 1, op_ptr_arr, resp_arr);

            int clt_i = r.client_id;
            int cb_i = clt_i % num_server_ports;
            int ud_qp_i = 0;

            sgl.length = resp_arr[0].val_len;
            sgl.addr = (uint64_t)(uintptr_t)resp_arr[0].val_ptr;
            
            wr = {}; // Zero out the struct
            wr.wr.ud.ah = ah[clt_i];
            wr.wr.ud.remote_qpn = clt_qp[clt_i]->qpn;
            wr.wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
            wr.opcode = IBV_WR_SEND_WITH_IMM;
            wr.imm_data = lwid; // ★★ 논리적 워커 ID 사용 ★★
            wr.num_sge = 1;
            wr.sg_list = &sgl;
            wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
            
            ibv_post_send(cb[cb_i]->dgram_qp[ud_qp_i], &wr, &bad_send_wr);
            current->record_latency(r.start_time);
        }
    });

    Task task(source, lwid, coroid);
    sched.emplace(std::move(task));
}

void herd_master_loop(Scheduler &sched, int tid, volatile struct mica_op* req_buf) {
    if (sleeping_flags[tid]) {
        core_state[tid] = SLEEPING;
        sleep_thread(tid);
        core_state[tid] = STARTED;
    }

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
                        while (sched.rx_queue.size() > 0)
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
                        while (sched.work_queue.empty()  || sched.rx_queue.size() > 0)
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
                else if (!g_stop.load() && sched.detect_SLO_violation_slice()&&sched.shard_count!=1)
                {
                    // 3-2-0) 내가 shard 1개만 가지고 있으면 load balancing 못함;;
                    // printf("[%d]DetectSLOviolation\n",tid);
                    // 3-2-1) first, set my state to CONSOLIDATED to prevent consolidation
                    if (state_active_to_consol(tid))
                    {
                        // 3-2-2) try load balancing
                        int target = -1;
                        for (int i = 0; i < MAX_CORES; i++)
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
            poll_owned_shards(sched, tid, req_buf);
        } // end if (++sched_count >= SCHEDULING_TICK)
    } // end while (!g_stop.load())

    // drain
    poll_owned_shards(sched, tid, req_buf);
    while (sched.work_queue.empty()  || sched.rx_queue.size() > 0)
    {
        sched.schedule();
    }
    printf("[%d]Master ended\n",sched.thread_id);

}