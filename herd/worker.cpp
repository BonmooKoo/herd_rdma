#include "scheduler_defs.h"

extern "C" {
#include "hrd.h"
#include "main.h"
#include "mica.h"
}

#include "scheduler.cpp" // 스케줄러 구현 파일을 여기에 포함

// main.cpp에 선언된 전역 변수들을 extern으로 참조
extern struct mica_kv kv_instances[MAX_CORES];
extern Route route_tbl[MAX_CORES];
extern std::atomic<bool> g_stop;

void* run_worker(void* arg) {
  auto params = *static_cast<struct thread_params*>(arg);
  int wrkr_lid = params.id;

  struct hrd_ctrl_blk* cb[MAX_SERVER_PORTS];
  int num_server_ports = params.num_server_ports; // 서버가 사용할 RDMA 네트워크의 포트수  = 1
  int base_port_index = params.base_port_index;   // 서버가 사용할 RDMA 포트의 시작 번호 

  for (int i = 0; i < num_server_ports; i++) {
    int ib_port_index = base_port_index + i;

    cb[i] = hrd_ctrl_blk_init(wrkr_lid,          /* local_hid */
                              ib_port_index, -1, /* port index, numa node */
                              0, 0,              /* #conn qps, uc */
                              NULL, 0, -1, /*prealloc conn buf, buf size, key */
                              NUM_UD_QPS, 4096,
                              -1); /* num_dgram_qps, dgram_buf_size, key */
  }

  /* Map the request region created by the master */
  volatile struct mica_op* req_buf;
  int sid = shmget(MASTER_SHM_KEY, RR_SIZE, SHM_HUGETLB | 0666);
  assert(sid != -1);
  req_buf = static_cast<volatile struct mica_op*>(shmat(sid, 0, 0));
  assert(req_buf != (void*)-1);

  /* Create an address handle for each client */
  struct ibv_ah* ah[NUM_CLIENTS];
  memset(ah, 0, NUM_CLIENTS * sizeof(uintptr_t));
  struct hrd_qp_attr* clt_qp[NUM_CLIENTS];

  //RDMA : Client 쪽 thread와 연결설정
  for (int i = 0; i < NUM_CLIENTS; i++) {
    /* Compute the control block and physical port index for client @i */
    int cb_i = i % num_server_ports;
    int local_port_i = base_port_index + cb_i;

    char clt_name[HRD_QP_NAME_SIZE];
    sprintf(clt_name, "client-dgram-%d", i);

    /* Get the UD queue pair for the ith client */
    clt_qp[i] = NULL;
    while (clt_qp[i] == NULL) {
      clt_qp[i] = hrd_get_published_qp(clt_name);
      if (clt_qp[i] == NULL) {
        usleep(200000);
      }
    }

    printf("main: Worker %d found client %d of %d clients. Client LID: %d\n",
           wrkr_lid, i, NUM_CLIENTS, clt_qp[i]->lid);

    struct ibv_ah_attr ah_attr = {
        .is_global = 0,
        .dlid = clt_qp[i]->lid,
        .sl = 0,
        .src_path_bits = 0,
        /* port_num (> 1): device-local port for responses to this client */
        .port_num = local_port_i + 1,
    };

    ah[i] = ibv_create_ah(cb[cb_i]->pd, &ah_attr);
    assert(ah[i] != NULL);
  }

  // =================================================================
  // 2. 코루틴 스케줄러 초기화 및 실행
  // =================================================================

  bind_cpu(wrkr_lid);
  Scheduler sched(wrkr_lid);

  // 전역 MICA 배열에서 내 논리적 ID에 해당하는 인스턴스 포인터 가져오기
  struct mica_kv* my_kv = &kv_instances[wrkr_lid];

  // 초기 워커 코루틴 생성
  const int coro_count = MAX_CORES; // 스레드 당 worker코루틴 수
  for (int i = 0; i < coro_count; ++i) {
      herd_worker_coroutine(sched, wrkr_lid, wrkr_lid * coro_count + i,
                            my_kv, req_buf, params.num_server_ports,
                            cb, ah, clt_qp);
  }

  printf("Worker %d: Starting scheduler loop.\n", wrkr_lid);
  herd_master_loop(sched, wrkr_lid, req_buf);

  return NULL;
}
