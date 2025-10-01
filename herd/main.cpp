#include <iostream>
#include <thread>
#include <atomic>
#include <getopt.h>

extern "C" {
#include "hrd.h"
#include "main.h"
#include "mica.h"
}
#include "scheduler_defs.h"
struct mica_kv kv_instances[MAX_CORES]; // KV index to save actual KV
Route route_tbl[MAX_CORES];              // Shard where client write thier request by RDMA Write
std::atomic<bool> g_stop{false};          // for test

// 코루틴 스케줄러 스레드 함수 프로토타입
void* run_worker(void* arg);
void timed_producer(int num_thread, int qps, int durationSec); // (참고용) Request 생성자



int main(int argc, char* argv[]) {
  int i, c;
  int is_master = -1;
  int num_threads = -1;
  int is_client = -1, machine_id = -1, postlist = -1, update_percentage = -1;
  int base_port_index = -1, num_server_ports = -1, num_client_ports = -1;
  struct thread_params* param_arr;
  pthread_t* thread_arr;

  static struct option opts[] = {
    {"master", required_argument, nullptr, 'M'},
    {"num-threads", required_argument, nullptr, 't'},
    {"base-port-index", required_argument, nullptr, 'b'},
    {"num-server-ports", required_argument, nullptr, 'N'},
    {"num-client-ports", required_argument, nullptr, 'n'},
    {"is-client", required_argument, nullptr, 'c'},
    {"update-percentage", required_argument, nullptr, 'u'},
    {"machine-id", required_argument, nullptr, 'm'},
    {"postlist", required_argument, nullptr, 'p'},
    {nullptr, 0, nullptr, 0} // 배열의 끝을 의미하는 NULL 초기화
  };
  /* Parse and check arguments */
  while (1) {
    c = getopt_long(argc, argv, "M:t:b:N:n:c:u:m:p", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'M':
        is_master = atoi(optarg);
        assert(is_master == 1);
        break;
      case 't':
        num_threads = atoi(optarg);
        break;
      case 'b':
        base_port_index = atoi(optarg);
        break;
      case 'N':
        num_server_ports = atoi(optarg);
        break;
      case 'n':
        num_client_ports = atoi(optarg);
        break;
      case 'c':
        is_client = atoi(optarg);
        break;
      case 'u':
        update_percentage = atoi(optarg);
        break;
      case 'm':
        machine_id = atoi(optarg);
        break;
      case 'p':
        postlist = atoi(optarg);
        break;
      default:
        printf("Invalid argument %d\n", c);
        assert(false);
    }
  }

  /* Common checks for all (master, workers, clients */
  assert(base_port_index >= 0 && base_port_index <= 8);
  assert(num_server_ports >= 1 && num_server_ports <= 8);

  /* Handle the master process specially */
  if (is_master == 1) {
    struct thread_params master_params;
    master_params.num_server_ports = num_server_ports;
    master_params.base_port_index = base_port_index;

    pthread_t master_thread;
    pthread_create(&master_thread, NULL, run_master, (void*)&master_params);
    pthread_join(master_thread, NULL);
    exit(0);
  }

  /* Common sanity checks for worker process and per-machine client process */
  assert(is_client == 0 || is_client == 1);

  if (is_client == 1) {
    assert(num_client_ports >= 1 && num_client_ports <= 8);
    assert(num_threads >= 1);
    assert(machine_id >= 0);
    assert(update_percentage >= 0 && update_percentage <= 100);
    assert(postlist == -1); /* Client postlist = MAX_CORES */
  } else {//is_client == 0
    num_threads = MAX_CORES; /* Needed to allocate thread structs later */
    for (int i = 0; i < MAX_CORES; i++) {
      // MICA 인스턴스 초기화
      mica_init(&kv_instances[i], i, 0, HERD_NUM_BKTS, HERD_LOG_CAP);
      mica_populate_fixed_len(&kv_instances[i], HERD_NUM_KEYS, HERD_VALUE_SIZE);

      // 라우팅 테이블(샤드 소유권) 초기화
      // 처음에는 i번 샤드를 i번 워커(스레드)가 담당합니다.
      route_tbl[i].owner.store(i, std::memory_order_relaxed);
    }
  }

  /* Launch a single server thread or multiple client threads */
  printf("main: Using %d threads\n", num_threads);
  param_arr = new thread_params[num_threads];
  thread_arr = new pthread_t[num_threads];

  for (i = 0; i < num_threads; i++) {
    param_arr[i].postlist = postlist;

    if (is_client) {
      param_arr[i].id = (machine_id * num_threads) + i;
      param_arr[i].base_port_index = base_port_index;
      param_arr[i].num_server_ports = num_server_ports;
      param_arr[i].num_client_ports = num_client_ports;
      param_arr[i].update_percentage = update_percentage;

      pthread_create(&thread_arr[i], NULL, run_client, &param_arr[i]);
    } else { // is_client ==0
      param_arr[i].id = i;
      param_arr[i].base_port_index = base_port_index;
      param_arr[i].num_server_ports = num_server_ports;
      param_arr[i].num_client_ports = num_client_ports;
      pthread_create(&thread_arr[i], NULL, run_worker, &param_arr[i]);
    }
  }

  for (i = 0; i < num_threads; i++) {
    pthread_join(thread_arr[i], NULL);
  }

  return 0;
}
