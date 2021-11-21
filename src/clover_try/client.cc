#include <getopt.h>
#include <glog/logging.h>

#include "clover/memcached.h"
#include "clover/mitsume.h"
#include "clover/mitsume_clt_test.h"
#include "clover/mitsume_clt_thread.h"
#include "clover/mitsume_clt_tool.h"
const char *DBG_STRING = "client";
#define P15_ROLE CLIENT

int MITSUME_CLT_NUM;
int MITSUME_MEM_NUM;

struct configuration_params *param_arr;
pthread_t *thread_arr;
pthread_barrier_t local_barrier;
struct ib_inf *node_share_inf;

struct mitsume_ctx_clt *client_init(struct configuration_params *input_arg) {
  int i;
  struct mitsume_ctx_clt *client_ctx = new struct mitsume_ctx_clt;

  mitsume_con_alloc_share_init();

  client_ctx->all_lh_attr = new ptr_attr[mitsume_con_alloc_get_total_lh()];
  client_ctx->ib_ctx = node_share_inf;
  client_ctx->client_id = get_client_id(input_arg);
  client_ctx->node_id = input_arg->machine_id;

  mitsume_clt_thread_metadata_setup(input_arg, client_ctx);
  i = 0;
  {
    MITSUME_PRINT("%llx %llx\n",
                  (unsigned long long int)client_ctx->thread_metadata[i]
                      .local_inf->user_input_space[0],
                  (unsigned long long int)client_ctx->thread_metadata[i]
                      .local_inf->user_input_mr[0]
                      ->lkey);
  }
  return client_ctx;
}

int client_get_shortcut(struct mitsume_ctx_clt *client_ctx) {
  /*ptr_attr *shortcut_attr = new ptr_attr[MITSUME_SHORTCUT_NUM];
  ptr_attr *tmp_attr;
  int per_allocation;
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  for(per_allocation=0;per_allocation<MITSUME_SHORTCUT_NUM;per_allocation++)
  {
      memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
      sprintf(memcached_string, MITSUME_MEMCACHED_SHORTCUT_STRING,
  per_allocation); tmp_attr = memcached_get_published_mr(memcached_string);
      memcpy(&shortcut_attr[per_allocation], tmp_attr, sizeof(ptr_attr));
      free(tmp_attr);
      if(per_allocation==0||per_allocation==1023)
          MITSUME_PRINT("%llx, %ld\n", (unsigned long
  long)shortcut_attr[per_allocation].addr,
  (long)shortcut_attr[per_allocation].rkey);
  }*/
  ptr_attr *shortcut_attr = new ptr_attr[MITSUME_SHORTCUT_NUM];
  ptr_attr *tmp_attr;
  int memory_id;
  int start_allocation, end_allocation;
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  uint32_t target_shortcut_entry_space;
  int current_index = 0;
  // target_shortcut_entry_space = MITSUME_ROUND_UP(MITSUME_SHORTCUT_NUM,
  // MITSUME_MEM_NUM);
  target_shortcut_entry_space = MITSUME_SHORTCUT_NUM / MITSUME_MEM_NUM;
  for (memory_id = 0; memory_id < MITSUME_MEM_NUM; memory_id++) {
    start_allocation = memory_id * target_shortcut_entry_space;
    end_allocation = (memory_id + 1) * target_shortcut_entry_space - 1;
    memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
    sprintf(memcached_string, MITSUME_MEMCACHED_SHORTCUT_STRING, memory_id);
    tmp_attr = (ptr_attr *)memcached_get_published_size(
        memcached_string,
        sizeof(ptr_attr) * (end_allocation - start_allocation + 1));
    memcpy(&shortcut_attr[current_index], tmp_attr,
           sizeof(ptr_attr) * (end_allocation - start_allocation + 1));
    current_index += end_allocation - start_allocation + 1;
    free(tmp_attr);
  }
  client_ctx->all_shortcut_attr = shortcut_attr;
  MITSUME_PRINT("finish getting shortcut\n");
  return MITSUME_SUCCESS;
}

int client_setup_post_recv(struct configuration_params *input_arg,
                           struct mitsume_ctx_clt *context) {
  void *alloc_space;
  ptr_attr *tmp_attr_ptr;
  int per_msg;
  int per_qp;
  uint32_t alloc_size = MITSUME_MAX_MESSAGE_SIZE;
  context->per_qp_mr_attr_list =
      new ptr_attr *[node_share_inf->num_local_rcqps];
  context->per_post_recv_mr_list =
      new struct ibv_mr *[node_share_inf->num_local_rcqps];

  // register a memory space for each qp
  for (per_qp = 0; per_qp < node_share_inf->num_local_rcqps; per_qp++) {
    context->per_qp_mr_attr_list[per_qp] =
        new ptr_attr[MITSUME_CON_MESSAGE_PER_POST];
    alloc_space = mitsume_malloc(alloc_size * MITSUME_CON_MESSAGE_PER_POST);
    context->per_post_recv_mr_list[per_qp] = ibv_reg_mr(
        node_share_inf->pd, alloc_space,
        alloc_size * MITSUME_CON_MESSAGE_PER_POST, MITSUME_MR_PERMISSION);
    tmp_attr_ptr = context->per_qp_mr_attr_list[per_qp];
    for (per_msg = 0; per_msg < MITSUME_CON_MESSAGE_PER_POST; per_msg++) {
      tmp_attr_ptr[per_msg].addr =
          (uint64_t)context->per_post_recv_mr_list[per_qp]->addr +
          (uint64_t)alloc_size * per_msg;
      tmp_attr_ptr[per_msg].rkey = context->per_post_recv_mr_list[per_qp]->rkey;
    }
  }

  // post all memory space into qp
  for (per_qp = 0; per_qp < node_share_inf->num_local_rcqps; per_qp++) {
    ib_post_recv_inf *input_inf =
        new ib_post_recv_inf[MITSUME_CON_MESSAGE_PER_POST];
    for (per_msg = 0; per_msg < MITSUME_CON_MESSAGE_PER_POST; per_msg++) {
      input_inf[per_msg].qp_index = per_qp;
      input_inf[per_msg].length = alloc_size;
      input_inf[per_msg].mr_index = per_msg;
    }
    ib_post_recv_connect_qp(node_share_inf, input_inf,
                            context->per_qp_mr_attr_list[per_qp],
                            MITSUME_CON_MESSAGE_PER_POST);
    free(input_inf);
  }

  return MITSUME_SUCCESS;
}

void *main_client(void *arg) {
  // int machine_id, thread_id;

  struct configuration_params *input_arg = (struct configuration_params *)arg;
  node_share_inf = ib_complete_setup(input_arg, P15_ROLE, DBG_STRING);
  assert(node_share_inf != NULL);

  struct mitsume_ctx_clt *client_ctx;
  client_ctx = client_init(input_arg);

  CPE(client_setup_post_recv(input_arg, client_ctx),
      "fail to setup post_recv\n", 0);
  CPE(client_get_shortcut(client_ctx), "fail to get correct shortcut\n", 0);

  mitsume_con_alloc_get_lh(NULL, client_ctx);
  mitsume_stat_init(MITSUME_IS_CLIENT);

  mitsume_tool_lru_init();
  mitsume_tool_gc_init(client_ctx);

  printf("finish all client setup\n");

  // Use thread-0 metadata
  mitsume_consumer_metadata *thread_metadata = &client_ctx->thread_metadata[0];
  // Following the usage in mitsume_benchmark_ycsb, where only the first 4K
  // bytes are used. Buffer is allocated at
  // mitsume_util.cc:mitsume_local_thread_setup() (line 209/215).
  const size_t kBufSize = 4096;
  // Buffer for reading, pre-allocated during context initialization.
  char *const rbuf =
      static_cast<char *>(thread_metadata->local_inf->user_output_space[0]);
  // Buffer for writing.
  char *const wbuf =
      static_cast<char *>(thread_metadata->local_inf->user_input_space[0]);
  fill(rbuf, rbuf + kBufSize, '\0');
  fill(wbuf, wbuf + kBufSize, '\0');

  string cmd, value;
  mitsume_key key = 123;
  int rc = 0;

  /**
   * @brief A stress test to find out the maximum of keys. Using default values,
   * this function halts at 335359 keys. This number is the amount of 128 byte
   * slots in slab allocator, which can be found in the terminal output of
   * metadata server during initialization.
   */
  auto max_keys_stress_test = [&]() {
    key = 0;
    while (rc == 0) {
      LOG_EVERY_N(INFO, 100000) << "opening key " << key;
      snprintf(wbuf, kBufSize, "key_%lu", key);
      rc =
          mitsume_tool_open(thread_metadata, key, wbuf, strnlen(wbuf, kBufSize),
                            MITSUME_BENCHMARK_REPLICATION);
      key++;
    }
  };
  // max_keys_stress_test();

  cout << "<open|read|write> <key> [value] $ ";
  while (cin >> cmd) {
    if (cmd == "open") {
      cin >> key >> value;
      strncpy(wbuf, value.c_str(), kBufSize);
      rc = mitsume_tool_open(thread_metadata, key, wbuf, value.length(),
                             MITSUME_BENCHMARK_REPLICATION);
    } else if (cmd == "read") {
      cin >> key;
      uint32_t read_size = 0;
      rc = mitsume_tool_read(thread_metadata, key, rbuf, &read_size,
                             MITSUME_TOOL_KVSTORE_READ);
      if (rc == MITSUME_SUCCESS) {
        value = string(rbuf);
      }
    } else if (cmd == "write") {
      cin >> key >> value;
      strncpy(wbuf, value.c_str(), kBufSize);
      rc = mitsume_tool_write(thread_metadata, key, wbuf, value.length(),
                              MITSUME_TOOL_KVSTORE_WRITE);
    }
    if (rc == MITSUME_SUCCESS) {
      cout << "Perform " << cmd << " on " << key << " success (value: " << value
           << ")" << endl;
    } else {
      cout << "Perform " << cmd << " on " << key
           << " failed, return code: " << rc << endl;
    }
    cout << "<open|read|write> <key> [value] $ ";
  }

  // mitsume_clt_test(client_ctx);
  /*
  char *test_write = new char[1024];
  char *test_read = new char[1024];
  struct ibv_mr *write_mr, *read_mr;
  uint64_t wr_id;


  wr_id = mitsume_local_thread_get_wr_id(local_inf);
  write_mr = ibv_reg_mr(node_share_inf->pd, test_write, 1024,
  MITSUME_MR_PERMISSION); read_mr = ibv_reg_mr(node_share_inf->pd, test_read,
  1024, MITSUME_MR_PERMISSION); userspace_one_read(node_share_inf, wr_id,
  read_mr, 64, &client_ctx->all_shortcut_attr[16], 0);
  userspace_one_poll(node_share_inf, wr_id, &client_ctx->all_shortcut_attr[16]);
  MITSUME_PRINT("%s\n", test_read);
  mitsume_local_thread_put_wr_id(local_inf, wr_id);

  memset(test_write, 0x41, 1024);
  wr_id = mitsume_local_thread_get_wr_id(local_inf);
  userspace_one_write(node_share_inf, wr_id, write_mr, 64,
  &client_ctx->all_shortcut_attr[16], 0); userspace_one_poll(node_share_inf,
  wr_id, &client_ctx->all_shortcut_attr[16]);
  mitsume_local_thread_put_wr_id(local_inf, wr_id);

  memset(test_write, 0x42, 1024);
  wr_id = mitsume_local_thread_get_wr_id(local_inf);
  userspace_one_write(node_share_inf, wr_id, write_mr, 64,
  &client_ctx->all_shortcut_attr[16], 16); userspace_one_poll(node_share_inf,
  wr_id, &client_ctx->all_shortcut_attr[16]);
  mitsume_local_thread_put_wr_id(local_inf, wr_id);

  wr_id = mitsume_local_thread_get_wr_id(local_inf);
  userspace_one_read(node_share_inf, wr_id, read_mr, 64,
  &client_ctx->all_shortcut_attr[16], 0); userspace_one_poll(node_share_inf,
  wr_id, &client_ctx->all_shortcut_attr[16]);
  mitsume_local_thread_put_wr_id(local_inf, wr_id);
  MITSUME_PRINT("%s\n", test_read);*/

  printf("ready to press ctrl+c to finish experiment\n");
  while (1)
    ;
}

void *run_client(void *arg) {
  int i;
  struct configuration_params *input_arg = (struct configuration_params *)arg;
  int machine_id = input_arg->machine_id;
  int num_threads = input_arg->total_threads;
  int num_servers = input_arg->num_servers;
  int num_clients = input_arg->num_clients;
  int num_memorys = input_arg->num_memorys;
  int base_port_index = input_arg->base_port_index;
  int ret;
  param_arr = (struct configuration_params *)malloc(
      num_threads * sizeof(struct configuration_params));
  thread_arr = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
  // initialize barrier
  ret = pthread_barrier_init(&local_barrier, NULL, input_arg->total_threads);
  if (ret)
    die_printf("[%s] fail to create barrier %d thread %d\n", __func__, ret,
               input_arg->total_threads);

  // initialize thread
  for (i = num_threads - 1; i >= 0; i--) {
    param_arr[i].global_thread_id = (machine_id << P15_ID_SHIFT) + i;
    param_arr[i].local_thread_id = i;
    param_arr[i].base_port_index = base_port_index;
    param_arr[i].num_servers = num_servers;
    param_arr[i].num_clients = num_clients;
    param_arr[i].num_memorys = num_memorys;
    param_arr[i].machine_id = machine_id;
    param_arr[i].total_threads = num_threads;
    param_arr[i].device_id = input_arg->device_id;
    param_arr[i].num_loopback = input_arg->num_loopback;
    if (i != 0)
      pthread_create(&thread_arr[i], NULL, main_client, &param_arr[i]);
    else
      main_client(&param_arr[0]);
  }
  return NULL;
}

// dummy code to test build
int main(int argc, char **argv) {
  int i, c;
  int is_master = -1;
  int num_threads = 1;
  int is_client = -1, machine_id = -1, is_server = -1, is_memory = -1;
  int base_port_index = -1;
  int num_clients, num_servers = MITSUME_CON_NUM, num_memorys;
  int device_id = 0;
  int num_loopback = -1;
  struct configuration_params *param_arr;
  pthread_t *thread_arr;

  static struct option opts[] = {
      {"base-port-index", 1, NULL, 'b'},     {"num-clients", 1, NULL, 'c'},
      {"num-servers", 1, NULL, 's'},         {"num-memorys", 1, NULL, 'm'},
      {"is-client", 1, NULL, 'C'},           {"is-server", 1, NULL, 'S'},
      {"is-memory", 1, NULL, 'M'},           {"machine-id", 1, NULL, 'I'},
      {"device-id", 1, NULL, 'd'},           {"num-loopbackset", 1, NULL, 'L'},
      {"memcached-server-ip", 1, NULL, 'X'}, {NULL, 0, NULL, 0}};

  /* Parse and check arguments */
  while (1) {
    c = getopt_long(argc, argv, "h:b:c:m:s:C:S:I:d:L:M:X:", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'h':
        is_master = atoi(optarg);
        assert(is_master == 1);
        break;
      case 'b':
        base_port_index = atoi(optarg);
        break;
      case 'c':
        num_clients = atoi(optarg);
        MITSUME_CLT_NUM = num_clients;
        break;
      case 's':
        num_servers = atoi(optarg);
        break;
      case 'm':
        num_memorys = atoi(optarg);
        MITSUME_MEM_NUM = num_memorys;
        break;
      case 'C':
        is_client = atoi(optarg);
        break;
      case 'S':
        is_server = atoi(optarg);
        break;
      case 'M':
        is_memory = atoi(optarg);
        break;
      case 'I':
        machine_id = atoi(optarg);
        break;
      case 'd':
        device_id = atoi(optarg);
        break;
      case 'L':
        num_loopback = atoi(optarg);
        break;
      case 'X':
        strncpy(MEMCACHED_IP, optarg, sizeof(MEMCACHED_IP));
        printf("%s:%s: memcached-server-ip = %s\n", __FILE__, __func__,
               MEMCACHED_IP);
        break;
      default:
        printf("Invalid argument %d\n", c);
        assert(0);
    }
  }

  /* Common checks for all (master, workers, clients */
  assert(base_port_index >= 0 && base_port_index <= 8);

  /* Common sanity checks for worker process and per-machine client process */
  assert((is_client + is_server + is_memory) == -1);
  assert((num_loopback) >= 0);

  printf("%s:%s: This is running as [%s]!\n", __FILE__, __func__,
         (is_client == 1)   ? "CN"
         : (is_memory == 1) ? "MN"
                            : "MS");
  printf("%s:%s: num_clients=%d, num_memorys=%d\n", __FILE__, __func__,
         num_clients, num_memorys);

  if (num_clients < 1 || num_servers < 1 || num_memorys < 1) {
    printf("%s:%s: Invalid num_clients=%d, num_memorys=%d num_servers=%d\n",
           __FILE__, __func__, num_clients, num_memorys, num_servers);
    exit(0);
  }

  if (machine_id < 0) {
    printf("Invalid machine_id %d\n", machine_id);
    exit(0);
  }

  if (is_client == 1) {
    assert(num_threads >= 1);
  }
  CHECK(num_threads == 1) << "Use only one thread.";

  param_arr = (struct configuration_params *)malloc(
      num_threads * sizeof(struct configuration_params));
  thread_arr = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
  assert(thread_arr);

  {
    param_arr[0].base_port_index = base_port_index;
    param_arr[0].num_servers = num_servers;
    param_arr[0].num_clients = num_clients;
    param_arr[0].num_memorys = num_memorys;
    param_arr[0].machine_id = machine_id;
    param_arr[0].total_threads = num_threads;
    param_arr[0].device_id = device_id;
    param_arr[0].num_loopback = num_loopback;

    run_client(&param_arr[0]);
  }

  while (1)
    ;

  for (i = 0; i < num_threads; i++) {
    pthread_join(thread_arr[i], NULL);
  }
  return 0;
}