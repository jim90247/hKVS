#include <getopt.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>

#include <thread>
#include <vector>

#include "clover/mitsume.h"
#include "clover/mitsume_clt_test.h"
#include "clover/mitsume_clt_thread.h"
#include "herd_main.h"
#include "libhrd/hrd.h"
#include "mica/mica.h"

DEFINE_int32(herd_server_ports, 1, "Number of server ports");
// Base port index of HERD: the i-th available IB port (start from 0)
DEFINE_int32(herd_base_port_index, 0, "HERD base port index");
DEFINE_int32(postlist, 1,
             "Post list size (max # of requests in ibv_post_send)");

DEFINE_int32(clover_machine_id, 1, "Clover's machine id");
DEFINE_int32(clover_ib_dev, 0, "Infiniband device id (start from 0)");
// Base port index of Clover: the port id of clover_ib_dev
DEFINE_int32(clover_ib_port, 1, "Clover's Infiniband port id (start from 1)");
DEFINE_int32(clover_cn, 1, "Number of Clover compute nodes");
DEFINE_int32(clover_dn, 1, "Number of Clover data nodes");
DEFINE_int32(clover_loopback, 2, "Number of loopbacks (?)");
DEFINE_string(clover_memcached_ip, "192.168.223.1", "Memcached IP");

ib_inf *node_share_inf;

int MITSUME_CLT_NUM;
int MITSUME_MEM_NUM;

/**
 * @brief Initialize Clover compute node context.
 *
 * @param input_arg Clover compute node parameters
 * @return the initialized Clover client context
 */
mitsume_ctx_clt *InitCloverCnContext(struct configuration_params *input_arg) {
  // MITSUME_CLT_CONSUMER_NUMBER is the maximum number of threads for Clover
  // compute node.
  static_assert(NUM_WORKERS <= MITSUME_CLT_CONSUMER_NUMBER);
  int i;
  mitsume_ctx_clt *client_ctx = new mitsume_ctx_clt;

  mitsume_con_alloc_share_init();

  client_ctx->all_lh_attr = new ptr_attr[mitsume_con_alloc_get_total_lh()];
  client_ctx->ib_ctx = node_share_inf;
  client_ctx->client_id = get_client_id(input_arg);
  client_ctx->node_id = input_arg->machine_id;

  // thread_metadata is populated in this function
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

/**
 * @brief Setup Clover compute node and KVS.
 *
 * This function should only be called once.
 *
 * @param params configuration parameters for Clover compute node.
 * @return the initialized Clover client context
 */
mitsume_ctx_clt *SetupClover(configuration_params *params) {
  node_share_inf = ib_complete_setup(params, CLIENT, "clover client");
  RAW_CHECK(node_share_inf != nullptr, "ib_complete_setup failed");

  mitsume_ctx_clt *client_ctx;
  client_ctx = InitCloverCnContext(params);

  RAW_CHECK(client_setup_post_recv(params, client_ctx) == 0,
            "Failed to setup post_recv");
  RAW_CHECK(client_get_shortcut(client_ctx) == 0,
            "Failed to get correct shortcut");

  mitsume_con_alloc_get_lh(nullptr, client_ctx);
  mitsume_stat_init(MITSUME_ROLE::MITSUME_IS_CLIENT);

  mitsume_tool_lru_init();
  mitsume_tool_gc_init(client_ctx);

  RAW_LOG(INFO, "Finish Clover client setup");
  return client_ctx;
}

void WorkerMain(herd_thread_params herd_params, mitsume_ctx_clt *clover_ctx) {
  int i, ret;
  int wrkr_lid = herd_params.id; /* Local ID of this worker thread*/
  int num_server_ports = herd_params.num_server_ports;
  int base_port_index = herd_params.base_port_index;
  int postlist = herd_params.postlist;

  /*
   * MICA-related checks. Note that @postlist is the largest batch size we
   * feed into MICA. The average postlist per port in a dual-port NIC should
   * be @postlist / 2.
   */
  assert(MICA_MAX_BATCH_SIZE >= postlist);
  static_assert(HERD_VALUE_SIZE <= MICA_MAX_VALUE);

  assert(UNSIG_BATCH >= postlist); /* Postlist check */
  assert(postlist <= NUM_CLIENTS); /* Static sizing of arrays below */

  /* MICA instance id = wrkr_lid, NUMA node = 0 */
  mica_kv kv;
  mica_init(&kv, wrkr_lid, 0, HERD_NUM_BKTS, HERD_LOG_CAP);
  mica_populate_fixed_len(&kv, HERD_NUM_KEYS, HERD_VALUE_SIZE);

  assert(num_server_ports < MAX_SERVER_PORTS); /* Avoid dynamic alloc */
  hrd_ctrl_blk *cb[MAX_SERVER_PORTS];

  // Create queue pairs for SEND responses for each server ports
  for (i = 0; i < num_server_ports; i++) {
    int ib_port_index = base_port_index + i;

    cb[i] = hrd_ctrl_blk_init(
        wrkr_lid,              /* local_hid */
        ib_port_index, -1,     /* port index, numa node */
        0, 0,                  /* #conn qps, uc */
        nullptr, 0, -1,        /*prealloc conn buf, buf size, key */
        NUM_UD_QPS, 4096, -1); /* num_dgram_qps, dgram_buf_size, key */
  }

  /* Map the request region created by the master */
  volatile mica_op *req_buf;
  int sid = shmget(MASTER_SHM_KEY, RR_SIZE, SHM_HUGETLB | 0666);
  assert(sid != -1);
  req_buf = static_cast<volatile mica_op *>(shmat(sid, 0, 0));
  assert(req_buf != (void *)-1);

  /* Create an address handle for each client */
  ibv_ah *ah[NUM_CLIENTS];
  memset(ah, 0, NUM_CLIENTS * sizeof(uintptr_t));
  hrd_qp_attr *clt_qp[NUM_CLIENTS];

  for (i = 0; i < NUM_CLIENTS; i++) {
    /* Compute the control block and physical port index for client @i */
    int cb_i = i % num_server_ports;
    int local_port_i = base_port_index + cb_i;

    char clt_name[HRD_QP_NAME_SIZE];
    sprintf(clt_name, "client-dgram-%d", i);

    /* Get the UD queue pair for the ith client */
    clt_qp[i] = nullptr;
    while (clt_qp[i] == nullptr) {
      clt_qp[i] = hrd_get_published_qp(clt_name);
      if (clt_qp[i] == nullptr) {
        usleep(200000);
      }
    }

    printf("main: Worker %d found client %d of %d clients. Client LID: %d\n",
           wrkr_lid, i, NUM_CLIENTS, clt_qp[i]->lid);

    ibv_ah_attr ah_attr = {
        .dlid = clt_qp[i]->lid,
        .sl = 0,
        .src_path_bits = 0,
        .is_global = 0,
        /* port_num (> 1): device-local port for responses to this client */
        .port_num = local_port_i + 1,
    };

    ah[i] = ibv_create_ah(cb[cb_i]->pd, &ah_attr);
    assert(ah[i] != nullptr);
  }

  RAW_LOG(INFO, "Finish HERD worker setup");

  int ws[NUM_CLIENTS] = {0}; /* Per-client window slot */

  /* We can detect at most NUM_CLIENTS requests in each step */
  mica_op *op_ptr_arr[NUM_CLIENTS];
  mica_resp resp_arr[NUM_CLIENTS];
  ibv_send_wr wr[NUM_CLIENTS], *bad_send_wr = nullptr;
  ibv_sge sgl[NUM_CLIENTS];

  /* If postlist is disabled, remember the cb to send() each @wr from */
  int cb_for_wr[NUM_CLIENTS];

  /* If postlist is enabled, we instead create per-cb linked lists of wr's */
  ibv_send_wr *first_send_wr[MAX_SERVER_PORTS] = {nullptr};
  ibv_send_wr *last_send_wr[MAX_SERVER_PORTS] = {nullptr};
  ibv_wc wc;
  long long rolling_iter = 0; /* For throughput measurement */
  long long nb_tx[MAX_SERVER_PORTS][NUM_UD_QPS] = {{0}}; /* CQE polling */
  int ud_qp_i = 0; /* UD QP index: same for both ports */
  long long nb_tx_tot = 0;
  long long nb_post_send = 0;

  /*
   * @cb_i is the control block to use for @clt_i's response. If NUM_CLIENTS
   * is a multiple of @num_server_ports, we can cycle @cb_i in the client loop
   * to maintain cb_i = clt_i % num_server_ports.
   *
   * @wr_i is the work request to use for @clt_i's response. We use contiguous
   * work requests for the responses in a batch. This is because (in HERD) we
   * need to  pass a contiguous array of operations to MICA, and marshalling
   * and unmarshalling the contiguous array will be expensive.
   */
  // Index of control block. There is one control block for each server port. It
  // means that cb_i is always zero if there's only one server port.
  int cb_i = -1;
  // index of client
  int clt_i = -1;
  int poll_i, wr_i;
  assert(NUM_CLIENTS % num_server_ports == 0);

  timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  while (1) {
    if (unlikely(rolling_iter >= M_4)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds = (end.tv_sec - start.tv_sec) +
                       (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      printf(
          "main: Worker %d: %.2f IOPS. Avg per-port postlist = %.2f. "
          "HERD lookup fail rate = %.4f\n",
          wrkr_lid, M_4 / seconds, (double)nb_tx_tot / nb_post_send,
          (double)kv.num_get_fail / kv.num_get_op);

      rolling_iter = 0;
      nb_tx_tot = 0;
      nb_post_send = 0;

      clock_gettime(CLOCK_REALTIME, &start);
    }

    /* Do a pass over requests from all clients */
    int nb_new_req[MAX_SERVER_PORTS] = {0}; /* New requests from port i*/
    wr_i = 0;

    /*
     * Request region prefetching needs to be done w/o messing up @clt_i,
     * so the loop below is wrong.
     * Recomputing @req_offset in the loop below is as expensive as storing
     * results in an extra array.
     */
    /*for(clt_i = 0; clt_i < NUM_CLIENTS; clt_i++) {
            int req_offset = OFFSET(wrkr_lid, clt_i, ws[clt_i]);
            __builtin_prefetch((void *) &req_buf[req_offset], 0, 2);
    }*/

    for (poll_i = 0; poll_i < NUM_CLIENTS; poll_i++) {
      /*
       * This cycling of @clt_i and @cb_i needs to be before polling. This
       * should be the only place where we modify @clt_i and @cb_i.
       */
      HRD_MOD_ADD(clt_i, NUM_CLIENTS);
      HRD_MOD_ADD(cb_i, num_server_ports);
      // assert(cb_i == clt_i % num_server_ports);	/* XXX */

      int req_offset = Offset(wrkr_lid, clt_i, ws[clt_i]);
      if (req_buf[req_offset].opcode < HERD_OP_GET) {
        continue;
      }

      /* Convert to a MICA opcode */
      req_buf[req_offset].opcode -= HERD_MICA_OFFSET;
      // assert(req_buf[req_offset].opcode == MICA_OP_GET ||	/* XXX */
      //		req_buf[req_offset].opcode == MICA_OP_PUT);

      op_ptr_arr[wr_i] = const_cast<mica_op *>(&req_buf[req_offset]);

      if (USE_POSTLIST == 1) {
        /* Add the SEND response for this client to the postlist */
        if (nb_new_req[cb_i] == 0) {
          first_send_wr[cb_i] = &wr[wr_i];
          last_send_wr[cb_i] = &wr[wr_i];
        } else {
          last_send_wr[cb_i]->next = &wr[wr_i];
          last_send_wr[cb_i] = &wr[wr_i];
        }
      } else {
        wr[wr_i].next = nullptr;
        cb_for_wr[wr_i] = cb_i;
      }

      /* Fill in the work request (except the scatter gather elements, they will
       * be filled in next for loop, after mica_batch_op(). */
      wr[wr_i].wr.ud.ah = ah[clt_i];
      wr[wr_i].wr.ud.remote_qpn = clt_qp[clt_i]->qpn;
      wr[wr_i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;

      wr[wr_i].opcode = IBV_WR_SEND_WITH_IMM;
      wr[wr_i].num_sge = 1;
      wr[wr_i].sg_list = &sgl[wr_i];
      wr[wr_i].imm_data = wrkr_lid;

      wr[wr_i].send_flags =
          ((nb_tx[cb_i][ud_qp_i] & UNSIG_BATCH_) == 0) ? IBV_SEND_SIGNALED : 0;
      if ((nb_tx[cb_i][ud_qp_i] & UNSIG_BATCH_) == UNSIG_BATCH_) {
        hrd_poll_cq(cb[cb_i]->dgram_send_cq[ud_qp_i], 1, &wc);
      }
      wr[wr_i].send_flags |= IBV_SEND_INLINE;

      HRD_MOD_ADD(ws[clt_i], WINDOW_SIZE);

      rolling_iter++;
      nb_tx[cb_i][ud_qp_i]++; /* Must increment inside loop */
      nb_tx_tot++;
      nb_new_req[cb_i]++;
      wr_i++;

      if (wr_i == postlist) {
        break;
      }
    }

    mica_batch_op(&kv, wr_i, op_ptr_arr, resp_arr);

    // We may insert mirroring/invalidation operations (Clover compute node)
    // here.

    /*
     * Fill in the computed @val_ptr's. For non-postlist mode, this loop
     * must go from 0 to (@wr_i - 1) to follow the signaling logic.
     */
    int nb_new_req_tot = wr_i;
    for (wr_i = 0; wr_i < nb_new_req_tot; wr_i++) {
      sgl[wr_i].length = resp_arr[wr_i].val_len;
      sgl[wr_i].addr = reinterpret_cast<uintptr_t>(resp_arr[wr_i].val_ptr);

      if (USE_POSTLIST == 0) {
        ret = ibv_post_send(cb[cb_for_wr[wr_i]]->dgram_qp[ud_qp_i], &wr[wr_i],
                            &bad_send_wr);
        CPE(ret, "ibv_post_send error", ret);
        nb_post_send++;
      }
    }

    for (i = 0; i < num_server_ports; i++) {
      if (nb_new_req[i] == 0) {
        continue;
      }

      /* If postlist is off, we should post replies in the loop above */
      if (USE_POSTLIST == 1) {
        last_send_wr[i]->next = nullptr;
        ret = ibv_post_send(cb[i]->dgram_qp[ud_qp_i], first_send_wr[i],
                            &bad_send_wr);
        CPE(ret, "ibv_post_send error", ret);
        nb_post_send++;
      }
    }

    /* Use a different UD QP for the next postlist */
    HRD_MOD_ADD(ud_qp_i, NUM_UD_QPS);
  }
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_clover_memcached_ip.copy(MEMCACHED_IP, sizeof(MEMCACHED_IP));
  MITSUME_CLT_NUM = FLAGS_clover_cn;
  MITSUME_MEM_NUM = FLAGS_clover_dn;

  /* Use small queues to reduce cache pressure */
  static_assert(HRD_Q_DEPTH == 128);

  /* All requests should fit into the master's request region */
  static_assert(sizeof(struct mica_op) * NUM_CLIENTS * NUM_WORKERS *
                    WINDOW_SIZE <
                RR_SIZE);

  /* Unsignaled completion checks. worker.c does its own check w/ @postlist */
  static_assert(UNSIG_BATCH >= WINDOW_SIZE); /* Pipelining check for clients */
  static_assert(HRD_Q_DEPTH >= 2 * UNSIG_BATCH); /* Queue capacity check */

  assert(FLAGS_base_port_index >= 0 && FLAGS_base_port_index <= 8);
  assert(FLAGS_herd_server_ports >= 1 && FLAGS_herd_server_ports <= 8);
  assert(FLAGS_postlist >= 1);

  /* Launch a single server thread or multiple client threads */
  printf("worker: Using %d threads\n", NUM_WORKERS);

  // Setup Clover compute node
  configuration_params clover_param = {
      .global_thread_id = (FLAGS_clover_machine_id << P15_ID_SHIFT) + 0,
      .local_thread_id = 0,
      .base_port_index = FLAGS_clover_ib_port,
      .num_servers = MITSUME_CON_NUM,
      .num_clients = FLAGS_clover_cn,
      .num_memorys = FLAGS_clover_dn,
      .machine_id = FLAGS_clover_machine_id,
      // one clover CN thread for each HERD worker
      .total_threads = NUM_WORKERS,
      .device_id = FLAGS_clover_ib_dev,
      .num_loopback = FLAGS_clover_loopback};
  mitsume_ctx_clt *clover_ctx = SetupClover(&clover_param);

  std::vector<std::thread> threads;
  for (int i = 0; i < NUM_WORKERS; i++) {
    herd_thread_params param = {
        .id = i,
        .base_port_index = FLAGS_herd_base_port_index,
        .num_server_ports = FLAGS_herd_server_ports,
        .num_client_ports = -1,   // does not matter for worker
        .update_percentage = -1,  // does not matter for worker
        .postlist = FLAGS_postlist};
    auto t = std::thread(WorkerMain, param, clover_ctx);
    threads.emplace_back(std::move(t));
  }

  for (auto &t : threads) {
    t.join();
  }
  return 0;
}
