#include <getopt.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <moodycamel/concurrentqueue.h>

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "clover_worker.h"
#include "clover_wrapper/cn.h"
#include "herd_main.h"
#include "libhrd/hrd.h"
#include "mica/mica.h"
#include "util/lru_records.h"

// The type of secondary KVS lookup table
using CloverLookupTable = std::unordered_set<mitsume_key>;

DEFINE_int32(herd_server_ports, 1, "Number of server ports");
// Base port index of HERD: the i-th available IB port (start from 0)
DEFINE_int32(herd_base_port_index, 0, "HERD base port index");
DEFINE_int32(postlist, 1,
             "Post list size (max # of requests in ibv_post_send)");

DEFINE_bool(clover_blocking, false,
            "Wait for Clover request complete before continuing");

DEFINE_uint64(lru_size, 8192, "Size of LRU record");
DEFINE_uint64(lru_window, 65536, "Size of LRU window");
DEFINE_int32(lru_min_count, 4,
             "Minimum amount of appearance to be placed in LRU");

/**
 * @brief Adds a new Clover request into request buffer and updates the request
 * counter.
 *
 * If a request with same key exists in request buffer, that request is replaced
 * with the new one. Otherwise the new request is appended to the end of the
 * request buffer, and the request counter is incremented.
 *
 * @param[in,out] reqs the request buffer
 * @param[in,out] req_cnt the request counter
 * @param[in] req the new request
 */
inline void AddNewCloverReq(std::vector<CloverRequest> &reqs,
                            unsigned int &req_cnt, const CloverRequest &req) {
  unsigned int req_idx = req_cnt;
  // overwrite previous requests in current batch with the same key
  for (unsigned int i = 0; i < req_cnt; i++) {
    if (reqs[i].key == req.key) {
      req_idx = i;
      break;
    }
  }
  reqs[req_idx] = req;
  if (req_idx == req_cnt) {
    req_cnt++;
  }
}

/**
 * @brief Checks and reports errors found in Clover response buffer.
 *
 * @param resp Clover response buffer
 * @param n number of responses
 * @return true if at least one error is found
 */
inline bool CheckCloverError(CloverResponse *resp, int n) {
  bool found_error = false;
  for (int i = 0; i < n; i++) {
    if (resp[i].rc != MITSUME_SUCCESS) {
      LOG(ERROR) << "Clover request " << resp[i].id
                 << " failed, type=" << static_cast<int>(resp[i].type)
                 << ", rc=" << resp[i].rc;
      found_error = true;
    }
  }
  return found_error;
}

void WorkerMain(herd_thread_params herd_params, SharedRequestQueue &req_queue,
                std::shared_ptr<SharedResponseQueue> resp_queue_ptr) {
  int i, ret;
  int wrkr_lid = herd_params.id; /* Local ID of this worker thread*/
  int num_server_ports = herd_params.num_server_ports;
  int base_port_index = herd_params.base_port_index;
  int postlist = herd_params.postlist;

  // A LRU system which records what keys are currently being offloaded. Uses
  // the version with minimum occurrence count to prevent frequent
  // insertion-eviction of less popular keys.
  LruRecordsWithMinCount<mitsume_key> lru(FLAGS_lru_size, FLAGS_lru_window,
                                          FLAGS_lru_min_count);
  // FIXME: a temporary workaround to prevent repeating insertion
  std::unordered_set<mitsume_key> inserted_keys;
  // Request queue producer token for this worker thread
  moodycamel::ProducerToken ptok(req_queue);
  // Clover request buffer. Stores "Write" and "Invalidation" requests.
  vector<CloverRequest> clover_req_buf(2 * MICA_MAX_BATCH_SIZE);
  // Clover response buffer
  CloverResponse clover_resp_buf[2 * MICA_MAX_BATCH_SIZE];
  // Reply option for Clover write requests
  CloverReplyOption write_reply_opt = FLAGS_clover_blocking
                                          ? CloverReplyOption::kAlways
                                          : CloverReplyOption::kNever;

  /* MICA instance id = wrkr_lid, NUMA node = 0 */
  mica_kv kv;
  mica_init(&kv, wrkr_lid, 0, HERD_NUM_BKTS, HERD_LOG_CAP);
  mica_populate_fixed_len(&kv, HERD_NUM_KEYS, HERD_VALUE_SIZE);

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
  RAW_CHECK(sid != -1, "shmget failed");
  req_buf = static_cast<volatile mica_op *>(shmat(sid, 0, 0));
  RAW_CHECK(req_buf != (void *)-1, "shmat failed");

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
        .dlid = static_cast<uint16_t>(clt_qp[i]->lid),
        .sl = 0,
        .src_path_bits = 0,
        .is_global = 0,
        /* port_num (> 1): device-local port for responses to this client */
        .port_num = static_cast<uint8_t>(local_port_i + 1),
    };

    ah[i] = ibv_create_ah(cb[cb_i]->pd, &ah_attr);
    RAW_CHECK(ah[i] != nullptr, "ibv_create_ah failed");
  }

  RAW_LOG(INFO, "Finish HERD worker setup");

  int ws[NUM_CLIENTS] = {0}; /* Per-client window slot */

  /* We can detect at most NUM_CLIENTS requests in each step */
  mica_op *op_ptr_arr[NUM_CLIENTS];
  mica_resp resp_arr[NUM_CLIENTS];
  ibv_send_wr wr[NUM_CLIENTS], *bad_send_wr = nullptr;
  ibv_sge sgl[NUM_CLIENTS];
  // buffer for sending newly added Clover key with IBV_WR_SEND_WITH_IMM
  mitsume_key mitsume_key_buf[NUM_CLIENTS];

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
  // Clover write/invalidation counter for measuring performance
  long num_clover_updates = 0;
  // Clover insertion counter for measuring performance
  long num_clover_inserts = 0;
  // LRU eviction (Clover invalidation) counter for measuring performance
  long num_lru_eviction = 0;

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

  timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  while (1) {
    if (unlikely(rolling_iter >= M_4)) {
      clock_gettime(CLOCK_REALTIME, &end);
      double seconds =
          (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
      printf(
          "main: Worker %d: %.2f IOPS. Avg per-port postlist = %.2f. "
          "HERD lookup fail rate = %.4f, Clover updates = %.2f/sec\n",
          wrkr_lid, M_4 / seconds, (double)nb_tx_tot / nb_post_send,
          (double)kv.num_get_fail / kv.num_get_op,
          num_clover_updates / seconds);
      /*
       * Clover insertion rate < LRU eviction rate means some previously evicted
       * keys are inserted to LRU again.
       * Re-insertion rate = max(LRU eviction rate - Clover insertion rate, 0)
       */
      LOG(INFO) << "Worker " << wrkr_lid
                << ": Clover total insertions: " << inserted_keys.size()
                << ", rate: " << num_clover_inserts / seconds
                << "/s. LRU eviction (Clover invalidation): "
                << num_lru_eviction / seconds << "/s";
      LOG_IF(INFO, wrkr_lid == 0) << "Approximated pending clover requests: "
                                  << req_queue.size_approx();

      rolling_iter = 0;
      nb_tx_tot = 0;
      nb_post_send = 0;
      num_clover_updates = 0;
      num_clover_inserts = 0;
      num_lru_eviction = 0;

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
      RAW_DCHECK(req_buf[req_offset].opcode == MICA_OP_GET ||
                     req_buf[req_offset].opcode == MICA_OP_PUT,
                 "Unknown MICA opcode");

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
      wr[wr_i].imm_data = HerdResponseCode::kNormal;

      wr[wr_i].send_flags =
          ((nb_tx[cb_i][ud_qp_i] & UNSIG_BATCH_) == 0) ? IBV_SEND_SIGNALED : 0;
      if ((nb_tx[cb_i][ud_qp_i] & UNSIG_BATCH_) == UNSIG_BATCH_) {
        hrd_poll_cq(cb[cb_i]->dgram_send_cq[ud_qp_i], 1, &wc);
      }
      // NOTE: In current implementation, IBV_SEND_INLINE is required since the
      // buffer filled in ibv_sge (MICA log) is not a registered memory region.
      // MICA log buffer is allocated in mica_init().
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
    unsigned int clover_req_cnt = 0, clover_insert_req_cnt = 0;
    for (int i = 0; i < wr_i; i++) {
      if (op_ptr_arr[i]->opcode == MICA_OP_PUT) {
        // We've modified HERD to use 64-bit hash and place the hash result in
        // the second 8 bytes of mica_key.
        mitsume_key key =
            ConvertHerdKeyToCloverKey(&op_ptr_arr[i]->key, wrkr_lid);
        bool contain_before = lru.Contain(key);
        auto evicted = lru.Put(key);
        bool contain_after = lru.Contain(key);
        // it is possible that contain_after is false:
        // 'occurrences in current time window' < FLAGS_lru_min_count
        // Insert or update the value in Clover only when it is in LRU.
        if (contain_after) {
          // FIXME: once "Clover deletion" is implemented, change the following
          // condition to:
          // if (!contain_before)
          if (inserted_keys.find(key) == inserted_keys.end()) {
            // Should insert this key
            CloverRequest req{
                key,                         // key
                op_ptr_arr[i]->value,        // buf
                op_ptr_arr[i]->val_len,      // len
                clover_insert_req_cnt,       // id
                CloverRequestType::kInsert,  // type
                wrkr_lid,                    // from
                CloverReplyOption::kAlways   // need_reply
            };
            while (!req_queue.try_enqueue(ptok, req))
              ;
            clover_insert_req_cnt++;
            inserted_keys.insert(key);
          } else {
            AddNewCloverReq(clover_req_buf, clover_req_cnt,
                            CloverRequest{
                                key,                        // key
                                op_ptr_arr[i]->value,       // buf
                                op_ptr_arr[i]->val_len,     // len
                                clover_req_cnt,             // id
                                CloverRequestType::kWrite,  // type
                                wrkr_lid,                   // from
                                write_reply_opt             // reply_opt
                            });
          }
        }
        if (!contain_before && contain_after) {
          // notify client that this key is available in Clover now
          wr[i].imm_data = HerdResponseCode::kNewOffload;
          mitsume_key_buf[i] = key;
          resp_arr[i].val_len = sizeof(mitsume_key);
          resp_arr[i].val_ptr =
              reinterpret_cast<uint8_t *>(&mitsume_key_buf[i]);
          DLOG(INFO) << "Add " << std::hex << key << std::dec << " to Clover";
        }
        if (evicted.has_value()) {
          /* FIXME: "delete" old keys instead of invalidate */
          // Special value indicating the correspondin value is invalid
          thread_local static char invalid_value[] = "error";
          // invalidate old keys
          AddNewCloverReq(clover_req_buf, clover_req_cnt,
                          CloverRequest{
                              key,                             // key
                              invalid_value,                   // buf
                              sizeof(invalid_value),           // len
                              clover_req_cnt,                  // id
                              CloverRequestType::kInvalidate,  // type
                              wrkr_lid,                        // from
                              write_reply_opt                  // reply_opt
                          });
          num_lru_eviction++;
        }
      }
    }

    // wait for insertion completes before writing values
    unsigned int clover_comps = 0;
    while (clover_comps < clover_insert_req_cnt) {
      clover_comps += resp_queue_ptr->try_dequeue_bulk(
          clover_resp_buf + clover_comps, clover_insert_req_cnt - clover_comps);
    }
    LOG_IF(FATAL, CheckCloverError(clover_resp_buf, clover_comps))
        << "Detect failed Clover insert, see above error message for details";

    while (!req_queue.try_enqueue_bulk(
        ptok, std::make_move_iterator(clover_req_buf.begin()), clover_req_cnt))
      ;
    clover_comps = 0;
    while (FLAGS_clover_blocking && clover_comps < clover_req_cnt) {
      clover_comps += resp_queue_ptr->try_dequeue_bulk(
          clover_resp_buf + clover_comps, clover_req_cnt - clover_comps);
    }
    LOG_IF(FATAL, CheckCloverError(clover_resp_buf, clover_comps))
        << "Detect failed Clover write, see above error message for details";
    num_clover_updates += clover_req_cnt;
    num_clover_inserts += clover_insert_req_cnt;

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
  FLAGS_colorlogtostderr = true;
  FLAGS_logtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();

  CHECK(FLAGS_herd_base_port_index >= 0 && FLAGS_herd_base_port_index <= 8);
  CHECK(FLAGS_herd_server_ports >= 1 && FLAGS_herd_server_ports <= 8);
  CHECK_GE(FLAGS_postlist, 1);
  CHECK_LT(FLAGS_herd_server_ports, MAX_SERVER_PORTS);
  CHECK(NUM_CLIENTS % FLAGS_herd_server_ports == 0);

  /// MICA-related checks. Note that postlist is the largest batch size we feed
  /// into MICA. The average postlist per port in a dual-port NIC should be
  /// postlist / 2.
  CHECK(MICA_MAX_BATCH_SIZE >= FLAGS_postlist);

  CHECK(UNSIG_BATCH >= FLAGS_postlist); /* Postlist check */
  CHECK(FLAGS_postlist <= NUM_CLIENTS); /* Static sizing of arrays below */

  LOG(INFO) << "Using " << NUM_WORKERS << " HERD worker threads and "
            << FLAGS_clover_threads << " Clover worker threads";
  LOG(INFO) << "Expecting " << NUM_CLIENTS << " client threads in total";

  LOG(INFO) << "LRU capacity: " << FLAGS_lru_size
            << ", window: " << FLAGS_lru_window
            << ", min count: " << FLAGS_lru_min_count;

  // Setup Clover compute node
  CloverComputeNodeWrapper clover_node(NUM_WORKERS);
  clover_node.Initialize();
  LOG(INFO) << "Done initializing clover compute node";

  SharedRequestQueue clover_req_queue(2 * MICA_MAX_BATCH_SIZE, NUM_WORKERS, 0);
  std::vector<std::shared_ptr<SharedResponseQueue>> clover_resp_queues;
  // using clover_resp_queues(NUM_WORKERS,
  // std::make_shared<SharedResponseQueue>(...)) will create multiple pointers
  // pointing to the same queue
  for (int i = 0; i < NUM_WORKERS; i++) {
    clover_resp_queues.emplace_back(std::make_shared<SharedResponseQueue>(
        2 * MICA_MAX_BATCH_SIZE, 0, FLAGS_clover_threads));
  }

  std::vector<std::thread> threads;
  for (int i = 0; i < NUM_WORKERS; i++) {
    herd_thread_params param = {
        .id = i,
        .base_port_index = FLAGS_herd_base_port_index,
        .num_server_ports = FLAGS_herd_server_ports,
        .num_client_ports = -1,   // does not matter for worker
        .update_percentage = 0U,  // does not matter for worker
        .postlist = FLAGS_postlist};
    auto t = std::thread(WorkerMain, param, std::ref(clover_req_queue),
                         clover_resp_queues.at(i));
    threads.emplace_back(std::move(t));
  }

  for (int i = 0; i < FLAGS_clover_threads; i++) {
    auto t = std::thread(CloverThreadMain, std::ref(clover_node),
                         std::ref(clover_req_queue),
                         std::cref(clover_resp_queues), i);
    threads.emplace_back(std::move(t));
  }

  for (auto &t : threads) {
    t.join();
  }
  return 0;
}
