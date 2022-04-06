#define _DEFAULT_SOURCE
#include <sys/shm.h>

#include "hrd.h"
#include "main.h"
#include "mica.h"

#define INLINE_CUTOFF \
  (HRD_MAX_INLINE - (sizeof(struct mica_key) + MICA_OBJ_METADATA_SIZE))

void* run_worker(void* arg) {
  int i, ret;
  struct thread_params params = *(struct thread_params*)arg;
  int wrkr_lid = params.id; /* Local ID of this worker thread*/
  int num_server_ports = params.num_server_ports;
  int base_port_index = params.base_port_index;
  int postlist = params.postlist;

  /*
   * MICA-related checks. Note that @postlist is the largest batch size we
   * feed into MICA. The average postlist per port in a dual-port NIC should
   * be @postlist / 2.
   */
  assert(MICA_MAX_BATCH_SIZE >= postlist);
  assert(HERD_VALUE_SIZE <= MICA_MAX_VALUE);

  assert(UNSIG_BATCH >= postlist); /* Postlist check */
  assert(postlist <= NUM_CLIENTS); /* Static sizing of arrays below */

  assert(num_server_ports < MAX_SERVER_PORTS); /* Avoid dynamic alloc */
  struct hrd_ctrl_blk* cb[MAX_SERVER_PORTS];

  // Create queue pairs for SEND responses for each server ports
  for (i = 0; i < num_server_ports; i++) {
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
  req_buf = shmat(sid, 0, 0);
  assert(req_buf != (void*)-1);

  // Dummy reply buffers
  volatile char* rep_buf = NULL;
  rep_buf = malloc(HERD_VALUE_SIZE * NUM_CLIENTS);
  if (rep_buf == NULL) {
    fputs("malloc rep_buf failed\n", stderr);
    exit(EXIT_FAILURE);
  }
  memset(rep_buf, 0, HERD_VALUE_SIZE * NUM_CLIENTS);

  struct ibv_mr** rep_buf_mrs = NULL;
  rep_buf_mrs = malloc(sizeof(struct ibv_mr*) * num_server_ports);
  for (int i = 0; i < num_server_ports; i++) {
    rep_buf_mrs[i] =
        ibv_reg_mr(cb[i]->pd, rep_buf, HERD_VALUE_SIZE * NUM_CLIENTS, 0);
    if (rep_buf_mrs[i] == NULL) {
      fputs("ibv_reg_mr returns NULL\n", stderr);
      exit(EXIT_FAILURE);
    }
  }

  /* Create an address handle for each client */
  struct ibv_ah* ah[NUM_CLIENTS];
  memset(ah, 0, NUM_CLIENTS * sizeof(uintptr_t));
  struct hrd_qp_attr* clt_qp[NUM_CLIENTS];

  for (i = 0; i < NUM_CLIENTS; i++) {
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

  int ws[NUM_CLIENTS] = {0}; /* Per-client window slot */

  /* We can detect at most NUM_CLIENTS requests in each step */
  struct mica_op* op_ptr_arr[NUM_CLIENTS];
  struct ibv_send_wr wr[NUM_CLIENTS], *bad_send_wr = NULL;
  struct ibv_sge sgl[NUM_CLIENTS];

  for (int i = 0; i < NUM_CLIENTS; i++) {
    sgl[i].length = HERD_VALUE_SIZE;

    wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    wr[i].opcode = IBV_WR_SEND_WITH_IMM;
    wr[i].num_sge = 1;
    wr[i].sg_list = &sgl[i];
  }

  /* If postlist is disabled, remember the cb to send() each @wr from */
  int cb_for_wr[NUM_CLIENTS];

  /* If postlist is enabled, we instead create per-cb linked lists of wr's */
  struct ibv_send_wr* first_send_wr[MAX_SERVER_PORTS] = {NULL};
  struct ibv_send_wr* last_send_wr[MAX_SERVER_PORTS] = {NULL};
  struct ibv_wc wc;
  long long rolling_iter = 0; /* For throughput measurement */
  long long nb_tx[MAX_SERVER_PORTS][NUM_UD_QPS] = {{0}}; /* CQE polling */
  int ud_qp_i = 0; /* UD QP index: same for both ports */

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

  printf("worker %2d is ready.\n", wrkr_lid);

  while (1) {
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

      int req_offset = OFFSET(wrkr_lid, clt_i, ws[clt_i]);
      if (req_buf[req_offset].opcode < HERD_OP_GET) {
        continue;
      }

      op_ptr_arr[wr_i] = (struct mica_op*)&req_buf[req_offset];

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
        wr[wr_i].next = NULL;
        cb_for_wr[wr_i] = cb_i;
      }

      /* Fill in the work request (except the scatter gather elements, they will
       * be filled in next for loop, after mica_batch_op(). */
      wr[wr_i].wr.ud.ah = ah[clt_i];
      wr[wr_i].wr.ud.remote_qpn = clt_qp[clt_i]->qpn;

      wr[wr_i].imm_data = op_ptr_arr[wr_i]->seq;

      wr[wr_i].send_flags =
          ((nb_tx[cb_i][ud_qp_i] & UNSIG_BATCH_) == 0) ? IBV_SEND_SIGNALED : 0;
      if ((nb_tx[cb_i][ud_qp_i] & UNSIG_BATCH_) == UNSIG_BATCH_) {
        hrd_poll_cq(cb[cb_i]->dgram_send_cq[ud_qp_i], 1, &wc);
      }
      if (HERD_VALUE_SIZE <= INLINE_CUTOFF) {
        wr[wr_i].send_flags |= IBV_SEND_INLINE;
      } else {
        sgl[wr_i].lkey = rep_buf_mrs[ud_qp_i]->lkey;
      }

      HRD_MOD_ADD(ws[clt_i], WINDOW_SIZE);

      rolling_iter++;
      nb_tx[cb_i][ud_qp_i]++; /* Must increment inside loop */
      nb_new_req[cb_i]++;
      wr_i++;

      if (wr_i == postlist) {
        break;
      }
    }

    /*
     * Fill in the computed @val_ptr's. For non-postlist mode, this loop
     * must go from 0 to (@wr_i - 1) to follow the signaling logic.
     */
    int nb_new_req_tot = wr_i;
    for (wr_i = 0; wr_i < nb_new_req_tot; wr_i++) {
      sgl[wr_i].addr = (uintptr_t)rep_buf + wr_i * HERD_VALUE_SIZE;

      if (USE_POSTLIST == 0) {
        ret = ibv_post_send(cb[cb_for_wr[wr_i]]->dgram_qp[ud_qp_i], &wr[wr_i],
                            &bad_send_wr);
        CPE(ret, "ibv_post_send error", ret);
      }
    }

    for (i = 0; i < num_server_ports; i++) {
      if (nb_new_req[i] == 0) {
        continue;
      }

      /* If postlist is off, we should post replies in the loop above */
      if (USE_POSTLIST == 1) {
        last_send_wr[i]->next = NULL;
        ret = ibv_post_send(cb[i]->dgram_qp[ud_qp_i], first_send_wr[i],
                            &bad_send_wr);
        CPE(ret, "ibv_post_send error", ret);
      }
    }

    /* Use a different UD QP for the next postlist */
    HRD_MOD_ADD(ud_qp_i, NUM_UD_QPS);
  }

  return NULL;
}
