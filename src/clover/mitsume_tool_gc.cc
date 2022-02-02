#include "mitsume_tool_gc.h"

#include <folly/logging/xlog.h>

#include <set>

/**
 * mitsume_tool_gc_submit_request: submit gc request
 * @thread_metadata: respected thread_metadata
 * @key: target key
 * @old_ptr: old mitsume pointer
 * @new_ptr: new mitsume pointer
 * return: return success
 */
// inline int mitsume_tool_gc_submit_request(struct mitsume_consumer_metadata
// *thread_metadata, mitsume_key key, uint64_t old_ptr, uint64_t new_ptr,
// uint64_t shortcut_ptr)
int mitsume_tool_gc_submit_request(
    struct mitsume_consumer_metadata *thread_metadata, mitsume_key key,
    struct mitsume_tool_communication *old_entry,
    struct mitsume_tool_communication *new_entry, int gc_mode) {
  // need to use key to put the request into the correct controller linked-list
  // async processing queue
  struct mitsume_gc_thread_request *request =
      (struct mitsume_gc_thread_request *)mitsume_tool_cache_alloc(
          MITSUME_ALLOCTYPE_GC_THREAD_REQUEST);
  int target_gc_thread_id;

  request->gc_entry.key = key;
  request->gc_mode = gc_mode;

  mitsume_struct_copy_ptr_replication(request->gc_entry.old_ptr,
                                      old_entry->replication_ptr,
                                      old_entry->replication_factor);
  mitsume_struct_copy_ptr_replication(request->gc_entry.new_ptr,
                                      new_entry->replication_ptr,
                                      new_entry->replication_factor);
  if (old_entry->replication_factor != new_entry->replication_factor) {
    MITSUME_PRINT_ERROR("replication factor doesn't match %d:%d\n",
                        (int)old_entry->replication_factor,
                        (int)new_entry->replication_factor);
  }
  request->gc_entry.replication_factor = old_entry->replication_factor;
  MITSUME_STRUCT_CHECKNULL_PTR_REPLICATION(
      request->gc_entry.old_ptr, request->gc_entry.replication_factor);
  MITSUME_STRUCT_CHECKNULL_PTR_REPLICATION(
      request->gc_entry.new_ptr, request->gc_entry.replication_factor);
  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(request->gc_entry.old_ptr,
  // request->gc_entry.replication_factor, key);
  // MITSUME_TOOL_PRINT_POINTER_KEY_REPLICATION(request->gc_entry.new_ptr,
  // request->gc_entry.replication_factor, key);
  // MITSUME_TOOL_PRINT_GC_POINTER_KEY(&request->gc_entry.old_ptr[0],
  // &request->gc_entry.new_ptr[0], key);
  request->shortcut_ptr.pointer = old_entry->shortcut_ptr.pointer;
  // MITSUME_TOOL_PRINT_POINTER_KEY(&old_ptr, &new_ptr, key);
  // MITSUME_INFO("%lld %llx %llx\n", request->gc_entry.key,
  // request->gc_entry.old_ptr.pointer, request->gc_entry.new_ptr.pointer);

  // target_gc_thread_id = thread_metadata->target_gc_thread_id;

  // target gc thread is determined during hashtable installing. Therefore, it
  // doesn't need to do modular in each request
  target_gc_thread_id = old_entry->target_gc_thread;
  request->target_controller = old_entry->target_gc_controller;

  thread_metadata->local_ctx_clt->gc_processing_queue[target_gc_thread_id]
      .enqueue(request);

  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_gc_shortcut_buffer: push the shortcut update request into
 * internal shortcut update linked-list
 * @lh: query-lh
 * @offset: query-offset
 * @shortcut_lh: update query info into shortcut-lh
 * @shortcut_offset: update query info into shortcut-offset
 * @replication_factor: number of replication
 * return: return a result after insert to list
 */
int mitsume_tool_gc_shortcut_buffer(
    struct mitsume_consumer_gc_metadata *gc_metadata, mitsume_key key,
    struct mitsume_ptr *ptr, struct mitsume_ptr *shortcut_ptr,
    uint32_t replication_factor, int target_controller_idx) {
  struct mitsume_consumer_gc_shortcut_update_element *target_element;
  target_element = (struct mitsume_consumer_gc_shortcut_update_element *)
      mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT);
  mitsume_struct_copy_ptr_replication(
      target_element->writing_shortcut.shortcut_ptr, ptr, replication_factor);
  target_element->shortcut_ptr.pointer = shortcut_ptr->pointer;
  target_element->key = key;

  gc_metadata->internal_shortcut_buffer_list[target_controller_idx].push(
      target_element);
  return MITSUME_SUCCESS;
}

/**
 * @brief Updates remote shortcut with RDMA_WRITE and pushes the write request
 * into internal_shortcut_send_list for polling later.
 *
 * The request is sent using master coroutine, which means that it won't yield
 * to another coroutine during request submission.
 *
 * @param target_element entry from buffer list
 * @return MITSUME_SUCCESS
 */
int mitsume_tool_gc_shortcut_send(
    struct mitsume_consumer_gc_metadata *gc_metadata,
    struct mitsume_consumer_gc_shortcut_update_element *target_element,
    int target_controller_idx) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_ptr *shortcut_ptr = &target_element->shortcut_ptr;
  struct mitsume_shortcut *output_ptr =
      (struct mitsume_shortcut *)gc_metadata->local_inf->input_space[coro_id];
  ptr_attr *remote_mr =
      &gc_metadata->local_ctx_clt
           ->all_shortcut_attr[MITSUME_GET_PTR_LH(shortcut_ptr->pointer)];
  memcpy(output_ptr, target_element->writing_shortcut.shortcut_ptr,
         sizeof(struct mitsume_shortcut));

  target_element->wr_id =
      mitsume_local_thread_get_wr_id(gc_metadata->local_inf);
  // MITSUME_PRINT("send %llu\n", (unsigned long long
  // int)target_element->wr_id); MITSUME_("send %llu\n", (unsigned long long
  // int)target_element->wr_id); MITSUME_TOOL_PRINT_POINTER_KEY(target_element)

  userspace_one_write_inline(gc_metadata->local_ctx_clt->ib_ctx,
                             target_element->wr_id,
                             gc_metadata->local_inf->input_mr[coro_id],
                             sizeof(struct mitsume_shortcut), remote_mr, 0);

  gc_metadata->internal_shortcut_send_list[target_controller_idx].push(
      target_element);

  return MITSUME_SUCCESS;
}

/**
 * mitsume_tool_gc_shortcut_poll: take a entry which is just removed from send
 * list, then poll
 * @target_element: entry from send list
 * return: return a result after poll
 */
int mitsume_tool_gc_shortcut_poll(
    struct mitsume_consumer_gc_metadata *gc_metadata,
    struct mitsume_consumer_gc_shortcut_update_element *target_element) {
  struct mitsume_ptr *shortcut_ptr = &target_element->shortcut_ptr;
  ptr_attr *remote_mr =
      &gc_metadata->local_ctx_clt
           ->all_shortcut_attr[MITSUME_GET_PTR_LH(shortcut_ptr->pointer)];

  // MITSUME_PRINT("poll %llu\n", (unsigned long long
  // int)target_element->wr_id);
  userspace_one_poll(gc_metadata->local_ctx_clt->ib_ctx, target_element->wr_id,
                     remote_mr);
  mitsume_local_thread_put_wr_id(gc_metadata->local_inf, target_element->wr_id);

  return MITSUME_SUCCESS;
}

/**
 * @brief Sends GC request to controller (server) and waits for completion.
 *
 * Runs at client side.
 *
 * @param gc_thread_metadata respected thread_metadata
 * @param gc_entry target gc entry
 * @param counts how many requests inside this entry
 * @param target_controller_id the controller id (server node)
 * @return MITSUME_SUCCESS
 */
static int mitsume_tool_gc_processing_requests(
    struct mitsume_consumer_gc_metadata *gc_thread_metadata,
    struct mitsume_gc_entry *gc_entry, int counts, int target_controller_id) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_large_msg *send;
  struct mitsume_msg *reply;
  int i;

  struct thread_local_inf *local_inf = gc_thread_metadata->local_inf;

  send = (struct mitsume_large_msg *)local_inf->input_space[coro_id];
  reply = local_inf->output_space[coro_id];
  // reply = mitsume_tool_cache_alloc(MITSUME_ALLOCTYPE_LARGE_MSG);

  // MITSUME_PRINT("mitsume ask entries for queue %d by size %d\n", queue_id,
  // request_size);
  send->msg_header.type = MITSUME_GC_REQUEST;
  send->msg_header.src_id = gc_thread_metadata->local_ctx_clt->node_id;
  send->msg_header.des_id = target_controller_id;
  send->msg_header.thread_id = gc_thread_metadata->gc_thread_id;
  send->content.msg_gc_request.gc_number = counts;

  send->msg_header.reply_attr.addr =
      (uint64_t)local_inf->output_mr[coro_id]->addr;
  send->msg_header.reply_attr.rkey = local_inf->output_mr[coro_id]->rkey;
  send->msg_header.reply_attr.machine_id =
      gc_thread_metadata->local_ctx_clt->node_id;

  reply->end_crc = MITSUME_WAIT_CRC;

  for (i = 0; i < counts; i++) {
    CopyGcEntry(&send->content.msg_gc_request.gc_entry[i], &gc_entry[i]);
  }

  // send the request out
  mitsume_send_full_message(gc_thread_metadata->local_ctx_clt->ib_ctx,
                            local_inf, local_inf->input_mr[coro_id],
                            local_inf->output_mr[coro_id], &send->msg_header,
                            gc_thread_metadata->local_ctx_clt->node_id,
                            target_controller_id, sizeof(mitsume_large_msg));

  if (reply->content.success_gc_number == 0) {
    MITSUME_PRINT_ERROR("check\n");
    for (i = 0; i < counts; i++) {
      MITSUME_TOOL_PRINT_POINTER_NULL(
          &send->content.msg_gc_request.gc_entry[i]
               .old_ptr[MITSUME_REPLICATION_PRIMARY]);
      MITSUME_TOOL_PRINT_POINTER_NULL(
          &send->content.msg_gc_request.gc_entry[i]
               .new_ptr[MITSUME_REPLICATION_PRIMARY]);
      /*if(send->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY].pointer!=reply->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY].pointer)
      {
          MITSUME_TOOL_PRINT_POINTER_NULL(&send->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY],
      &reply->content.msg_gc_request.gc_entry[i].old_ptr[MITSUME_REPLICATION_PRIMARY]);
      }
      if(send->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY].pointer!=reply->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY].pointer)
      {
          MITSUME_TOOL_PRINT_POINTER_NULL(&send->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY],
      &reply->content.msg_gc_request.gc_entry[i].new_ptr[MITSUME_REPLICATION_PRIMARY]);
      }
      if(send->content.msg_gc_request.gc_entry[i].key!=reply->content.msg_gc_request.gc_entry[i].key)
      {
          MITSUME_ERROR("check here %lld %lld",
      send->content.msg_gc_request.gc_entry[i].key,
      reply->content.msg_gc_request.gc_entry[i].key);
      }*/
    }
  }
  // MITSUME_PRINT("finish gc %d %d\n", gc_number,
  // reply->content.success_gc_number);
  return MITSUME_SUCCESS;
}

static void PrepareGcSendMsg(mitsume_large_msg *msg,
                             mitsume_consumer_gc_metadata *thread_metadata,
                             mitsume_gc_entry *src_entries, int counts,
                             int controller, ibv_mr *resp_mr) {
  int node_id = thread_metadata->local_ctx_clt->node_id;
  msg->msg_header.type = MITSUME_GC_REQUEST;
  msg->msg_header.src_id = node_id;
  msg->msg_header.des_id = controller;
  msg->msg_header.thread_id = thread_metadata->gc_thread_id;

  msg->content.msg_gc_request.gc_number = counts;
  for (int i = 0; i < counts; i++) {
    CopyGcEntry(&msg->content.msg_gc_request.gc_entry[i], &src_entries[i]);
  }

  // Let controller know where to put response
  msg->msg_header.reply_attr.addr = (uint64_t)resp_mr->addr;
  msg->msg_header.reply_attr.rkey = resp_mr->rkey;
  msg->msg_header.reply_attr.machine_id = node_id;
}

/**
 * @brief Sends GC requests to server (controller) and ignores the response.
 *
 * This function uses input_space[0] of the GC thread to store response. Even
 * though this function does not block for the response, the buffer will still
 * contains response data. If another task in GC thread also uses input_space[0]
 * for response, they might also receive responses with header being
 * MITSUME_GC_REQUEST_ACK.
 *
 * @param gc_thread_metadata the GC thread metadata
 * @param gc_entry GC requests to send
 * @param counts number of entries in gc_entry
 * @param controller the remote controller index (base index is 0)
 */
static void SendGcRequest(mitsume_consumer_gc_metadata *gc_thread_metadata,
                          mitsume_gc_entry *gc_entry, int counts,
                          int controller) {
  // master coroutine
  constexpr int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;

  if (counts == 0) {
    return;
  }

  thread_local_inf *local_inf = gc_thread_metadata->local_inf;
  ib_inf *ib_ctx = gc_thread_metadata->local_ctx_clt->ib_ctx;

  mitsume_large_msg *send =
      reinterpret_cast<mitsume_large_msg *>(local_inf->input_space[coro_id]);
  mitsume_msg *reply = local_inf->output_space[coro_id];
  auto send_mr = local_inf->input_mr[coro_id];
  auto recv_mr = local_inf->output_mr[coro_id];

  PrepareGcSendMsg(send, gc_thread_metadata, gc_entry, counts, controller,
                   recv_mr);

  reply->end_crc = MITSUME_WAIT_CRC;

  auto wr_id = mitsume_local_thread_get_wr_id(local_inf);
  int qp_idx = wr_id_to_qp_index(wr_id, controller);
  userspace_one_send(ib_ctx->conn_qp[qp_idx], send_mr,
                     sizeof(mitsume_large_msg), wr_id);

  ptr_attr tmp_ptr_attr;
  tmp_ptr_attr.machine_id = controller;
  auto ret = userspace_one_poll(ib_ctx, wr_id, &tmp_ptr_attr);
  if (ret != MITSUME_SUCCESS) {
    throw std::runtime_error("GC request polling failed");
  }
  mitsume_local_thread_put_wr_id(local_inf, wr_id);
}

void UpdateShortcutsWithGcRequests(mitsume_consumer_gc_metadata *gc_thread,
                                   int controller) {
  std::set<mitsume_key> submitted_keys;

  // At most MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS requests will be
  // submitted
  while (!gc_thread->internal_shortcut_buffer_list[controller].empty()) {
    auto tmp = gc_thread->internal_shortcut_buffer_list[controller].front();
    gc_thread->internal_shortcut_buffer_list[controller].pop();

    if (submitted_keys.find(tmp->key) == submitted_keys.end()) {
      mitsume_tool_gc_shortcut_send(gc_thread, tmp, controller);
    } else {
      // There's already another request that updates the shortcut for current
      // key. Therefore, we may discard this shortcut update request.
      mitsume_tool_cache_free(tmp,
                              MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT);
    }
  }
  while (!gc_thread->internal_shortcut_send_list[controller].empty()) {
    auto tmp = gc_thread->internal_shortcut_send_list[controller].front();
    gc_thread->internal_shortcut_send_list[controller].pop();

    mitsume_tool_gc_shortcut_poll(gc_thread, tmp);
    mitsume_tool_cache_free(tmp, MITSUME_ALLOCTYPE_GC_SHORTCUT_UPDATE_ELEMENT);
  }
}

/**
 * @brief Main function of client-side GC thread. Process shortcut update and GC
 * request from queue.
 */
void *mitsume_tool_gc_running_thread(void *input_metadata) {
  struct mitsume_consumer_gc_metadata *gc_thread =
      (struct mitsume_consumer_gc_metadata *)input_metadata;
  int end_flag = 0;
  struct mitsume_gc_entry base_entry[MITSUME_CON_NUM]
                                    [MITSUME_CLT_CONSUMER_MAX_GC_NUMS];
  struct mitsume_gc_thread_request *new_request;
  int accumulate_gc_num[MITSUME_CON_NUM],
      accumulate_shortcut_num[MITSUME_CON_NUM], accumulate_shortcut_total_num;
  int target_controller_idx = -1;
  int per_controller_idx;
  int gc_thread_id = gc_thread->gc_thread_id;

  MITSUME_INFO("running gc thread %d (%d)\n", gc_thread->gc_thread_id,
               gettid());

  // The queue of GC requests of this GC thread
  auto &gc_req_queue =
      gc_thread->local_ctx_clt->gc_processing_queue[gc_thread_id];

  // For benchmarking
  using clock = std::chrono::steady_clock;
  long process_cnt = 0;
  constexpr long kReportIter = 2000000;
  auto start = clock::now();

  while (!end_flag) {
    while (gc_thread->local_ctx_clt->gc_processing_block.load())
      ;
    if (!gc_req_queue.empty()) {
      memset(accumulate_gc_num, 0, sizeof(int) * MITSUME_CON_NUM);
      memset(accumulate_shortcut_num, 0, sizeof(int) * MITSUME_CON_NUM);
      accumulate_shortcut_total_num = 0;
      while (!gc_req_queue.empty() &&
             accumulate_shortcut_total_num <
                 MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS) {
        // get one entry from list and makesure the total available shortcut
        // number is still below limitation
        gc_req_queue.dequeue(new_request);
        target_controller_idx =
            new_request->target_controller - MITSUME_FIRST_ID;
        // if the next gc requests is already above limitation, abort current
        // entry, move to submit target_controller_idx is used here because there
        // is a mismatch between idx and controller id (id is + MITSUME_FIRST_ID
        // of idx)
        if (accumulate_gc_num[target_controller_idx] ==
                MITSUME_CLT_CONSUMER_MAX_GC_NUMS ||
            accumulate_shortcut_num[target_controller_idx] ==
                MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS) {
          break;
        }

        accumulate_shortcut_total_num++;
        accumulate_shortcut_num[target_controller_idx]++;

        assert(new_request->gc_mode == MITSUME_TOOL_GC_REGULAR_PROCESSING);
        if (new_request->gc_mode == MITSUME_TOOL_GC_REGULAR_PROCESSING) {
          // release lock, the speed of this gc thread is not critical.
          int base_entry_idx = accumulate_gc_num[target_controller_idx];
          CopyGcEntry(&base_entry[target_controller_idx][base_entry_idx],
                      &new_request->gc_entry);

          if (!new_request->gc_entry.new_ptr[MITSUME_REPLICATION_PRIMARY]
                   .pointer ||
              !new_request->gc_entry.key ||
              !new_request->shortcut_ptr.pointer) {
            MITSUME_PRINT_ERROR("NULL setup for GC request %d ptr %#lx key %ld",
                                accumulate_gc_num[target_controller_idx],
                                (long unsigned int)new_request->gc_entry
                                    .new_ptr[MITSUME_REPLICATION_PRIMARY]
                                    .pointer,
                                (long unsigned int)new_request->gc_entry.key);
            MITSUME_IDLE_HERE;
          }

          // Push item into internal_shortcut_buffer_list
          mitsume_tool_gc_shortcut_buffer(
              gc_thread, new_request->gc_entry.key,
              new_request->gc_entry.new_ptr, &new_request->shortcut_ptr,
              new_request->gc_entry.replication_factor, target_controller_idx);
          accumulate_gc_num[target_controller_idx]++;
        } else if (new_request->gc_mode ==
                   MITSUME_TOOL_GC_UPDATE_SHORTCUT_ONLY) {
          MITSUME_PRINT_ERROR("This mode is not supported currently\n");
          MITSUME_IDLE_HERE;
          // mitsume_tool_gc_shortcut_buffer(gc_thread,
          // new_request->gc_entry.key, new_request->gc_entry.new_ptr,
          // &new_request->shortcut_ptr,
          // new_request->gc_entry.replication_factor, target_controller_idx);
        } else {
          MITSUME_PRINT_ERROR("wrong gc mode %d\n", new_request->gc_mode);
        }

        mitsume_tool_cache_free(new_request,
                                MITSUME_ALLOCTYPE_GC_THREAD_REQUEST);
      }

      for (per_controller_idx = 0; per_controller_idx < MITSUME_CON_NUM;
           per_controller_idx++) {
        UpdateShortcutsWithGcRequests(gc_thread, per_controller_idx);

        if (accumulate_gc_num[per_controller_idx]) {
          mitsume_tool_gc_processing_requests(
              gc_thread, base_entry[per_controller_idx],
              accumulate_gc_num[per_controller_idx],
              per_controller_idx + MITSUME_FIRST_ID);
          // SendGcRequest(gc_thread, base_entry[per_controller_idx],
          //               accumulate_gc_num[per_controller_idx],
          //               per_controller_idx + MITSUME_FIRST_ID);
          process_cnt += accumulate_gc_num[per_controller_idx];
          if (process_cnt >= kReportIter) {
            auto end = clock::now();
            double sec = std::chrono::duration<double>(end - start).count();
            XLOGF(INFO, "GC {}: {:.2f} submit/s, {} in queue",
                  gc_thread_id, process_cnt / sec, gc_req_queue.size());
            process_cnt = 0;
            start = clock::now();
          }
        }
        // MITSUME_STAT_ARRAY_ADD(gc_thread->gc_thread_id, 1);
      }
    } else {
      schedule();
    }
  }
  MITSUME_INFO("exit gc thread %d\n", gc_thread->gc_thread_id);
  return NULL;
}

/**
 * @brief A background thread to check if there are any epoch forwarding
 * request.
 *
 * Runs at client side.
 *
 * @param epoch_thread epoch thread metadata
 */
void *mitsume_tool_gc_epoch_forwarding(void *input_epoch_thread) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_consumer_epoch_metadata *epoch_thread =
      (struct mitsume_consumer_epoch_metadata *)input_epoch_thread;
  struct mitsume_msg *received_message;
  struct mitsume_msg *replied_message;
  uint32_t received_length;
  uint64_t received_id;
  struct mitsume_ctx_clt *local_ctx_clt = epoch_thread->local_ctx_clt;
  struct thread_local_inf *local_inf = epoch_thread->local_inf;
  int end_flag = 0;
  long long int target_mr;
  long long int target_qp;
  uint64_t wr_id;
  ptr_attr *target_qp_mr_attr;
  int per_thread;
  int try_lock;
  replied_message = local_inf->input_space[coro_id];
  ibv_wc input_wc[1];
  while (!end_flag) {
    userspace_one_poll_wr(local_ctx_clt->ib_ctx->server_recv_cqs, 1,
                          MITSUME_CON_ALLOCATOR_THREAD_NUMBER, input_wc, true);
    // MITSUME_INFO("get message\n");
    // usleep(MITSUME_GC_CLT_EPOCH_DELAY*1000);
    received_length = input_wc[0].byte_len;
    received_id = input_wc[0].wr_id;

    target_qp = RSEC_ID_TO_QP(received_id);
    target_mr = RSEC_ID_TO_RECV_MR(received_id);
    target_qp_mr_attr = local_ctx_clt->per_qp_mr_attr_list[target_qp];

    received_message = (struct mitsume_msg *)target_qp_mr_attr[target_mr].addr;
    if (received_length == sizeof(struct mitsume_msg)) {
      switch (received_message->msg_header.type) {
      case MITSUME_GC_EPOCH_FORWARD: {
        if (received_message->content.msg_gc_epoch_forward
                    .request_epoch_number <= local_ctx_clt->gc_current_epoch ||
            received_message->content.msg_gc_epoch_forward
                    .request_epoch_number !=
                local_ctx_clt->gc_current_epoch + MITSUME_GC_EPOCH_STEPSIZE) {
          MITSUME_PRINT_ERROR("wrong epoch:%d (current:%d)\n",
                              received_message->content.msg_gc_epoch_forward
                                  .request_epoch_number,
                              local_ctx_clt->gc_current_epoch);
          replied_message->content.msg_gc_epoch_forward.request_epoch_number =
              0;
          replied_message->msg_header.type = MITSUME_GC_EPOCH_FORWARD_ACK;

          local_ctx_clt->gc_current_epoch =
              received_message->content.msg_gc_epoch_forward
                  .request_epoch_number;
          // liteapi_reply_message(replied_message, sizeof(struct mitsume_msg),
          // received_descriptor);

          replied_message->end_crc = MITSUME_REPLY_CRC;
          wr_id = mitsume_local_thread_get_wr_id(local_inf);
          mitsume_reply_full_message(
              local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
              local_inf->input_mr[coro_id], sizeof(mitsume_msg));
          userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                             &received_message->msg_header.reply_attr);
          mitsume_local_thread_put_wr_id(local_inf, wr_id);
          continue;
        }
        MITSUME_INFO("epoch %d end\n", local_ctx_clt->gc_current_epoch);
        local_ctx_clt->gc_epoch_block_lock.lock();
        // lock the mitsume tool to avoid all the future requests
        // drain all requests

        for (per_thread = 0; per_thread < MITSUME_CLT_CONSUMER_NUMBER;
             per_thread++) {
          // try_spin_lock(&local_ctx_clt->thread_metadata[per_thread].current_running_lock);
          for (int per_coro = 0; per_coro < MITSUME_CLT_COROUTINE_NUMBER;
               per_coro++) {
            do {
              try_lock = local_ctx_clt->thread_metadata[per_thread]
                             .current_running_lock[per_coro]
                             .try_lock();
              schedule();
            } while (!try_lock);
          }
        }

        // Pause client-side GC threads
        local_ctx_clt->gc_processing_block.store(true);

        replied_message->content.msg_gc_epoch_forward.request_epoch_number =
            local_ctx_clt->gc_current_epoch;
        replied_message->msg_header.type = MITSUME_GC_EPOCH_FORWARD_ACK;

        local_ctx_clt->gc_current_epoch =
            received_message->content.msg_gc_epoch_forward.request_epoch_number;
        // MITSUME_STAT_SET(MITSUME_CURRENT_EPOCH,
        // local_ctx_clt->gc_current_epoch);

        replied_message->end_crc = MITSUME_REPLY_CRC;
        wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                           &received_message->msg_header.reply_attr);
        mitsume_local_thread_put_wr_id(local_inf, wr_id);

        for (per_thread = 0; per_thread < MITSUME_CLT_CONSUMER_NUMBER;
             per_thread++) {
          for (int per_coro = 0; per_coro < MITSUME_CLT_COROUTINE_NUMBER;
               per_coro++) {
            local_ctx_clt->thread_metadata[per_thread]
                .current_running_lock[MITSUME_CLT_COROUTINE_NUMBER]
                .unlock();
          }
        }

        local_ctx_clt->gc_processing_block.store(false);
        local_ctx_clt->gc_epoch_block_lock.unlock();
      } break;
      default:
        MITSUME_PRINT_ERROR("wrong type %d\n",
                            received_message->msg_header.type);
        replied_message->content.msg_gc_epoch_forward.request_epoch_number = 0;
        replied_message->msg_header.type = MITSUME_GC_GENERAL_FAULT;

        local_ctx_clt->gc_current_epoch =
            received_message->content.msg_gc_epoch_forward.request_epoch_number;

        replied_message->end_crc = MITSUME_REPLY_CRC;
        wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                           &received_message->msg_header.reply_attr);
        mitsume_local_thread_put_wr_id(local_inf, wr_id);
        // liteapi_reply_message(replied_message, sizeof(struct mitsume_msg),
        // received_descriptor);
      }
    } else {
      if (received_length) {
        if (received_length != sizeof(struct mitsume_msg)) {
          MITSUME_PRINT_ERROR("wrong current_running value %d\n",
                              received_length);
        }
        replied_message->content.msg_gc_epoch_forward.request_epoch_number = 0;
        replied_message->msg_header.type = MITSUME_GC_GENERAL_FAULT;

        local_ctx_clt->gc_current_epoch =
            received_message->content.msg_gc_epoch_forward.request_epoch_number;

        replied_message->end_crc = MITSUME_REPLY_CRC;
        wr_id = mitsume_local_thread_get_wr_id(local_inf);
        mitsume_reply_full_message(
            local_ctx_clt->ib_ctx, wr_id, &received_message->msg_header,
            local_inf->input_mr[coro_id], sizeof(mitsume_msg));
        userspace_one_poll(local_ctx_clt->ib_ctx, wr_id,
                           &received_message->msg_header.reply_attr);
        mitsume_local_thread_put_wr_id(local_inf, wr_id);
        // liteapi_reply_message(replied_message, sizeof(struct mitsume_msg),
        // received_descriptor);
      }
    }
    userspace_refill_used_postrecv(local_ctx_clt->ib_ctx,
                                   local_ctx_clt->per_qp_mr_attr_list, input_wc,
                                   1, MITSUME_MAX_MESSAGE_SIZE);
  }
  // mitsume_tool_cache_free(received_message, MITSUME_ALLOCTYPE_MSG);
  // mitsume_tool_cache_free(replied_message, MITSUME_ALLOCTYPE_MSG);
  MITSUME_INFO("exit epoch thread\n");

  return NULL;
}

void *mitsume_tool_gc_stat_thread(void *input_metadata) {
  int coro_id = MITSUME_CLT_TEMP_COROUTINE_ID;
  struct mitsume_consumer_stat_metadata *stat_metadata =
      (struct mitsume_consumer_stat_metadata *)input_metadata;
  struct mitsume_msg *send;
  struct mitsume_msg *recv;
  struct mitsume_ctx_clt *local_ctx_clt = stat_metadata->local_ctx_clt;
  struct thread_local_inf *local_inf = stat_metadata->local_inf;
  int end_flag = 0;
  int each_controller;
  int per_bucket;
  long int tmp_read;
  long int tmp_write;
  int record_flag;
  send = local_inf->input_space[coro_id];
  recv = local_inf->output_space[coro_id];
  send->msg_header.src_id = local_ctx_clt->node_id;
  send->msg_header.type = MITSUME_STAT_UPDATE;
  while (!end_flag) {
    usleep(MITSUME_GC_CLT_LOAD_BALANBING_DELAY_MS * 1000);
    record_flag = 0;
    for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
         per_bucket++) {
      tmp_read = local_ctx_clt->read_bucket_counter[per_bucket].load();
      tmp_write = local_ctx_clt->write_bucket_counter[per_bucket].load();
      if (tmp_read || tmp_write)
        record_flag = 1;
      local_ctx_clt->read_bucket_counter[per_bucket] -= tmp_read;
      local_ctx_clt->write_bucket_counter[per_bucket] -= tmp_write;
      send->content.msg_stat_message.read_bucket_counter[per_bucket] = tmp_read;
      send->content.msg_stat_message.write_bucket_counter[per_bucket] =
          tmp_write;
    }
    if (record_flag) {
      for (each_controller = MITSUME_FIRST_ID;
           each_controller < MITSUME_FIRST_ID + MITSUME_CON_NUM;
           each_controller++) {
        send->msg_header.des_id = each_controller;

        send->msg_header.reply_attr.addr =
            (uint64_t)local_inf->output_mr[coro_id]->addr;
        send->msg_header.reply_attr.rkey = local_inf->output_mr[coro_id]->rkey;
        send->msg_header.reply_attr.machine_id =
            stat_metadata->local_ctx_clt->node_id;

        recv->end_crc = MITSUME_WAIT_CRC;

        // send the request out
        mitsume_send_full_message(
            stat_metadata->local_ctx_clt->ib_ctx, local_inf,
            local_inf->input_mr[coro_id], local_inf->output_mr[coro_id],
            &send->msg_header, stat_metadata->local_ctx_clt->node_id,
            each_controller, sizeof(mitsume_msg));
        if (recv->msg_header.type != MITSUME_STAT_UPDATE_ACK)
          MITSUME_PRINT_ERROR("reply type doesn't match %d\n",
                              recv->msg_header.type);

        // returned_length = liteapi_send_reply_imm(each_controller, port,
        // &send, sizeof(struct mitsume_msg), &recv, sizeof(struct mitsume_msg));
        // if(recv.msg_header.type != MITSUME_STAT_UPDATE_ACK)
        //    MITSUME_ERROR("reply type doesn't match %d\n",
        //    recv.msg_header.type);
      }
    }
  }
  MITSUME_INFO("exit stat thread\n");
  return NULL;
}

int mitsume_tool_gc_init(struct mitsume_ctx_clt *ctx_clt) {
  int i;
  int running_gc_thread_nums = MITSUME_CLT_CONSUMER_GC_THREAD_NUMS;

  // MITSUME_STAT_SET(MITSUME_CURRENT_EPOCH, ctx_clt->gc_current_epoch);
  // init task_struct, queue, lock
  for (i = 0; i < running_gc_thread_nums; i++) {
    ctx_clt->gc_thread_metadata[i].gc_thread_id = i;
    ctx_clt->gc_thread_metadata[i].local_ctx_clt = ctx_clt;
    ctx_clt->gc_thread_metadata[i].gc_allocator_counter = 0;
    ctx_clt->gc_thread_metadata[i].local_inf =
        mitsume_local_thread_setup(ctx_clt->ib_ctx, i);
  }
  // run thread
  for (i = 0; i < running_gc_thread_nums; i++) {
    pthread_create(&ctx_clt->thread_gc_thread[i], NULL,
                   mitsume_tool_gc_running_thread,
                   &ctx_clt->gc_thread_metadata[i]);
  }

  // run epoch thread
  ctx_clt->epoch_thread_metadata.local_ctx_clt = ctx_clt;
  ctx_clt->epoch_thread_metadata.local_inf =
      mitsume_local_thread_setup(ctx_clt->ib_ctx, 0);
  pthread_create(&ctx_clt->thread_epoch, NULL, mitsume_tool_gc_epoch_forwarding,
                 &ctx_clt->epoch_thread_metadata);
  // ctx_clt->thread_epoch = kthread_create((void
  // *)mitsume_tool_gc_epoch_forwarding, &ctx_clt->epoch_thread_metadata,
  // epoch_thread_name);

  for (i = 0; i < MITSUME_NUM_REPLICATION_BUCKET; i++) {
    ctx_clt->read_bucket_counter[i] = 0;
    ctx_clt->write_bucket_counter[i] = 0;
  }

  // run stat thread
  if (MITSUME_TOOL_TRAFFIC_STAT) {
    ctx_clt->stat_thread_metadata.local_ctx_clt = ctx_clt;
    ctx_clt->stat_thread_metadata.local_inf =
        mitsume_local_thread_setup(ctx_clt->ib_ctx, 0);
    pthread_create(&ctx_clt->thread_stat, NULL, mitsume_tool_gc_stat_thread,
                   &ctx_clt->stat_thread_metadata);
  }

  return MITSUME_SUCCESS;
}
