#include "cn.h"

#include <glog/logging.h>
#include <glog/raw_logging.h>
#include <infiniband/verbs.h>

#include "clover/mitsume_clt_test.h"
#include "clover/mitsume_clt_thread.h"

int MITSUME_CLT_NUM;
int MITSUME_MEM_NUM;

DEFINE_int32(clover_machine_id, 1, "Clover's machine id");
DEFINE_int32(clover_ib_dev, 0, "Infiniband device id (start from 0)");
// Base port index of Clover: the port id of clover_ib_dev
DEFINE_int32(clover_ib_port, 1, "Clover's Infiniband port id (start from 1)");
DEFINE_int32(clover_cn, 1, "Number of Clover compute nodes");
DEFINE_int32(clover_dn, 1, "Number of Clover data nodes");
DEFINE_int32(clover_loopback, 2, "Number of loopbacks (?)");
DEFINE_string(clover_memcached_ip, "192.168.223.1", "Memcached IP");

CloverComputeNodeWrapper::CloverComputeNodeWrapper(int workers)
    : global_thread_id_(FLAGS_clover_machine_id << P15_ID_SHIFT),
      local_thread_id_(0),
      ib_dev_idx_(FLAGS_clover_ib_dev),
      ib_port_index_(FLAGS_clover_ib_port),
      num_loopback_(FLAGS_clover_loopback),
      machine_id_(FLAGS_clover_machine_id),
      num_ms_(MITSUME_CON_NUM),
      num_cn_(FLAGS_clover_cn),
      num_dn_(FLAGS_clover_dn),
      workers_(workers),
      clover_ctx_(nullptr),
      ib_inf_(nullptr) {
  MITSUME_CLT_NUM = num_cn_;
  MITSUME_MEM_NUM = num_dn_;

  // MITSUME_CLT_CONSUMER_NUMBER is the maximum number of threads for Clover
  // compute node.
  CHECK_LE(workers_, MITSUME_CLT_CONSUMER_NUMBER);
}

CloverComputeNodeWrapper::~CloverComputeNodeWrapper() {
  if (clover_ctx_ != nullptr) {
    delete clover_ctx_;
  }
  if (ib_inf_ != nullptr) {
    free(ib_inf_);
  }
}

int CloverComputeNodeWrapper::Initialize() {
  configuration_params params = {.global_thread_id = global_thread_id_,
                                 .local_thread_id = local_thread_id_,
                                 .base_port_index = ib_port_index_,
                                 .num_servers = num_ms_,
                                 .num_clients = num_cn_,
                                 .num_memorys = num_dn_,
                                 .is_master = -1,  // dummy value
                                 .machine_id = machine_id_,
                                 // one clover CN thread for each HERD worker
                                 .total_threads = workers_,
                                 .device_id = ib_dev_idx_,
                                 .num_loopback = num_loopback_};
  ib_inf_ = ib_complete_setup(&params, CLIENT, "clover client");
  CHECK_NOTNULL(ib_inf_);

  mitsume_con_alloc_share_init();

  clover_ctx_ = new mitsume_ctx_clt;
  clover_ctx_->all_lh_attr = new ptr_attr[mitsume_con_alloc_get_total_lh()];
  clover_ctx_->ib_ctx = ib_inf_;
  clover_ctx_->node_id = machine_id_;
  clover_ctx_->client_id = get_client_id(&params);

  // thread_metadata is populated in this function
  mitsume_clt_thread_metadata_setup(&params, clover_ctx_);
  LOG(INFO)
      << std::hex
      << clover_ctx_->thread_metadata[0].local_inf->user_input_space[0]
      << std::dec << ' '
      << clover_ctx_->thread_metadata[0].local_inf->user_input_mr[0]->lkey;

  CHECK_EQ(SetupPostRecv(), MITSUME_SUCCESS) << "Failed to setup post_recv";
  CHECK_EQ(GetShortcut(), MITSUME_SUCCESS) << "Failed to get correct shortcut";

  mitsume_con_alloc_get_lh(nullptr, clover_ctx_);
  mitsume_stat_init(MITSUME_ROLE::MITSUME_IS_CLIENT);

  mitsume_tool_lru_init();
  mitsume_tool_gc_init(clover_ctx_);
  return MITSUME_SUCCESS;
}

int CloverComputeNodeWrapper::GetShortcut() {
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
  clover_ctx_->all_shortcut_attr = shortcut_attr;
  MITSUME_PRINT("finish getting shortcut\n");
  return MITSUME_SUCCESS;
}

int CloverComputeNodeWrapper::SetupPostRecv() {
  void *alloc_space;
  ptr_attr *tmp_attr_ptr;
  int per_msg;
  int per_qp;
  uint32_t alloc_size = MITSUME_MAX_MESSAGE_SIZE;
  clover_ctx_->per_qp_mr_attr_list = new ptr_attr *[ib_inf_->num_local_rcqps];
  clover_ctx_->per_post_recv_mr_list =
      new struct ibv_mr *[ib_inf_->num_local_rcqps];

  // register a memory space for each qp
  for (per_qp = 0; per_qp < ib_inf_->num_local_rcqps; per_qp++) {
    clover_ctx_->per_qp_mr_attr_list[per_qp] =
        new ptr_attr[MITSUME_CON_MESSAGE_PER_POST];
    alloc_space = mitsume_malloc(alloc_size * MITSUME_CON_MESSAGE_PER_POST);
    clover_ctx_->per_post_recv_mr_list[per_qp] = ibv_reg_mr(
        ib_inf_->pd, alloc_space, alloc_size * MITSUME_CON_MESSAGE_PER_POST,
        MITSUME_MR_PERMISSION);
    tmp_attr_ptr = clover_ctx_->per_qp_mr_attr_list[per_qp];
    for (per_msg = 0; per_msg < MITSUME_CON_MESSAGE_PER_POST; per_msg++) {
      tmp_attr_ptr[per_msg].addr =
          (uint64_t)clover_ctx_->per_post_recv_mr_list[per_qp]->addr +
          (uint64_t)alloc_size * per_msg;
      tmp_attr_ptr[per_msg].rkey =
          clover_ctx_->per_post_recv_mr_list[per_qp]->rkey;
    }
  }

  // post all memory space into qp
  for (per_qp = 0; per_qp < ib_inf_->num_local_rcqps; per_qp++) {
    ib_post_recv_inf *input_inf =
        new ib_post_recv_inf[MITSUME_CON_MESSAGE_PER_POST];
    for (per_msg = 0; per_msg < MITSUME_CON_MESSAGE_PER_POST; per_msg++) {
      input_inf[per_msg].qp_index = per_qp;
      input_inf[per_msg].length = alloc_size;
      input_inf[per_msg].mr_index = per_msg;
    }
    ib_post_recv_connect_qp(ib_inf_, input_inf,
                            clover_ctx_->per_qp_mr_attr_list[per_qp],
                            MITSUME_CON_MESSAGE_PER_POST);
    free(input_inf);
  }

  return MITSUME_SUCCESS;
}

CloverCnThreadWrapper::CloverCnThreadWrapper(CloverComputeNodeWrapper &cn,
                                             int thread_id)
    : thread_id_(thread_id),
      node_(cn),
      metadata_(cn.clover_ctx_ != nullptr
                    ? &cn.clover_ctx_->thread_metadata[thread_id_]
                    : nullptr) {
  CHECK_NOTNULL(cn.clover_ctx_);

  for (int i = 0; i < MITSUME_CLT_COROUTINE_NUMBER; i++) {
    // Follow the usage in mitsume_benchmark_ycsb, where only the first 4K bytes
    // are used. Buffer is allocated at mitsume_local_thread_setup()
    // (mitsume_util.cc line 209/215).
    rbuf_[i] = static_cast<char *>(metadata_->local_inf->user_output_space[i]);
    fill(rbuf_[i], rbuf_[i] + kBufSize, '\0');
    wbuf_[i] = static_cast<char *>(metadata_->local_inf->user_input_space[i]);
    fill(wbuf_[i], wbuf_[i] + kBufSize, '\0');
  }
}

int CloverCnThreadWrapper::InsertKVPair(mitsume_key key, const void *val,
                                        size_t len) {
  RAW_CHECK(val != nullptr, "Value buffer is null");

  memcpy(wbuf_[0], val, len);
  return mitsume_tool_open(metadata_, key, wbuf_[0], len,
                           MITSUME_NUM_REPLICATION_BUCKET);
}

int CloverCnThreadWrapper::ReadKVPair(mitsume_key key, void *val, uint32_t *len,
                                      size_t maxlen) {
  RAW_CHECK(val != nullptr, "Value buffer is null");
  RAW_CHECK(len != nullptr, "Value size buffer is null");

  int rc = mitsume_tool_read(metadata_, key, rbuf_[0], len,
                             MITSUME_TOOL_KVSTORE_READ);
  if (rc == MITSUME_SUCCESS) {
    if (*len > maxlen) {
      RAW_LOG(WARNING,
              "Provided buffer is too small to fit the value (%lu < %u)",
              maxlen, *len);
      memcpy(val, rbuf_[0], maxlen);
    } else {
      memcpy(val, rbuf_[0], *len);
    }
  }
  return rc;
}

int CloverCnThreadWrapper::WriteKVPair(mitsume_key key, const void *val,
                                       size_t len) {
  RAW_CHECK(val != nullptr, "Value buffer is null");
  memcpy(wbuf_[0], val, len);
  return mitsume_tool_write(metadata_, key, wbuf_[0], len,
                            MITSUME_TOOL_KVSTORE_WRITE);
}