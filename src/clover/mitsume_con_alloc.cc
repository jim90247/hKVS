#include "mitsume_con_alloc.h"

uint32_t MITSUME_CON_ALLOC_BLOCK[MITSUME_CON_ALLOCATOR_SLAB_NUMBER];
const unsigned long long int MIN_LH_IN_ONE_NODE =
    (unsigned long long)MITSUME_MEMORY_PER_ALLOCATION_KB * 1024 /
    MITSUME_MEMORY_LH_CUT_MAX_UNIT;
unsigned long long int MITSUME_PER_NODE_LH;

/**
 * @brief Prefix sum of the size of slab sets.
 *
 * There are multiple sets of slabs with different chunk sizes. Set i are slabs
 * with chunk size MITSUME_CON_ALLOCATOR_SLAB_SIZE_GRANULARITY << i. The number
 * of slabs in i-th set (size of i-th slab set) is max(0, set_lhnum[i] -
 * set_lhnum[i-1]), or set_lhnum[i] if i is 0.
 */
const static unsigned long long set_lhnum[MITSUME_CON_ALLOCATOR_SLAB_NUMBER] = {
    MIN_LH_IN_ONE_NODE / 100 * 20,
    MIN_LH_IN_ONE_NODE / 100 * 20,
    MIN_LH_IN_ONE_NODE / 100 * 20,
    MIN_LH_IN_ONE_NODE / 100 * 20,
    MIN_LH_IN_ONE_NODE / 100 * 98,
    MIN_LH_IN_ONE_NODE / 100 * 98,
    MIN_LH_IN_ONE_NODE / 100 * 98,
    MIN_LH_IN_ONE_NODE / 100 * 98,
    MIN_LH_IN_ONE_NODE};

static unsigned long long real_lhnum[MITSUME_CON_ALLOCATOR_SLAB_NUMBER] = {
    0};  // set by init

unsigned long long int mitsume_con_alloc_get_total_lh() {
  return MITSUME_PER_NODE_LH * MITSUME_MEM_NUM;
}

/**
 * mitsume_con_alloc_rr_get_controller_id - based on key to controller
 * @key: request key
 * return: return controller node id
 */
uint32_t mitsume_con_alloc_rr_get_controller_id(
    struct mitsume_consumer_metadata *thread_metadata) {
  thread_metadata->rr_allocator_counter++;
  if (thread_metadata->rr_allocator_counter == MITSUME_CON_NUM) {
    thread_metadata->rr_allocator_counter = 0;
  }
  return thread_metadata->rr_allocator_counter + MITSUME_FIRST_ID;
  // return MITSUME_FIRST_ID+1;//[TODO] single controller
}

/**
 * mitsume_con_alloc_gc_key_to_controller_id - map key into correct controller
 * @key: request key
 * return: return controller node id
 */
uint32_t mitsume_con_alloc_gc_key_to_gc_thread(mitsume_key key) {
  return key % MITSUME_CLT_CONSUMER_GC_THREAD_NUMS;
}

/**
 * mitsume_con_alloc_key_to_controller_id - map key into correct controller
 * @key: request key
 * return: return controller node id
 */
uint32_t mitsume_con_alloc_key_to_controller_id(mitsume_key key) {
  return key % MITSUME_CON_NUM + MITSUME_FIRST_ID;
  //[TODO] this should map to a controller based on static hash (same for all
  //clients)
}

/**
 * mitsume_con_alloc_size_to_alloc_num - map size into correct list (one lh
 * always maps to same size)
 * @size: request size
 * return: return correct queue number
 */
int mitsume_con_alloc_size_to_list_num(uint32_t size) {
  int i = 0;
  while (size > MITSUME_CON_ALLOC_BLOCK[i]) {
    i++;
  }
  return i;
}

/**
 * mitsume_con_alloc_pointer_to_size - map pointer to correct size based on
 * replication_factor (-sizeof pointer*replication_factor)
 * @lh: lh
 * @replication_factor:
 * return: return correct size
 */
uint32_t mitsume_con_alloc_pointer_to_size(uint64_t pointer,
                                           uint32_t replication_factor) {
  return mitsume_con_alloc_lh_to_size(MITSUME_GET_PTR_LH(pointer)) -
         sizeof(struct mitsume_ptr) * replication_factor - MITSUME_CRC_SIZE;
}

/// Maps the internal lh to slab set id.
int mitsume_con_alloc_internal_lh_to_list_num(uint64_t internal_lh) {
  int i;
  for (i = 0; i < MITSUME_CON_ALLOCATOR_SLAB_NUMBER - 1; i++) {
    if (internal_lh < set_lhnum[i]) {
      break;
    }
  }
  return i;
}

/// Maps the raw lh to slab set id.
int mitsume_con_alloc_lh_to_list_num(uint64_t raw_lh) {
  uint64_t lh = raw_lh % MITSUME_PER_NODE_LH;
  for (int i = 0; i < MITSUME_CON_ALLOCATOR_SLAB_NUMBER; i++) {
    if (lh < real_lhnum[i]) {
      return i;
    }
  }
  die_printf("lh too large - %llu\n", lh);
  return -1;
}

/**
 * mitsume_con_alloc_lh_to_size - map lh to correct size
 * @lh: lh
 * return: return correct size
 */
uint32_t mitsume_con_alloc_lh_to_size(uint64_t lh) {
  return MITSUME_CON_ALLOC_BLOCK[mitsume_con_alloc_lh_to_list_num(lh)];
}

/**
 * mitsume_con_alloc_list_num_to_size - map list_num to correct size
 * @list_num: list_num
 * return: return correct size
 */
uint32_t mitsume_con_alloc_list_num_to_size(int list_num) {
  return MITSUME_CON_ALLOC_BLOCK[list_num];
}

int mitsume_con_alloc_share_init(void) {
  int i;
  MITSUME_INFO("setup allocator entry metadata\n");
  for (i = 0; i < MITSUME_CON_ALLOCATOR_SLAB_NUMBER; i++) {
    MITSUME_CON_ALLOC_BLOCK[i] = MITSUME_CON_ALLOCATOR_SLAB_SIZE_GRANULARITY
                                 << i;
    MITSUME_PRINT("list:size (%d:%d)\n", i, (int)MITSUME_CON_ALLOC_BLOCK[i]);
    if (MITSUME_CON_ALLOC_BLOCK[i] > MITSUME_MEMORY_LH_CUT_MAX_UNIT ||
        MITSUME_MEMORY_LH_CUT_MAX_UNIT % MITSUME_CON_ALLOC_BLOCK[i] != 0) {
      die_printf("error BLOCK size (%d:%d)", MITSUME_CON_ALLOC_BLOCK[i],
                 MITSUME_MEMORY_LH_CUT_MAX_UNIT);
    }
  }

  uint64_t cur_size, cur_offset;
  int tar_list;
  unsigned long long cur_lh_num;
  unsigned long long list_count[MITSUME_CON_ALLOCATOR_SLAB_NUMBER] = {0};
  int per_slab;
  unsigned long long node_lh_count = 0;
  unsigned long long cumulated_count = 0;
  for (cur_lh_num = 0; cur_lh_num < MIN_LH_IN_ONE_NODE; cur_lh_num++) {
    cur_offset = 0;
    tar_list = mitsume_con_alloc_internal_lh_to_list_num(
        cur_lh_num); // get size from set_lh_num
    cur_size = mitsume_con_alloc_list_num_to_size(
        tar_list); // get size from set_lh_num
    while (cur_offset < MITSUME_MEMORY_LH_CUT_MAX_UNIT) {
      cur_offset += cur_size;
      list_count[tar_list]++;
      node_lh_count++;
    }
  }

  for (per_slab = 0; per_slab < MITSUME_CON_ALLOCATOR_SLAB_NUMBER; per_slab++) {
    // MITSUME_PRINT("%d, %d, %llu\n", per_slab,
    // mitsume_con_alloc_list_num_to_size(per_slab), list_count[per_slab]);

    real_lhnum[per_slab] = list_count[per_slab] + cumulated_count;
    cumulated_count += list_count[per_slab];
  }
  MITSUME_PER_NODE_LH = node_lh_count;

  for (per_slab = 0; per_slab < MITSUME_CON_ALLOCATOR_SLAB_NUMBER; per_slab++) {
    MITSUME_PRINT("%d(%d), %llu\n", per_slab,
                  mitsume_con_alloc_list_num_to_size(per_slab),
                  list_count[per_slab]);
  }
  MITSUME_PRINT("total: %llu\n", MITSUME_PER_NODE_LH);

  return MITSUME_SUCCESS;
}

/// Publishes lh information into memcached.
int mitsume_con_alloc_populate_lh(struct mitsume_ctx_con *local_ctx_con) {
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  unsigned long long int i;
  memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
  MITSUME_PRINT("populate lh table to memcached %llu\n",
                (unsigned long long int)MITSUME_PER_NODE_LH);
  for (i = 0; i < MITSUME_MEM_NUM; i++) {
    sprintf(memcached_string, MITSUME_MEMCACHED_LH_STRING, i);
    memcached_publish(memcached_string,
                      &local_ctx_con->all_lh_attr[MITSUME_PER_NODE_LH * i],
                      sizeof(ptr_attr) * MITSUME_PER_NODE_LH);
  }

  // memcached_publish(memcached_string, local_ctx_con->all_lh_attr,
  // sizeof(ptr_attr)*mitsume_con_alloc_get_total_lh());
  return MITSUME_SUCCESS;
}

/// Gets lh information from memcached.
int mitsume_con_alloc_get_lh(struct mitsume_ctx_con *local_ctx_con,
                             struct mitsume_ctx_clt *local_ctx_clt) {
  assert(local_ctx_con || local_ctx_clt);    // one of it should be TRUE
  assert(!(local_ctx_con && local_ctx_clt)); // the other one should be nULL
  char memcached_string[MEMCACHED_MAX_NAME_LEN];
  unsigned long long int i;
  void *tmp_attr;
  /*memset(memcached_string, 0, MEMCACHED_MAX_NAME_LEN);
  sprintf(memcached_string, MITSUME_MEMCACHED_LH_STRING, per_lh);
  tmp_attr = memcached_get_published_size(memcached_string,
  sizeof(ptr_attr)*mitsume_con_alloc_get_total_lh());
  memcpy(local_ctx_con->all_lh_attr, tmp_attr,
  sizeof(ptr_attr)*mitsume_con_alloc_get_total_lh()); free(tmp_attr);*/

  for (i = 0; i < MITSUME_MEM_NUM; i++) {
    sprintf(memcached_string, MITSUME_MEMCACHED_LH_STRING, i);
    tmp_attr = memcached_get_published_size(
        memcached_string, sizeof(ptr_attr) * MITSUME_PER_NODE_LH);
    if (local_ctx_con)
      memcpy(&local_ctx_con->all_lh_attr[i * MITSUME_PER_NODE_LH], tmp_attr,
             sizeof(ptr_attr) * MITSUME_PER_NODE_LH);
    else
      memcpy(&local_ctx_clt->all_lh_attr[i * MITSUME_PER_NODE_LH], tmp_attr,
             sizeof(ptr_attr) * MITSUME_PER_NODE_LH);
    free(tmp_attr);
  }
  MITSUME_PRINT("get lh table from memcached\n");
  return MITSUME_SUCCESS;
}

/**
 * mitsume_con_alloc_split_lh_into_entries - split and divide all available LH
 * into entries
 * @local_ctx_con: controller context
 * return: return 0 for success, -1 for error happens
 */
int mitsume_con_alloc_split_space_into_lh(
    struct mitsume_ctx_con *local_ctx_con) {
  int i;
  uint64_t cur_size, cur_offset, cur_base;
  // int ret;
  // struct mitsume_allocator_entry *new_entry;
  // int count;
  unsigned long long total_mem_count, per_mem_count;
  unsigned long long cur_lh_num, real_lh_num;
  unsigned long long MIN_LH_IN_ONE_NODE =
      (unsigned long long)MITSUME_MEMORY_PER_ALLOCATION_KB * 1024 /
      MITSUME_MEMORY_LH_CUT_MAX_UNIT;
  unsigned long long MAX_LH_IN_ONE_NODE =
      (unsigned long long)MITSUME_MEMORY_PER_ALLOCATION_KB * 1024 /
      MITSUME_MEMORY_LH_CUT_MIN_UNIT;
  int tar_list;
  int record[MITSUME_CON_ALLOCATOR_SLAB_NUMBER];
  unsigned long long list_count[MITSUME_CON_ALLOCATOR_SLAB_NUMBER] = {0};
  int per_slab;
  ptr_attr *memory_cut = new ptr_attr[MAX_LH_IN_ONE_NODE * MITSUME_MEM_NUM];
  // int current_entry = 0;

  // ptr_attr *all_memory_attr;

  memset(record, 0, sizeof(int) * MITSUME_CON_ALLOCATOR_SLAB_NUMBER);
  total_mem_count = 0;
  for (i = 0; i < MITSUME_MEM_NUM; i++) // first cut memory space into lh
  {
    per_mem_count = 0;
    real_lh_num = 0;
    for (cur_lh_num = 0; cur_lh_num < MIN_LH_IN_ONE_NODE; cur_lh_num++) {
      cur_offset = 0;
      cur_base = cur_lh_num * MITSUME_MEMORY_LH_CUT_MAX_UNIT;
      tar_list = mitsume_con_alloc_internal_lh_to_list_num(
          cur_lh_num); // get size from set_lh_num
      cur_size = mitsume_con_alloc_list_num_to_size(
          tar_list); // get size from set_lh_num
      while (cur_offset < MITSUME_MEMORY_LH_CUT_MAX_UNIT) {
        memory_cut[total_mem_count].addr =
            local_ctx_con->all_memory_attr[i].addr + cur_base + cur_offset;
        memory_cut[total_mem_count].rkey =
            local_ctx_con->all_memory_attr[i].rkey;
        memory_cut[total_mem_count].machine_id =
            local_ctx_con->all_memory_attr[i].machine_id;
        cur_offset += cur_size;
        real_lh_num++;
        list_count[tar_list]++;
        per_mem_count++;
        total_mem_count++;
      }
    }
    for (per_slab = 0; per_slab < MITSUME_CON_ALLOCATOR_SLAB_NUMBER;
         per_slab++) {
      MITSUME_PRINT("%d, %d, %llu\n", per_slab,
                    mitsume_con_alloc_list_num_to_size(per_slab),
                    list_count[per_slab]);
    }
  }
  local_ctx_con->all_lh_attr = memory_cut;
  MITSUME_PRINT("per_mem_count:%llu\n", per_mem_count);
  return 0;
}

/**
 * mitsume_con_alloc_pickthread - tell requester or allocater which place should
 * be used for the entry
 * @local_ctx_con: controller context
 * @input_entry: target entry
 * return: return target thread
 */
int mitsume_con_alloc_pickthread_to_put(
    struct mitsume_ctx_con *local_ctx_con,
    struct mitsume_allocator_entry *input_entry) {
  // return 0;//this place should be RR or other algorithms in the future
  return (MITSUME_GET_PTR_LH(input_entry->ptr.pointer)) %
         MITSUME_CON_ALLOCATOR_THREAD_NUMBER;
}

/**
 * mitsume_con_alloc_lh_to_bucket_based_on_predefined_map - map lh to correct
 * node id which is used for replication later this mapping is shared from
 * controller
 * @lh: lh
 * return: return node id
 */
inline int mitsume_con_alloc_lh_to_bucket_based_on_predefined_map(uint64_t lh) {
  return (lh / MITSUME_PER_NODE_LH) % MITSUME_NUM_REPLICATION_BUCKET;
  /*if(mitsume_lh_to_node_id_base)
      return mitsume_lh_to_node_id_base[lh];
  return 0;*/
}

/**
 * mitsume_con_alloc_get_lh_to_node_id_bucket - based on lh to correct bucket
 * lh: lh
 * return: return bucket
 */
uint32_t mitsume_con_alloc_lh_to_node_id_bucket(uint64_t lh) {
  // return 0;
  // uint32_t ret;
  return mitsume_con_alloc_lh_to_bucket_based_on_predefined_map(lh);
}

/**
 * mitsume_con_alloc_put_entry_into_thread - put an available entry into thread
 * @local_ctx_con: controller context
 * @input_entry: target entry (available entry)
 * @target_thread: target thread
 * @tail: true if insert into tail
 * return: return 0 if success. return -1 if error happens
 */
int mitsume_con_alloc_put_entry_into_thread(
    struct mitsume_ctx_con *local_ctx_con,
    struct mitsume_allocator_entry *input_entry, int tail) {
  int target_thread =
      mitsume_con_alloc_pickthread_to_put(local_ctx_con, input_entry);
  int target_list = mitsume_con_alloc_lh_to_list_num(
      MITSUME_GET_PTR_LH(input_entry->ptr.pointer));
  int target_replication_bucket = mitsume_con_alloc_lh_to_node_id_bucket(
      MITSUME_GET_PTR_LH(input_entry->ptr.pointer));
  // struct mitsume_allocator_entry *target_allocator_list =
  // local_ctx_con->thread_metadata[target_thread].allocator_node_branch[target_replication_bucket];
  local_ctx_con->thread_metadata[target_thread]
      .allocator_lock_branch[target_replication_bucket][target_list]
      .lock();
  if (tail)
    local_ctx_con->thread_metadata[target_thread]
        .allocator_node_branch[target_replication_bucket][target_list]
        .push_back(input_entry->ptr.pointer);
  else
    local_ctx_con->thread_metadata[target_thread]
        .allocator_node_branch[target_replication_bucket][target_list]
        .push_front(input_entry->ptr.pointer);

  local_ctx_con->thread_metadata[target_thread]
      .allocator_lock_branch[target_replication_bucket][target_list]
      .unlock();
  return MITSUME_SUCCESS;
}

int mitsume_con_alloc_split_lh_into_entries(
    struct mitsume_ctx_con *local_ctx_con) {

  unsigned long long int cur_lh_num;
  unsigned long long int record[MITSUME_CON_ALLOCATOR_SLAB_NUMBER] = {0};
  struct mitsume_allocator_entry tmp_entry;
  int tar_list;
  int i, per_bucket, per_thread;
  unsigned long long lh_count = 0;

  for (cur_lh_num = 0; cur_lh_num < MITSUME_PER_NODE_LH * MITSUME_MEM_NUM;
       cur_lh_num++) {
    if (cur_lh_num < MITSUME_SMALLEST_LH)
      continue;
    if (cur_lh_num % MITSUME_CON_NUM != local_ctx_con->controller_id)
      continue;
    // tmp_entry = new struct mitsume_allocator_entry;
    tmp_entry.ptr.pointer = mitsume_struct_set_pointer(
        cur_lh_num, 0, MITSUME_ENTRY_MIN_VERSION, 0, 0);
    if (mitsume_con_alloc_put_entry_into_thread(local_ctx_con, &tmp_entry,
                                                MITSUME_CON_ALLOCATOR_PUT_TAIL))
      die_printf("%llu fail to insert\n", tmp_entry.ptr.pointer);
    tar_list = mitsume_con_alloc_lh_to_list_num(cur_lh_num);
    record[tar_list]++;
    assert(tmp_entry.ptr.pointer);
    lh_count++;
    if (cur_lh_num % 1000000 == 0) {
      MITSUME_TOOL_PRINT_POINTER_NULL(&tmp_entry.ptr);
    }
  }
  for (i = 0; i < MITSUME_CON_ALLOCATOR_SLAB_NUMBER; i++) {
    string count_string = "(";

    for (per_bucket = 0; per_bucket < MITSUME_NUM_REPLICATION_BUCKET;
         per_bucket++) {
      uint32_t local_sum = 0;
      char local_use[128];
      for (per_thread = 0; per_thread < MITSUME_CON_ALLOCATOR_THREAD_NUMBER;
           per_thread++)
        local_sum += local_ctx_con->thread_metadata[per_thread]
                         .allocator_node_branch[per_bucket][i]
                         .size();
      sprintf(local_use, "%llu:", (unsigned long long int)local_sum);
      count_string = count_string + local_use;
    }
    count_string = count_string + ")";
    char count_string_array[count_string.length() + 1];
    strcpy(count_string_array, count_string.c_str());
    MITSUME_INFO("size:number (%llu:%llu) %s\n",
                 (unsigned long long int)mitsume_con_alloc_list_num_to_size(i),
                 record[i], count_string_array);
  }
  MITSUME_INFO("hold with %llu:%llu lh\n", lh_count,
               MITSUME_PER_NODE_LH * MITSUME_MEM_NUM);
  return MITSUME_SUCCESS;
}

/**
 * mitsume_con_alloc_put_shortcut_into_list - put available shortcut entry into
 * the list which will be used in the future during receiving OPEN request
 * @local_ctx_con: local controller context
 * return: return success
 */
int mitsume_con_alloc_put_shortcut_into_list(
    struct mitsume_ctx_con *local_ctx_con) {
  unsigned long long int per_sh;
  unsigned long long int available_shortcut = 0;
  struct mitsume_shortcut_entry tmp_entry;
  for (per_sh = MITSUME_SHORTCUT_LH_BASE; per_sh < MITSUME_SHORTCUT_NUM;
       per_sh++) {
    if (per_sh % MITSUME_CON_NUM != local_ctx_con->controller_id)
      continue;
    // cur_offset = 0;
    // while(cur_offset < MITSUME_SHORTCUT_SIZE)
    //{
    tmp_entry.ptr.pointer = mitsume_struct_set_pointer(per_sh, 0, 0, 0, 0);
    local_ctx_con->shortcut_lh_list.push(tmp_entry.ptr.pointer);
    available_shortcut++;
    //}
  }
  MITSUME_INFO("initialize shortcut %llu\n", available_shortcut);
  return MITSUME_SUCCESS;
}

/**
 * mitsume_con_alloc_get_shortcut_from_list - get an available shortcut spzce
 * @thread_metadata: target thread_metadata
 * @output: available shortcut
 * return: return general success
 */
int mitsume_con_alloc_get_shortcut_from_list(
    struct mitsume_allocator_metadata *thread_metadata,
    struct mitsume_shortcut_entry *output) {
  struct mitsume_ctx_con *local_ctx_con = thread_metadata->local_ctx_con;
  local_ctx_con->shortcut_lock.lock();
  if (local_ctx_con->shortcut_lh_list.empty()) {
    local_ctx_con->shortcut_lock.unlock();
    MITSUME_PRINT_ERROR("Shortcut is not enough\n");
    return MITSUME_ERROR;
  }
  output->ptr.pointer = local_ctx_con->shortcut_lh_list.front();
  local_ctx_con->shortcut_lh_list.pop();
  local_ctx_con->shortcut_lock.unlock();

  return MITSUME_SUCCESS;
}

/**
 * mitsume_con_alloc_entry_init - initialization of MITSUME_CON_ALLOC_ENTRY
 * this function should be called no matter this node is a client or a
 * controller after calling this function, consumer can start getting available
 * entries from controller return: return 0 success, -1 error
 */
int mitsume_con_alloc_entry_init(struct configuration_params *input_arg,
                                 struct mitsume_ctx_con *server_ctx) {
  mitsume_con_alloc_split_lh_into_entries(server_ctx);
  mitsume_con_alloc_put_shortcut_into_list(server_ctx);

  return MITSUME_SUCCESS;
}
