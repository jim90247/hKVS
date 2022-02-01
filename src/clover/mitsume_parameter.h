#ifndef MITSUME_PARAMETER_HEADER
#define MITSUME_PARAMETER_HEADER
#include <cstdint>
#define MITSUME_MR_PERMISSION                                                  \
  (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | \
   IBV_ACCESS_REMOTE_ATOMIC)

#define MITSUME_PTR_MASK_LH 0x0ffffffff0000000
//#define MITSUME_PTR_MASK_OFFSET                 0x0000fffff8000000
#define MITSUME_PTR_MASK_NEXT_VERSION 0x0000000007f80000
#define MITSUME_PTR_MASK_ENTRY_VERSION 0x000000000007f800
#define MITSUME_PTR_MASK_XACT_AREA 0x00000000000007fe
#define MITSUME_PTR_MASK_OPTION 0x0000000000000001

#define MITSUME_GET_PTR_LH(A) (A & MITSUME_PTR_MASK_LH) >> 28
//#define MITSUME_GET_PTR_OFFSET(A) ((A&MITSUME_PTR_MASK_OFFSET)>>27)<<7 //it
//should be 27 bit. However, the granularity is 128B which is 7 B -->27-7 = 20
#define MITSUME_GET_PTR_NEXT_VERSION(A)                                        \
  (A & MITSUME_PTR_MASK_NEXT_VERSION) >> 19
#define MITSUME_GET_PTR_ENTRY_VERSION(A)                                       \
  (A & MITSUME_PTR_MASK_ENTRY_VERSION) >> 11
#define MITSUME_GET_PTR_XACT_AREA(A) (A & MITSUME_PTR_MASK_XACT_AREA) >> 1
#define MITSUME_GET_PTR_OPTION(A) (A & MITSUME_PTR_MASK_OPTION)

#define MITSUME_SET_PTR_LH(A) ((((uint64_t)A) << 28) & MITSUME_PTR_MASK_LH)
//#define MITSUME_SET_PTR_OFFSET(A)             ((((uint64_t)A) << 20) &
//MITSUME_PTR_MASK_OFFSET) #define MITSUME_SET_PTR_OFFSET(A)
//((((uint64_t)A>>7)<<27)& MITSUME_PTR_MASK_OFFSET)
#define MITSUME_SET_PTR_NEXT_VERSION(A)                                        \
  ((((uint64_t)A) << 19) & MITSUME_PTR_MASK_NEXT_VERSION)
#define MITSUME_SET_PTR_ENTRY_VERSION(A)                                       \
  ((((uint64_t)A) << 11) & MITSUME_PTR_MASK_ENTRY_VERSION)
#define MITSUME_SET_PTR_XACT_AREA(A)                                           \
  ((((uint64_t)A) << 1) & MITSUME_PTR_MASK_XACT_AREA)
#define MITSUME_SET_PTR_OPTION(A) (((uint64_t)A) & MITSUME_PTR_MASK_OPTION)

///////////////////////////////

//#define MITSUME_TOOL_DEBUG

#define MITSUME_I64_MAX 9223372036854775807
#define MITSUME_DEFAULT_WR_ID 3378

#define MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT 20
#define MITSUME_TOOL_QUERY_HASHTABLE_SIZE_BIT                                  \
  MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT
#define MITSUME_CON_GC_HASHTABLE_SIZE_BIT MITSUME_CON_NAMER_HASHTABLE_SIZE_BIT

#define MITSUME_CON_ALLOCATOR_PUT_HEAD 0
#define MITSUME_CON_ALLOCATOR_PUT_TAIL 1
#define MITSUME_CON_THREAD_ADD_TO_HEAD 0
#define MITSUME_CON_THREAD_ADD_TO_TAIL 1

//#define MITSUME_CON_GC_ADD_TO_HEAD

/*
 * Yizhou HACK!!!
 */
//#define MITSUME_NUM_REPLICATION_BUCKET MITSUME_MEM_NUM
#define MITSUME_NUM_REPLICATION_BUCKET 1

#define MITSUME_MAX_REPLICATION MITSUME_NUM_REPLICATION_BUCKET
//#define MITSUME_MAX_REPLICATION 3
//#define MITSUME_MAX_REPLICATION 1
#define MITSUME_REPLICATION_PRIMARY 0

// Number of GC threads at metadata server
constexpr int MITSUME_CON_GC_THREAD_NUMBER = 4;
// Number of controller threads at metadata server
constexpr int MITSUME_CON_ALLOCATOR_THREAD_NUMBER = 4;
#define MITSUME_CON_MESSAGE_PER_POST 128

#define MITSUME_CON_ASYNC_MESSAGE_POLL_SIZE 12

constexpr int MITSUME_CON_ALLOCATOR_SLAB_NUMBER = 9;
constexpr int MITSUME_CON_ALLOCATOR_SLAB_SIZE_GRANULARITY = 64;
#define MITSUME_CON_ALLOCATOR_BLOCK_TO_ENTRY_RATIO 1

#define MITSUME_CLT_USER_FILE_MAX_SIZE (4096 + 512)

#define MITSUME_ENTRY_MIN_VERSION 1

#define MITSUME_CLT_MAX_WRID_BATCH 128

#define MITSUME_MASTER_CON 0

#define MITSUME_FIRST_ID 0

#define MITSUME_CON_NUM 1
extern int MITSUME_CLT_NUM;
extern int MITSUME_MEM_NUM;

constexpr int MITSUME_MAX_KEYS = 100000;
#define MITSUME_ENTRY_NUM (1024 * 16)
#define MITSUME_MEMORY_PER_ALLOCATION_KB (1024 * 1024 * 4) // 4GB
#define MITSUME_MEMORY_LH_CUT_MAX_UNIT (1024 * 32)
#define MITSUME_MEMORY_LH_CUT_MIN_UNIT                                         \
  (MITSUME_CON_ALLOCATOR_SLAB_SIZE_GRANULARITY)
//#define MITSUME_CON_ASKLH_SIZE 4194304
//#define MITSUME_SHORTCUT_LH_NUM 16
constexpr int MITSUME_SHORTCUT_NUM = MITSUME_MAX_KEYS * 4;
#define MITSUME_SHORTCUT_LH_BASE 128

#define MITSUME_CON_BACKUP_NUM 0
#define MITSUME_GC_CON_SLEEP_TIME 10
#define MITSUME_GC_CON_PER_PROGRESS 64

#define MITSUME_TOOL_CHASING_THRESHOLD 4
#define MITSUME_TOOL_ASKING_THRESHOLD 7

#define MITSUME_CLT_CONSUMER_PER_ASK_NUMS 128
#define MITSUME_CLT_CONSUMER_MAX_ENTRY_NUMS MITSUME_CLT_CONSUMER_PER_ASK_NUMS
#define MITSUME_CLT_CONSUMER_MAX_GC_NUMS 128
// Max number of consumer threads (submit open/read/write reqs) at one client
constexpr int MITSUME_CLT_CONSUMER_NUMBER = 8;
#define MITSUME_CLT_CONSUMER_GC_THREAD_NUMS 4
#define MITSUME_CLT_CONSUMER_MAX_SHORTCUT_UPDATE_NUMS 128
#define MITSUME_GC_MAX_BACK_UPDATE_PER_ENTRY 1
#define MITSUME_CLT_COROUTINE_NUMBER 20
#define MITSUME_CLT_TEMP_COROUTINE_ID 0

#define MITSUME_TOOL_LOAD_BALANCING_NOENTRY -1
#define MITSUME_WAIT_CRC 0x8BADF00D
#define MITSUME_REPLY_CRC 0xCAFEBABE

#define MITSUME_CLT_GET_ENTRY_RETRY_TIMEOUT 10
#define MITSUME_CON_GC_THRESHOLD_TIMEOUT 10
#define MITSUME_CON_MANAGEMENT_SLEEP_TIMEOUT 10

#define MITSUME_SMALLEST_LH 1
#define MITSUME_TEST_ONE 1
#define MITSUME_TEST_THREE 3

#define MITSUME_DISABLE_CRC
#ifdef MITSUME_DISABLE_CRC
#define MITSUME_CRC_SIZE 0
#else
#define MITSUME_CRC_SIZE 8
#endif

#define MITSUME_TOOL_LOAD_BALANCING_MODE 0
#define MITSUME_TOOL_READ_LOAD_BALANCING_MODE 0
#define MITSUME_TOOL_TRAFFIC_STAT 0
#define MITSUME_GC_CLT_LOAD_BALANBING_DELAY_MS 100

#define MITSUME_GC_EPOCH_INITIAL 1
#define MITSUME_GC_EPOCH_THRESHOLD 2
#define MITSUME_GC_EPOCH_STEPSIZE 1
#define MITSUME_GC_CON_EPOCH_TRIGGER_SIZE 1024
#define MITSUME_GC_CON_EPOCH_SLEEP_TIME 10
#define MITSUME_GC_CON_SLEEP_TIME 10
#define MITSUME_GC_CLT_EPOCH_DELAY 10
#define MITSUME_GC_WRAPUP_VERSION 254

#define MITSUME_POLLING_SLEEP_TIME 10

#define MITSUME_TOOL_MAX_IB_SGE_SIZE 4

// Use asynchronous send when asking more entries for write
constexpr bool kAskEntryAsync = false;

//#define MITSUME_ENABLE_FIFO_LRU_QUEUE
#define MITSUME_LRU_SIZE 100000
// if we want to test LRU_SIZE, disable local query cache directly
#define MITSUME_LRU_DEFAULT_UNUSED_VALUE 3378
#define MITSUME_LRU_BUCKET_BIT 8
#define MITSUME_LRU_PER_QUEUE_SIZE                                             \
  (MITSUME_LRU_SIZE / (1 << MITSUME_LRU_BUCKET_BIT))

//#define MITSUME_DISABLE_LOCAL_QUERY_CACHE
#ifdef MITSUME_DISABLE_LOCAL_QUERY_CACHE
#define MITSUME_DEFAULT_REPLICATION_NUM 1
#endif
//#define MITSUME_ENABLE_READ_VERIFICATION
#define MITSUME_GC_MEASURE_TIME_FLAG 0
//#define MITSUME_WRITE_OPT_DISABLE_LOCAL_CACHE_UPDATE
#define MITSUME_GC_SLEEP_TWO_RTT

#endif
