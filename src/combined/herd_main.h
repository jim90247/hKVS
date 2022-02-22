#include <cstdint>

#include "clover/mitsume_struct.h"
#include "libhrd/hrd.h"
#include "mica/mica.h"

/*
 * The polling logic in HERD requires the following:
 * 1. 0 < MICA_OP_GET < MICA_OP_PUT < HERD_OP_GET < HERD_OP_PUT
 * 2. HERD_OP_GET = MICA_OP_GET + HERD_MICA_OFFSET
 * 3. HERD_OP_PUT = MICA_OP_PUT + HERD_MICA_OFFSET
 *
 * This allows us to detect HERD requests by checking if the request region
 * opcode is more than MICA_OP_PUT. And then we can convert a HERD opcode to
 * a MICA opcode by subtracting HERD_MICA_OFFSET from it.
 */
constexpr int HERD_MICA_OFFSET = 10;
constexpr int HERD_OP_GET = MICA_OP_GET + HERD_MICA_OFFSET;
constexpr int HERD_OP_PUT = MICA_OP_PUT + HERD_MICA_OFFSET;

constexpr int HERD_NUM_BKTS = 2 * 1024 * 1024;
constexpr int HERD_LOG_CAP = 1024 * 1024 * 1024;

constexpr int HERD_NUM_KEYS = 8 * 1024 * 1024;
constexpr int HERD_VALUE_SIZE = 32;
/// Send/write can be inlined if the value size does not exceed this value.
constexpr int kInlineCutOff =
    HRD_MAX_INLINE - (sizeof(mica_key) + MICA_OBJ_METADATA_SIZE);

/* Request sizes */
constexpr int HERD_GET_REQ_SIZE = 16 + 1 + 1; /* 16 byte key + opcode + seq */

/* Key, metadata, val */
constexpr int HERD_PUT_REQ_SIZE = 16 + MICA_OBJ_METADATA_SIZE + HERD_VALUE_SIZE;

/* Configuration options */
constexpr int MAX_SERVER_PORTS = 4;
constexpr int NUM_WORKERS = 24;
/// Total number of HERD client threads.
constexpr int NUM_CLIENTS = 72;

/* Performance options */
constexpr int WINDOW_SIZE = 64; /* Outstanding requests kept by each client */
constexpr int NUM_UD_QPS = 1;   /* Number of UD QPs per port */
constexpr int USE_POSTLIST = 1;
static_assert(WINDOW_SIZE <= 256);  // fit in mica_op::seq

constexpr int UNSIG_BATCH = 64; /* XXX Check if increasing this helps */
constexpr int UNSIG_BATCH_ = UNSIG_BATCH - 1;

/* SHM key for the 1st request region created by master. ++ for other RRs.*/
constexpr int MASTER_SHM_KEY = 24;
constexpr int RR_SIZE = 16 * 1024 * 1024; /* Request region size */
constexpr int Offset(int wn, int cn, int ws) {
  return (wn * NUM_CLIENTS * WINDOW_SIZE) + (cn * WINDOW_SIZE) + ws;
}
struct herd_thread_params {
  int id;
  int base_port_index;
  int num_server_ports;
  int num_client_ports;
  uint32_t update_percentage;
  int postlist;
};

/* Use small queues to reduce cache pressure */
static_assert(HRD_Q_DEPTH == 128);

/* All requests should fit into the master's request region */
static_assert(sizeof(mica_op) * NUM_CLIENTS * NUM_WORKERS * WINDOW_SIZE <=
              RR_SIZE);

/* Unsignaled completion checks. worker.c does its own check w/ @postlist */
static_assert(UNSIG_BATCH >= WINDOW_SIZE); /* Pipelining check for clients */
static_assert(HRD_Q_DEPTH >= 2 * UNSIG_BATCH); /* Queue capacity check */

static_assert(HERD_VALUE_SIZE <= MICA_MAX_VALUE);

enum HerdResponseCode : unsigned int { kNormal = 0, kOffloaded = 1 };

/* NOTE: although each herd worker uses same set of keys (0~HERD_NUM_KEYS-1),
 * they should be treated as different keys when inserting them into Clover.
 *
 * A possible workaround is to mask out some hash bits and use those fields to
 * store thread id.
 */
/**
 * @brief Converts HERD key hash of specific worker thread to the key used in
 * Clover by replacing the lowest 8 bits with the worker thread id.
 *
 * @param herd_key the key used in HERD
 * @param tid the id of HERD worker thread
 * @return the corresponding key to query in Clover
 */
inline mitsume_key ConvertHerdKeyToCloverKey(mica_key *herd_key, uint8_t tid) {
  mitsume_key clover_key = reinterpret_cast<uint64 *>(herd_key)[1];
  clover_key &= ~((1UL << 8) - 1UL);  // mask out lowest 8 bits
  return (clover_key | tid);
}

/**
 * @brief Converts plain-text keys to HERD key hash.
 *
 * @param plain_key the plain-text key which is a number in range [0,
 * HERD_NUM_KEYS)
 * @return the HERD key hash
 */
inline uint128 ConvertPlainKeyToHerdKey(int plain_key) {
  return CityHash128_High64((char *)&plain_key, sizeof(int));
}
