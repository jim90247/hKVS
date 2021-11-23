#include <cstdint>

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

/* Request sizes */
constexpr int HERD_GET_REQ_SIZE = 16 + 1; /* 16 byte key + opcode */

/* Key, op, len, val */
constexpr int HERD_PUT_REQ_SIZE = 16 + 1 + 1 + HERD_VALUE_SIZE;

/* Configuration options */
constexpr int MAX_SERVER_PORTS = 4;
constexpr int NUM_WORKERS = 8;
constexpr int NUM_CLIENTS = 16;

/* Performance options */
constexpr int WINDOW_SIZE = 32; /* Outstanding requests kept by each client */
constexpr int NUM_UD_QPS = 1;   /* Number of UD QPs per port */
constexpr int USE_POSTLIST = 1;

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
  int update_percentage;
  int postlist;
};
