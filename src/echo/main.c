#define _GNU_SOURCE

#include "main.h"

#include <getopt.h>

#include "hrd.h"
#include "mica.h"

int main(int argc, char* argv[]) {
  /* Use small queues to reduce cache pressure */
  assert(HRD_Q_DEPTH == 128);

  /* All requests should fit into the master's request region */
  assert(sizeof(struct mica_op) * NUM_CLIENTS * NUM_WORKERS * WINDOW_SIZE <=
         RR_SIZE);

  /* Unsignaled completion checks. worker.c does its own check w/ @postlist */
  assert(UNSIG_BATCH >= WINDOW_SIZE);     /* Pipelining check for clients */
  assert(HRD_Q_DEPTH >= 2 * UNSIG_BATCH); /* Queue capacity check */

  int i, c;
  int is_master = -1;
  int num_threads = -1;
  int postlist = -1;
  int base_port_index = -1, num_server_ports = -1, num_client_ports = -1;
  struct thread_params* param_arr;
  pthread_t* thread_arr;

  static struct option opts[] = {
      {.name = "master", .has_arg = 1, .val = 'M'},
      {.name = "num-threads", .has_arg = 1, .val = 't'},
      {.name = "base-port-index", .has_arg = 1, .val = 'b'},
      {.name = "num-server-ports", .has_arg = 1, .val = 'N'},
      {.name = "num-client-ports", .has_arg = 1, .val = 'n'},
      {.name = "postlist", .has_arg = 1, .val = 'p'},
      {0}};

  /* Parse and check arguments */
  while (1) {
    c = getopt_long(argc, argv, "M:t:b:N:n:m:p", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'M':
        is_master = atoi(optarg);
        assert(is_master == 1);
        break;
      case 't':
        num_threads = atoi(optarg);
        break;
      case 'b':
        base_port_index = atoi(optarg);
        break;
      case 'N':
        num_server_ports = atoi(optarg);
        break;
      case 'n':
        num_client_ports = atoi(optarg);
        break;
      case 'p':
        postlist = atoi(optarg);
        break;
      default:
        printf("Invalid argument %d\n", c);
        exit(EXIT_FAILURE);
    }
  }

  /* Common checks for all (master, workers, clients */
  assert(base_port_index >= 0 && base_port_index <= 8);
  assert(num_server_ports >= 1 && num_server_ports <= 8);

  /* Handle the master process specially */
  if (is_master == 1) {
    struct thread_params master_params;
    master_params.num_server_ports = num_server_ports;
    master_params.base_port_index = base_port_index;

    pthread_t master_thread;
    pthread_create(&master_thread, NULL, run_master, (void*)&master_params);
    pthread_join(master_thread, NULL);
    exit(0);
  }

  /* Server does not need to know number of client ports */
  assert(num_threads == -1); /* Number of server threads is fixed */
  num_threads = NUM_WORKERS; /* Needed to allocate thread structs later */
  assert(machine_id == -1);
  assert(update_percentage == -1);

  assert(postlist >= 1);

  /* Launch a single server thread or multiple client threads */
  printf("main: Using %d threads\n", num_threads);
  param_arr = malloc(num_threads * sizeof(struct thread_params));
  thread_arr = malloc(num_threads * sizeof(pthread_t));

  for (i = 0; i < num_threads; i++) {
    param_arr[i].postlist = postlist;
    param_arr[i].id = i;
    param_arr[i].base_port_index = base_port_index;
    param_arr[i].num_server_ports = num_server_ports;
    param_arr[i].num_client_ports = num_client_ports;
    pthread_create(&thread_arr[i], NULL, run_worker, &param_arr[i]);
  }

  for (int i = 0; i < num_threads; i++) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i], sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      fprintf(stderr, "Failed to pin thread to core %d\n", i);
      exit(EXIT_FAILURE);
    }
  }

  for (i = 0; i < num_threads; i++) {
    pthread_join(thread_arr[i], NULL);
  }

  return 0;
}
