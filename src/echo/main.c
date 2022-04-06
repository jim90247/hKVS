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
  int postlist = 1;
  int base_port_index = 0, num_server_ports = 1;
  struct thread_params* param_arr;
  pthread_t* thread_arr;

  static struct option opts[] = {
      {.name = "base-port-index", .has_arg = 1, .val = 'b'},
      {.name = "num-server-ports", .has_arg = 1, .val = 'N'},
      {.name = "postlist", .has_arg = 1, .val = 'p'},
      {0}};

  /* Parse and check arguments */
  while (1) {
    c = getopt_long(argc, argv, "b:N:p:", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'b':
        base_port_index = atoi(optarg);
        break;
      case 'N':
        num_server_ports = atoi(optarg);
        break;
      case 'p':
        postlist = atoi(optarg);
        break;
      default:
        printf("Invalid argument %d\n", c);
        exit(EXIT_FAILURE);
    }
  }
  fprintf(stderr, "postlist=%d\n", postlist);

  /* Common checks for all (master, workers, clients */
  assert(base_port_index >= 0 && base_port_index <= 8);
  assert(num_server_ports >= 1 && num_server_ports <= 8);

  /* Handle the master process specially */
  struct thread_params master_params;
  master_params.num_server_ports = num_server_ports;
  master_params.base_port_index = base_port_index;

  pthread_t master_thread;
  pthread_create(&master_thread, NULL, run_master, (void*)&master_params);

  // Give the master process time to create and register per-port request
  // regions
  sleep(2);

  assert(machine_id == -1);
  assert(update_percentage == -1);

  assert(postlist >= 1);

  /* Launch a single server thread or multiple client threads */
  printf("main: Using %d threads\n", NUM_WORKERS);
  param_arr = malloc(NUM_WORKERS * sizeof(struct thread_params));
  thread_arr = malloc(NUM_WORKERS * sizeof(pthread_t));

  for (i = 0; i < NUM_WORKERS; i++) {
    param_arr[i].postlist = postlist;
    param_arr[i].id = i;
    param_arr[i].base_port_index = base_port_index;
    param_arr[i].num_server_ports = num_server_ports;
    pthread_create(&thread_arr[i], NULL, run_worker, &param_arr[i]);
  }

  for (int i = 0; i < NUM_WORKERS; i++) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i], sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      fprintf(stderr, "Failed to pin thread to core %d\n", i);
      exit(EXIT_FAILURE);
    }
  }

  for (i = 0; i < NUM_WORKERS; i++) {
    pthread_join(thread_arr[i], NULL);
  }
  pthread_join(master_thread, NULL);

  return 0;
}
