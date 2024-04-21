/**
 * The MapReduce coordinator.
 */

#ifndef H1_H__
#define H1_H__
#include "../rpc/rpc.h"
#include "../lib/lib.h"
#include "../app/app.h"
#include "job.h"
#include <glib.h>
#include <stdio.h>
#include <stdbool.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdlib.h>
#include <rpc/pmap_clnt.h>
#include <string.h>
#include <memory.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/stat.h>

struct job_info_client {
  bool done;           /* Job has completed. */
  bool failed;         /* Job has failed. */
};

struct args {
  u_int args_len;
	char *args_val;
};
struct files {
  u_int files_len;
	path *files_val;
};
struct job_info {
  int job_id;
  path output_dir;
  char *app;
  int n_reduce;
  struct args *args;
  struct files *files;
  int num_mapped_assigned;
  int num_map_completed;
  int num_reduce_assigned;
  int num_reduce_completed;
};
typedef struct {
  int next_job_ID;
  GQueue *job_queue;
  GHashTable *hashmap;
  GList *assigned_list;
} coordinator;

void coordinator_init(coordinator** coord_ptr);
#endif
