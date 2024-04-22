/**
 * The MapReduce coordinator.
 */

#include "coordinator.h"
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#ifndef SIG_PF
#define SIG_PF void (*)(int)
#endif


/* Global coordinator state. */
coordinator* state;

struct assigned_job {
  int job_id;
  time_t start;
  int task;
  path output_dir;
  char *app;
  int n_reduce;
  int n_map;
  struct args *args;
  char* file;
  bool reduce;
};
extern void coordinator_1(struct svc_req*, SVCXPRT*);

/* Set up and run RPC server. */
int main(int argc, char** argv) {
  register SVCXPRT* transp;

  pmap_unset(COORDINATOR, COORDINATOR_V1);

  transp = svcudp_create(RPC_ANYSOCK);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create udp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_UDP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, udp).");
    exit(1);
  }

  transp = svctcp_create(RPC_ANYSOCK, 0, 0);
  if (transp == NULL) {
    fprintf(stderr, "%s", "cannot create tcp service.");
    exit(1);
  }
  if (!svc_register(transp, COORDINATOR, COORDINATOR_V1, coordinator_1, IPPROTO_TCP)) {
    fprintf(stderr, "%s", "unable to register (COORDINATOR, COORDINATOR_V1, tcp).");
    exit(1);
  }

  coordinator_init(&state);

  svc_run();
  fprintf(stderr, "%s", "svc_run returned");
  exit(1);
  /* NOTREACHED */
}

/* EXAMPLE RPC implementation. */
int* example_1_svc(int* argp, struct svc_req* rqstp) {
  static int result;

  result = *argp + 1;

  return &result;
}

/* SUBMIT_JOB RPC implementation. */
int* submit_job_1_svc(submit_job_request* argp, struct svc_req* rqstp) {
  static int result;

  printf("Received submit job request\n");
  struct stat st;
  if (stat(argp->output_dir, &st) == -1) {
    mkdirp(argp->output_dir);
  }
  /* if app isn't valid */
  app res = get_app(argp->app);
  if (res.name == NULL && res.map == NULL && res.reduce == NULL && res.process_output == NULL) {
    result = -1;
    return &result;
  }

  struct job_info *jb = malloc(sizeof(struct job_info));

  jb->job_id = state->next_job_ID;
  state->next_job_ID += 1;
  
  jb->output_dir = strdup(argp->output_dir);
  jb->app = strdup(argp->app);
 
  jb->files = malloc(sizeof(struct files));
  jb->files->files_len = argp->files.files_len;
  jb->files->files_val = malloc(argp->files.files_len * sizeof(path));

  for (int i = 0; i < argp->files.files_len; i++) {
    jb->files->files_val[i] = strdup(argp->files.files_val[i]);
  }

  jb->n_reduce = argp->n_reduce;

  jb->args = (struct args *) malloc(sizeof(struct args));

  jb->args->args_len = argp->args.args_len;

  jb->args->args_val = malloc(argp->args.args_len + 1);
  memcpy(jb->args->args_val, argp->args.args_val, argp->args.args_len);
  jb->args->args_val[argp->args.args_len] = '\0';


  jb->num_mapped_assigned = 0;
  jb->num_map_completed = 0;
  jb->num_reduce_assigned = 0;
  jb->num_reduce_completed = 0;


  g_queue_push_tail(state->job_queue, jb);

  struct job_info_client *jbc = malloc(sizeof(struct job_info_client));
  jbc->done = false;
  jbc->failed = false;
  g_hash_table_insert(state->hashmap, GINT_TO_POINTER(jb->job_id), jbc);
  result = jb->job_id;
  return &result;

}

/* POLL_JOB RPC implementation. */
poll_job_reply* poll_job_1_svc(int* argp, struct svc_req* rqstp) {
  static poll_job_reply result;
  struct job_info_client* jbc = g_hash_table_lookup(state->hashmap, GINT_TO_POINTER(*argp));
  
  if (jbc == NULL) {
    result.done = false;
    result.failed = false;
    result.invalid_job_id = true;
  } else { 
    result.done = jbc->done;
    result.failed = jbc->failed;
    result.invalid_job_id = false;
  }
  return &result;
}
void update_res(get_task_reply *result, struct job_info *jb) {
  result->job_id = jb->job_id;
  result->output_dir = strdup(jb->output_dir);
  result->app = strdup(jb->app);
  result->n_map = jb->files->files_len;
  result->n_reduce = jb->n_reduce;
  result->args.args_len = jb->args->args_len;
  result->args.args_val = strdup(jb->args->args_val);
}
void clean_assigned_list(int job_id_to_delete) {
  GList *iter;
  struct assigned_job *aj;
  while (iter != NULL) {
    aj = iter->data;
    if (aj->job_id == job_id_to_delete) {
      GList *node_to_remove = iter;
      iter = iter->next; // Move to the next node before removing
      state->assigned_list = g_list_delete_link(state->assigned_list, node_to_remove);
    } else { 
      iter = iter->next;
    }
  }
}
/* GET_TASK RPC implementation. */
void insert_assigned(get_task_reply *result) {
  struct assigned_job *aj = malloc(sizeof(struct assigned_job));
  aj->job_id = result->job_id;
  aj->start = time(NULL);
  aj->task = result->task;
  aj->output_dir = strdup(result->output_dir);
  aj->app = strdup(result->app);
  aj->n_reduce = result->n_reduce;
  aj->n_map = result->n_map;
  aj->args = malloc(sizeof(struct args));
  aj->args->args_len = result->args.args_len;
  aj->args->args_val = strdup(result->args.args_val);
  aj->file = strdup(result->file);
  aj->reduce = result->reduce;
  state->assigned_list = g_list_append(state->assigned_list , aj);
}
get_task_reply* get_task_1_svc(void* argp, struct svc_req* rqstp) {
  static get_task_reply result;
  printf("Received get task request\n");

  GList *iter;
  struct assigned_job *curr;
  
  /* first check if any tasks can be taken up that had previously timed out */
  // for (iter = state->assigned_list; iter != NULL; iter = iter->next) {
  //   curr = iter->data;
  //   if ((time(NULL) - curr->start) > TASK_TIMEOUT_SECS) {
  //     result.job_id = curr->job_id;
  //     result.task = curr->task;
  //     result.file = strdup(curr->file);
  //     result.output_dir = strdup(curr->output_dir);
  //     result.app = strdup(curr->app);
  //     result.n_map = curr->n_map;
  //     result.n_reduce = curr->n_reduce;
  //     result.reduce = curr->reduce;
  //     result.wait = false;
  //     result.args.args_len = curr->args->args_len;
  //     result.args.args_val = strdup(curr->args->args_val);
  //     curr->start = time(NULL);
  //     return &result;
  //   }
  // }

  result.file = "";
  result.output_dir = "";
  result.app = "";
  result.args.args_len = 0;
  struct job_info *jb;
  result.wait = false;
  for (iter = state->job_queue->head; iter != NULL; iter = iter->next) {
    jb = iter->data;
    
    /* if we can process the mapping phase still*/
    if (jb->num_mapped_assigned < jb->files->files_len) {
      result.task = jb->num_mapped_assigned;
      jb->num_mapped_assigned += 1;
      result.file = strdup(jb->files->files_val[result.task]);
      update_res(&result, jb);
      insert_assigned(&result);
      return &result;
    }    
    /* if we are on the reduce phase for this job */
    if (jb->num_map_completed == jb->files->files_len && jb->num_reduce_assigned < jb->n_reduce) {
      result.task = jb->num_reduce_assigned;
      jb->num_reduce_assigned += 1;
      result.file = "";
      update_res(&result, jb);
      insert_assigned(&result);
      return &result;
    }
  }
  result.file = "";
  result.output_dir = "";
  result.app = "";
  result.wait = true;
  result.args.args_len = 0;
  return &result;
}

GList* get_iter(int desired_id) {
  GList *iter;
  struct job_info *jb;
  for (iter = state->job_queue->head; iter != NULL; iter = iter->next) {
    jb = iter->data;
    if (jb->job_id == desired_id) {
      return iter;
    }
  }
  return NULL;
}


GList* get_iter_assigned(int job_id, int task_id) {
  GList *iter;
  struct assigned_job *aj;
  for (iter = state->assigned_list; iter != NULL; iter = iter->next) {
    aj = iter->data;
    if (aj->job_id == job_id && aj->task == task_id) {
      return iter;
    }
  }
}
/* FINISH_TASK RPC implementation. */
void* finish_task_1_svc(finish_task_request* argp, struct svc_req* rqstp) {
  static char* result;
  printf("Received finish task request\n");
  GList *iter = get_iter(argp->job_id);
  /* in the case where a task has already failed for this job */
  if (iter == NULL) {
    return (void*)&result;
  }
  // GList *iter2 = get_iter_assigned(argp->job_id, argp->task);
  // if (iter2) { 
  //   state->assigned_list = g_list_delete_link(state->assigned_list, iter2);
  // } else { 
  //   /* case where we re-assigned this task */
  //   return (void*)&result;
  // }
  struct job_info *jb = iter->data;
  if (!argp->success) {
    struct job_info_client* jbc = g_hash_table_lookup(state->hashmap, GINT_TO_POINTER(jb->job_id));
    jbc->done = true;
    jbc->failed = true;
    /* remove from list of jobs so no other workers get it assigned to them*/
    clean_assigned_list(argp->job_id);
    g_queue_delete_link(state->job_queue, iter);
    g_hash_table_insert(state->hashmap, GINT_TO_POINTER(jb->job_id), jbc);
    return (void*)&result;
  }
  if (jb->num_map_completed != jb->files->files_len) {
    jb->num_map_completed += 1;
  } else if (jb->num_reduce_completed != jb->n_reduce) {
    jb->num_reduce_completed += 1;
  }
  if (jb->num_reduce_completed == jb->n_reduce) {
    /* remove from list*/
    g_queue_delete_link(state->job_queue, iter);
    struct job_info_client* jbc = g_hash_table_lookup(state->hashmap, GINT_TO_POINTER(jb->job_id));
    jbc->done = true;
    jbc->failed = false;
    g_hash_table_insert(state->hashmap, GINT_TO_POINTER(jb->job_id), jbc);
  }
  return (void*)&result;
}
void free_jb_struct(struct job_info *jb) {
  free(jb->output_dir);
  free(jb->app);
  for (int i = 0; i < jb->files->files_len; i++) {
    free(jb->files->files_val[i]);
  }
  free(jb->files);
  free(jb->args->args_val);
  free(jb);
}

/* Initialize coordinator state. */
void coordinator_init(coordinator** coord_ptr) {
  *coord_ptr = malloc(sizeof(coordinator));
  coordinator* coord = *coord_ptr;
  coord->next_job_ID = 0;
  coord->job_queue = g_queue_new();
  coord->hashmap = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
  coord->assigned_list = NULL;
}
