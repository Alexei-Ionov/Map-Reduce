/**
 * The MapReduce coordinator.
 */

#include "coordinator.h"

#ifndef SIG_PF
#define SIG_PF void (*)(int)
#endif

/* Global coordinator state. */
coordinator* state;

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

  /* TODO */

  /* Do not modify the following code. */
  /* BEGIN */
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
  /* initalize list array of files in job info struct*/
  jb->files = (struct files*) malloc(sizeof(argp->files));
  jb->files->files_len = argp->files.files_len;
  for (int i = 0; i < argp->files.files_len; i++) {
    jb->files->files_val[i] = strdup(argp->files.files_val[i]);
  }
  jb->n_reduce = argp->n_reduce;
  jb->args = (struct args *) malloc(sizeof(argp->args));
  jb->args->args_len = argp->args.args_len;
  jb->args->args_val = strdup(argp->args.args_val);

  jb->num_mapped_assigned = 0;
  jb->num_map_completed = 0;
  jb->num_reduce_assigned = 0;
  jb->num_reduce_completed = 0;
  jb->failed_list = NULL;

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

  printf("Received poll job request\n");
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
  struct {
    u_int args_len;
    char *args_val;
  } args;
  args.args_len = jb->args->args_len;
  args.args_val = strdup(jb->args->args_val);
}
/* GET_TASK RPC implementation. */
get_task_reply* get_task_1_svc(void* argp, struct svc_req* rqstp) {
  static get_task_reply result;

  printf("Received get task request\n");
  result.file = "";
  result.output_dir = "";
  result.app = "";
  result.wait = true;
  result.args.args_len = 0;
  /* if no tasks can be serviced from empty queue then just return with wait set to true */
  if (g_queue_is_empty(state->job_queue)) {
    return &result;
  }
  GList *iter;
  struct job_info *jb;
  result.wait = false;
  for (iter = state->job_queue->head; iter != NULL; iter = iter->next) {
    jb = iter->data;
    update_res(&result, jb);
    /* if we can process the mapping phase still*/
    if (jb->num_mapped_assigned < jb->files->files_len || jb->num_map_completed < jb->files->files_len && g_list_length(jb->failed_list)) {
      /* if we have more mapping to assign*/
      if (jb->num_mapped_assigned < jb->files->files_len) {
        result.task = jb->num_mapped_assigned;
        jb->num_mapped_assigned += 1;
      } else { 
        int *task = g_list_nth_data(jb->failed_list, 0);
        g_list_remove(jb->failed_list, task);
        // int *task = g_list_remove(jb->failed_list, g_list_first(jb->failed_list));
        result.task = *task;
      }
      result.file = strdup(jb->files->files_val[result.task]);
      result.reduce = false;
      return &result;
    }    
    /* if we are on the reduce phase for this job */
    if (jb->num_map_completed == jb->files->files_len) {
      if (jb->num_reduce_assigned < jb->n_reduce) {
        result.task = jb->num_mapped_assigned;
        result.reduce = true;
        jb->num_mapped_assigned += 1;
        return &result;
      }
      /* if there exists any failed workers. note that we don't need to check whether completed is less than since at this point we are only concerned with hte reduction phase*/
      if (g_list_length(jb->failed_list)) {
        int *task = g_list_nth_data(jb->failed_list, 0);
        g_list_remove(jb->failed_list, task);
        // int *task = g_list_remove(jb->failed_list, g_list_first(jb->failed_list));
        result.task = *task;
        result.reduce = true;
        return &result;
      }
    }
  }
  result.wait = true;
  return &result;
}

/* FINISH_TASK RPC implementation. */
void* finish_task_1_svc(finish_task_request* argp, struct svc_req* rqstp) {
  static char* result;

  printf("Received finish task request\n");
  GList *iter;
  struct job_info *jb;
  for (iter = state->job_queue->head; iter != NULL; iter = iter->next) {
    jb = iter->data;
    if (jb->job_id == argp->job_id) {
      /* if task finished successfully, update counts*/
      if (argp->success) {
        if (jb->num_map_completed != jb->files->files_len) {
          jb->num_map_completed += 1;
        } else if (jb->num_reduce_completed != jb->n_reduce) {
          jb->num_reduce_completed += 1;
        }
      /* need to add task to failed list*/
      } else { 
        g_list_append(jb->failed_list, GINT_TO_POINTER(argp->task));
      }
      if (jb->num_reduce_completed == jb->n_reduce) {
        /* remove from list*/
        g_queue_delete_link(state->job_queue, iter);
        // free_jb_struct(jb);
      }
      break;
    }
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
}
