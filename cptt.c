#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#include <wait.h>
#include <rados/librados.h>
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <string.h>
#include <jansson.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define OPERATION_WRITE 1
#define OPERATION_READ 2
#define OPERATION_DELETE 3

#define DEBUG 1
#define INFO 1 

#define print_debug(fmt,...) do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)
#define print_info(fmt,...) do { if (INFO) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

typedef struct {
    char *mon_host;
    char *cluster_name;
    char *user;
    char *key;
    char *pool_name;
} ceph_conf_t;

typedef struct {
    int node_id;
    int work_id;
    int thread_id;
    unsigned long long object_id;
    struct timeval start_time;
    struct timeval stop_time;
    int  op_type;
    long duration;
    size_t object_size;
    int status;
} operation_statistics_t;

typedef struct op_stat_list_t {
    struct op_stat_list_t *prev;
    struct op_stat_list_t *next;
    operation_statistics_t *op_stat;
} op_stat_list_t;

typedef struct {
  int thread_count;
  int total_ops;
  int op_type;
  size_t object_size;
} workload_definition_t;

typedef struct {
    int node_id;
    int work_id;
    int thread_id;
    ceph_conf_t *ceph_conf;
    pthread_t worker_thread;
    int launched;
    int stop;
    int stopped;
    unsigned long long completed_ops;
    int op_type;
    size_t object_size;
    int start_at;
    int op_stat_bus;
    int mgmt_bus;
    op_stat_list_t *ops_stats_link_list;
} worker_info_t;

typedef struct {
    workload_definition_t workload_def;
    int work_completed;
    worker_info_t **workers_info;
} work_info_t;    

typedef struct {
    int node_id;
    ceph_conf_t ceph_conf;
    pthread_mutex_t test_launched_lock;
    pthread_t listen_mgmt_requests_thread;
    int port_number;
    int start_at;
    int shutdown;
    int start_test;
    int test_canceled;
    int works_count;
    work_info_t **works_info;
} global_config_t;

typedef struct {
    int sock;
    global_config_t *cptt;
} request_struct_t;

/*****
 * Functions definitions
 *****/
void collect_operations_statistics(void *worker_info_void);
int save_test_results(global_config_t *cptt);
int delete_object(rados_ioctx_t io_ctx, char* object_name, size_t object_size, int result_bus, int work_id, int thread_id);
void start_worker_wrapper(void *worker_info_void);
void start_worker(worker_info_t *worker_info);
void listen_mgmt_requests(void *cptt_void);
int pars_workload_definition(char *root, global_config_t *cptt);
void process_mgmt_request(void *request_struct_void);
int read_object(rados_ioctx_t io_ctx, char* object_name,size_t object_size, int result_bus, int work_id, int thread_id);
int write_object(rados_ioctx_t io_ctx, char* object_name,size_t object_size, struct timespec *start_time, long *duration, int *status);

/*****
 * Main function.
 *
 *
 *****/
int main(int argc, const char **argv)
{
    global_config_t cptt;
    cptt.node_id=0;
    cptt.ceph_conf.pool_name=NULL;
    cptt.shutdown=0;
    cptt.test_canceled=0;

    cptt.works_count=0;
    cptt.works_info=NULL;

    //TODO: parse argv. improve
    if(argc != 2){
        fprintf(stderr, "Usage: ./cptt <port number>\n");
        return 1;
    }
    cptt.port_number=atoi(argv[1]);

    if (pthread_mutex_init(&cptt.test_launched_lock, NULL) != 0){
        fprintf(stderr,"Main: mutex init failed\n");
    }

    if(pthread_create(&cptt.listen_mgmt_requests_thread, NULL,(void *) &listen_mgmt_requests,&cptt)) {
        fprintf(stderr, "Main: Error creating thread\n");
        exit(1);
    }

    // Main loop
    while(1) {
        print_debug("Main: Waiting test\n");
        while(1) {
            if (cptt.start_test == 1){
                break;
            }
            if (cptt.shutdown == 1){
                print_debug("Main: Shutingdown\n");
                if(pthread_cancel(cptt.listen_mgmt_requests_thread)) {
                    fprintf(stderr, "Main: Error joining thread\n");
                    exit(1);
                }
                return 0;
            }
            sleep(10);
        }
        print_debug("Main: Starting test\n");
        for(int work_id=0;work_id<cptt.works_count;work_id++) {
            print_debug("Main: Init work: %d\n",work_id);
            cptt.works_info[work_id]->work_completed=0;
            cptt.works_info[work_id]->workers_info=(worker_info_t **)malloc(cptt.works_info[work_id]->workload_def.thread_count*sizeof(worker_info_t *));
            for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count ; thread_id++){
                print_debug("Main: init thread %d\n",thread_id);
                cptt.works_info[work_id]->workers_info[thread_id]=(worker_info_t *)malloc(sizeof(worker_info_t));
                cptt.works_info[work_id]->workers_info[thread_id]->node_id=cptt.node_id;
                cptt.works_info[work_id]->workers_info[thread_id]->work_id=work_id;
                cptt.works_info[work_id]->workers_info[thread_id]->thread_id=thread_id;
                cptt.works_info[work_id]->workers_info[thread_id]->ceph_conf=&cptt.ceph_conf;
                cptt.works_info[work_id]->workers_info[thread_id]->completed_ops=0;
                cptt.works_info[work_id]->workers_info[thread_id]->op_type=cptt.works_info[work_id]->workload_def.op_type;
                cptt.works_info[work_id]->workers_info[thread_id]->start_at=cptt.start_at;
                cptt.works_info[work_id]->workers_info[thread_id]->ops_stats_link_list=NULL;
                cptt.works_info[work_id]->workers_info[thread_id]->launched=0;
                cptt.works_info[work_id]->workers_info[thread_id]->stopped=0;
                cptt.works_info[work_id]->workers_info[thread_id]->stop=0;
                cptt.works_info[work_id]->workers_info[thread_id]->object_size=cptt.works_info[work_id]->workload_def.object_size;
                if (pthread_create(&cptt.works_info[work_id]->workers_info[thread_id]->worker_thread, NULL, (void *)&start_worker_wrapper, cptt.works_info[work_id]->workers_info[thread_id])) {
                    fprintf(stderr, "Main: Error creating thread\n");
                    exit(1);
                }             
            }
        }

        print_debug("Main: All threads started\n");

        while (1) {
            int completed_works=0;
            for(int work_id=0; work_id < cptt.works_count; work_id++){
                if (cptt.works_info[work_id]->work_completed!=0){
                    completed_works++;
                    continue;
                }
                unsigned long long total_objects=0; 
                int stopped_threads=0;
                for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count; thread_id++){
                    print_info("Main: Total objects for work %d thread %d is %llu\n", work_id, thread_id, (cptt.works_info[work_id]->workers_info[thread_id]->completed_ops));
                    total_objects+=cptt.works_info[work_id]->workers_info[thread_id]->completed_ops;
                    if(cptt.works_info[work_id]->workers_info[thread_id]->stopped != 0){
                        stopped_threads++;
                    }
                }

                print_info("Main: Total objects for work %d is %llu\n",work_id, total_objects);

                if(stopped_threads >= cptt.works_info[work_id]->workload_def.thread_count){
                    print_debug("Main: Work %d is completd\n",  work_id);
                    cptt.works_info[work_id]->work_completed=1;
                    continue;
                }
                if (total_objects >= cptt.works_info[work_id]->workload_def.total_ops){
                    print_debug("Main: write %llu objects from %d for work %d\n", total_objects, cptt.works_info[work_id]->workload_def.total_ops, work_id);
                    for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count; thread_id++){
                        cptt.works_info[work_id]->workers_info[thread_id]->stop = 1;
                    }
                    
                    cptt.works_info[work_id]->work_completed=1;
                    continue;
                }
            }
            if(completed_works >= cptt.works_count){
                print_debug("Main: all works is completed\n");
                break;
            }

            if(cptt.shutdown == 1 || cptt.test_canceled == 1){
                for(int work_id=0; work_id<cptt.works_count;work_id++){
                    for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count; thread_id++){
                        cptt.works_info[work_id]->workers_info[thread_id]->stop = 1;
                    }
                }
            }
            
            sleep(10);
        }
        
        print_debug("Main: all works is finished\n");

	for(int work_id=0; work_id<cptt.works_count;work_id++){
            print_debug("Main: Join threads for %d work\n", work_id);
	    for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count; thread_id++){
                print_debug("Main: Join thread %d\n",thread_id);
                if(pthread_join(cptt.works_info[work_id]->workers_info[thread_id]->worker_thread, NULL)){
                    fprintf(stderr, "Main: error joining thread\n");
                    exit(1);
                }
	    }
        }

        printf("Main: all thread is joined\n");

        if(cptt.shutdown == 1){
            if(pthread_cancel(cptt.listen_mgmt_requests_thread)) {
                fprintf(stderr, "Main: Error joining thread\n");
                exit(1);
            }
            return 0;
        }

        save_test_results(&cptt);

        for(int work_id=0;work_id<cptt.works_count;work_id++) {
            print_debug("Main: Free memory for  work: %d\n",work_id);
            for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count ; thread_id++){
                print_debug("Main: Free memeory for %d thread %d work\n",thread_id, work_id);
                op_stat_list_t *next_el_in_ll=cptt.works_info[work_id]->workers_info[thread_id]->ops_stats_link_list;
                op_stat_list_t *cur_el_in_ll=NULL;
                while(1){
                    if(next_el_in_ll == NULL){
                        break;
                    }
                    print_debug("Main: Free memory for %llu object\n", next_el_in_ll->op_stat->object_id);
                    cur_el_in_ll=next_el_in_ll;
                    free(cur_el_in_ll->op_stat);
                    next_el_in_ll=cur_el_in_ll->prev;
                    free(cur_el_in_ll);
                    cur_el_in_ll=NULL;                    
                }
                print_debug("Main: free memory for thread %d\n", thread_id);
                free(cptt.works_info[work_id]->workers_info[thread_id]);
            }
            print_debug("Main: free memory for work %d\n", work_id);
            free(cptt.works_info[work_id]);
        }        
        cptt.test_canceled=0;
        cptt.start_test=0;
        print_debug("Main: Test finished\n");
        pthread_mutex_unlock(&cptt.test_launched_lock);
    }


    return 0;
}

int save_test_results(global_config_t *cptt){
    operation_statistics_t *op_stat;
    FILE *test_results_file = fopen("result.csv","w");
    if (test_results_file == NULL) {
        fprintf(stderr,"ERROR OPEN FILE\n");
        return 1;
    }

    fprintf(test_results_file,"Work ID,Thread ID,Object ID,Operation type,Start time,Duration,Object size,Status\n");
    fflush(test_results_file);
   
    for(int work_id=0;work_id<cptt->works_count;work_id++) {
        for (int thread_id=0; thread_id < cptt->works_info[work_id]->workload_def.thread_count ; thread_id++){
            op_stat_list_t *next_el_in_ll=cptt->works_info[work_id]->workers_info[thread_id]->ops_stats_link_list;
            op_stat_list_t *cur_el_in_ll=NULL;
            while(1){
                if(next_el_in_ll == NULL){
                    break;
                }
                cur_el_in_ll=next_el_in_ll;
                op_stat=cur_el_in_ll->op_stat;
                fprintf(test_results_file,"%d,%d,%llu,%d,%lld.%ld,%ld,%zu,%s\n",
op_stat->work_id,
op_stat->thread_id,
op_stat->object_id,
op_stat->op_type,
(long long) op_stat->start_time.tv_sec,
(long)round(op_stat->start_time.tv_usec / 1.0e3),
op_stat->duration,
op_stat->object_size,
strerror(-op_stat->status));
                next_el_in_ll=cur_el_in_ll->prev;
                cur_el_in_ll=NULL;                    
            }
        }
    }
    fflush(test_results_file);
    fclose(test_results_file);
    return 0;
}

void receive_worker_mgmt_command_loop(void *worker_info_void){
    worker_info_t *worker_info=(worker_info_t *)worker_info_void;
    char command[255];
    print_debug("Worker%d,MGMT: thread is started\n",worker_info->thread_id);

    while(1){
        int ret=read(worker_info->mgmt_bus,command,4);
        command[4]='\0';
        if(ret == 4){
            if(strcmp(command,"Stop") == 0){
		print_debug("Worker%d,MGMT: Receive Stop command\n", worker_info->thread_id);
                worker_info->stop=1;
                close(worker_info->mgmt_bus);
                return;
            } else {
                print_debug("Worker%d,MGMT: Unknown command: \"%s\"\n", worker_info->thread_id, command);
            }
        } else {
            //MGMT bus has broked, we must stop.
	    print_debug("Worker%d,MGMT: mgmt bus is broken, shutdown.\n", worker_info->thread_id);
            worker_info->stop=1;
            close(worker_info->mgmt_bus);
            return;
        }
        if(worker_info->stopped == 1){
            print_debug("Worker%d,MGMT: Main process is not live, shutdown.\n", worker_info->thread_id);
            //Main worker thread has finished, we must stop.
            close(worker_info->mgmt_bus);
            return;
        }
    }       
}

/*****
 *
 *
 *
 *****/
void start_worker(worker_info_t *worker_info){
    int ret;

    rados_t rados = NULL;
    rados_ioctx_t io_ctx = NULL;

    pthread_t worker_mgmt_thread;

    if (pthread_create(&worker_mgmt_thread,NULL,(void *)receive_worker_mgmt_command_loop,worker_info) != 0){
        printf("Worker%d,Main: Error creating thread\n", worker_info->thread_id);
        return;
    }


    print_debug("Worker%d,Main: start init ceph\n", worker_info->thread_id);
    ret = rados_create2(&rados, worker_info->ceph_conf->cluster_name, worker_info->ceph_conf->user, 0 );
    if (ret < 0) {
        fprintf(stderr,"Worker%d,Main: couldn't initialize rados! %s, cluster:%s, user:%s\n", worker_info->thread_id, strerror(errno),worker_info->ceph_conf->cluster_name, worker_info->ceph_conf->user);
        worker_info->stopped=1;
        close(worker_info->op_stat_bus);
        return;
    }

    ret = rados_conf_set(rados, "mon_host", worker_info->ceph_conf->mon_host);
    if (ret < 0) {
        fprintf(stderr, "Worker%d,Main: failed to set mon_host. %s\n", worker_info->thread_id, strerror(errno));
        worker_info->stopped=1;
        close(worker_info->op_stat_bus);
        return;
    }

    ret = rados_conf_set(rados, "key", worker_info->ceph_conf->key);
    if (ret < 0) {
        print_debug("Worker%d,Main: failed to set keyring. %s\n", worker_info->thread_id, strerror(-errno));
        worker_info->stopped=1;
        close(worker_info->op_stat_bus);
        return;
    }

    print_debug("Worker%d,Main: config success, start connect\n", worker_info->thread_id);
    ret = rados_connect(rados);
    if (ret < 0) {
        fprintf(stderr,"Worker%d,Main: couldn't connect to cluster! error %d\n", worker_info->thread_id, ret);
        worker_info->stopped=1;
        close(worker_info->op_stat_bus);
        return;
    }

    print_debug("Worker%d,Main: connect succes, start ioctx create\n", worker_info->thread_id); 
    ret = rados_ioctx_create(rados, worker_info->ceph_conf->pool_name, &io_ctx);
    if (ret < 0) {
        fprintf(stderr,"Worker%d,Main: couldn't set up ioctx! error %d. Pool=%s\n", worker_info->thread_id, ret, worker_info->ceph_conf->pool_name);
        rados_shutdown(rados);
        worker_info->stopped=1;
        close(worker_info->op_stat_bus);
        return;
    }
    print_debug("Worker%d,Main: io create succes\n", worker_info->thread_id);
    while(1) {
        if (time(0)>=worker_info->start_at){
            break;
        }
        if(worker_info->stop == 1){
            print_debug("Worker%d,Main: worker_info->stop is true, shutting down.\n", worker_info->thread_id);
            rados_shutdown(rados);
            worker_info->stopped=1;
            close(worker_info->op_stat_bus);
            return;
        }
    }
    //TODO: change to more memory safe operations[1]
    char *object_name = malloc(50*sizeof(char));

    operation_statistics_t op_stat;

    op_stat.node_id=worker_info->node_id;
    op_stat.work_id=worker_info->work_id;
    op_stat.thread_id=worker_info->thread_id;
    op_stat.op_type=worker_info->op_type;
    op_stat.object_size=worker_info->object_size;
    op_stat.object_id=0;

    while (1){
        op_stat.object_id++;
        print_debug("Worker%d,Main: Write object %llu\n", op_stat.thread_id,op_stat.object_id);
        //TODO: change to more memory safe operations[2]
        sprintf(object_name, "cptt-n%d-w%d-t%d-o%llu", op_stat.node_id, op_stat.work_id, op_stat.thread_id, op_stat.object_id);

        char* buf = (char*)malloc(op_stat.object_size*sizeof(char));

        gettimeofday(&(op_stat.start_time),NULL);

        if (worker_info->op_type == OPERATION_WRITE) {
            ret=rados_write_full(io_ctx, object_name, buf, op_stat.object_size);
        } else if (worker_info->op_type == OPERATION_READ) {
            ret=rados_read(io_ctx, object_name, buf, op_stat.object_size, 0);
        } else if (worker_info->op_type == OPERATION_DELETE) {
            ret=rados_remove(io_ctx, object_name);
        } else {
            fprintf(stderr,"Worker%d,Main: Unknown operation type\n", worker_info->thread_id);
            if (io_ctx) {
                rados_ioctx_destroy(io_ctx);
            }
            rados_shutdown(rados);
            free(object_name);
            close(worker_info->op_stat_bus);
            worker_info->stopped=1;
            return;
        }
        gettimeofday(&(op_stat.stop_time),NULL);
        
        free(buf);
        if(ret < 0){
            op_stat.status=ret;    
        } else {
            op_stat.status=0;
        }
        op_stat.duration=op_stat.stop_time.tv_sec * 1000 + round(op_stat.stop_time.tv_usec / 1000) - op_stat.start_time.tv_sec * 1000 - round(op_stat.start_time.tv_usec / 1000);
        write(worker_info->op_stat_bus,&op_stat,sizeof(operation_statistics_t));

        print_debug("Worker%d,Main: finish write object %llu\n", worker_info->thread_id,op_stat.object_id);

        if(worker_info->stop == 1){
            print_debug("Worker%d,Main: stopping\n", worker_info->thread_id);
            break;
        }
    }
    free(object_name);
    if (io_ctx) {
        rados_ioctx_destroy(io_ctx);
    }
    rados_shutdown(rados);
    close(worker_info->op_stat_bus);
    worker_info->stopped=1;
    return;
}


void start_worker_wrapper(void *worker_info_void){
    worker_info_t *worker_info=(worker_info_t *)worker_info_void;
    pthread_t op_stat_collector_thread;
 
    print_debug("Worker%d,wrapper: started\n",worker_info->thread_id);

    int op_stat_bus[2];
    int mgmt_bus[2];   

    pipe(op_stat_bus);
    pipe(mgmt_bus);

    pid_t worker_pid = fork();
    if(worker_pid < 0){
        fprintf(stderr,"Worker%d,wrapper: Fork error\n",worker_info->thread_id);
        return;
    } else if(worker_pid == 0){
        close(op_stat_bus[0]);
        worker_info->op_stat_bus=op_stat_bus[1];
        close(mgmt_bus[1]);
        worker_info->mgmt_bus=mgmt_bus[0];

        start_worker(worker_info);

        return;
    }
    print_debug("Worker%d,wrapper: process forked\n",worker_info->thread_id);
    close(op_stat_bus[1]);
    worker_info->op_stat_bus=op_stat_bus[0];
    close(mgmt_bus[10]);
    worker_info->mgmt_bus=mgmt_bus[1];
    if(pthread_create(&op_stat_collector_thread,NULL,(void *)&collect_operations_statistics, worker_info) < 0){

    }
    print_debug("Worker%d,wrapper: oper stat coll started\n",worker_info->thread_id);
    while(1){
        if(worker_info->stop == 1){
            print_debug("Worker%d,wrapper: worker_info->stop is true, will stop\n",worker_info->thread_id);
            write(worker_info->mgmt_bus, "Stop", 4);
            int status;
            waitpid(worker_pid,&status,0);
            print_debug("Worker%d,wrapper: whait join thread\n",worker_info->thread_id);
            pthread_join(op_stat_collector_thread,NULL);
            print_debug("Worker%d,wrapper: thread joined\n",worker_info->thread_id);
            close(worker_info->op_stat_bus);
            close(worker_info->mgmt_bus);
	    worker_info->stopped=1;
            return;
        }
        sleep(5);
    }

}

/*****
 *
 *
 *
 *****/
void collect_operations_statistics(void *worker_info_void){
    int ret;
    worker_info_t *worker_info=(worker_info_t *)worker_info_void;
    operation_statistics_t *op_stat;

    op_stat_list_t *prev_el_in_ll=NULL;
    op_stat_list_t *cur_el_in_ll=NULL;

    worker_info->completed_ops=0;
    print_debug("Worker%d,collector: started\n", worker_info->thread_id);
    while(1){
	op_stat=(operation_statistics_t *)malloc(sizeof(operation_statistics_t));
        ret=read(worker_info->op_stat_bus,op_stat,sizeof(operation_statistics_t));
        if (ret>0) {
		cur_el_in_ll=(op_stat_list_t *)malloc(sizeof(op_stat_list_t));
                print_debug("Worker%d,collector: receive object num %llu\n", worker_info->thread_id,op_stat->object_id);
		cur_el_in_ll->op_stat=op_stat;
		cur_el_in_ll->prev=prev_el_in_ll;

		worker_info->completed_ops++;

		prev_el_in_ll=cur_el_in_ll;
		cur_el_in_ll=NULL;
        } else {
            worker_info->ops_stats_link_list=prev_el_in_ll;            
            worker_info->stop=1;
            print_debug("Worker%d,collector: op stat bus is broken, shutdown\n", worker_info->thread_id);
            break;
        }
    }

    return ;
}

/*****
 *
 *
 *
 *****/
int pars_workload_definition(char *buff, global_config_t *cptt) {
    json_t *root;
    json_error_t error;
    json_t *tmp_object1;
    json_t *tmp_object2;
    json_t *tmp_object3;

    const char *tmp_str;

    root = json_loads(buff,0, &error);
    
    tmp_object1 = json_object_get(root, "mon_host");
    if(!json_is_string(tmp_object1)){
        fprintf(stderr, "error: mon_host is not string\n");
        return 1;
    }
    tmp_str=json_string_value(tmp_object1);
    cptt->ceph_conf.mon_host=(char *)malloc(strlen(tmp_str)*sizeof(char));
    strcpy(cptt->ceph_conf.mon_host,tmp_str);
    
    tmp_object1 = json_object_get(root, "cluster_name");
    if(!json_is_string(tmp_object1)){
        fprintf(stderr, "error: user is not string\n");
        return 1;
    }
    tmp_str=json_string_value(tmp_object1);
    cptt->ceph_conf.cluster_name=(char *)malloc(strlen(tmp_str)*sizeof(char));
    strcpy(cptt->ceph_conf.cluster_name,tmp_str);
    
    tmp_object1 = json_object_get(root, "user");
    if(!json_is_string(tmp_object1)){
        fprintf(stderr, "error: user is not string\n");
        return 1;
    }
    tmp_str=json_string_value(tmp_object1);
    cptt->ceph_conf.user=(char *)malloc(strlen(tmp_str)*sizeof(char));
    strcpy(cptt->ceph_conf.user,tmp_str);

    tmp_object1 = json_object_get(root, "key");
    if(!json_is_string(tmp_object1)){
        fprintf(stderr, "error: key is not string\n");
        return 1;
    }
    tmp_str=json_string_value(tmp_object1);
    cptt->ceph_conf.key=(char *)malloc(strlen(tmp_str)*sizeof(char));
    strcpy(cptt->ceph_conf.key,tmp_str);

    tmp_object1 = json_object_get(root, "pool_name");
    if(!json_is_string(tmp_object1)){
        fprintf(stderr, "error: pool_name is not string\n");
        return 1;
    }
    tmp_str=json_string_value(tmp_object1);
    cptt->ceph_conf.pool_name=(char *)malloc(strlen(tmp_str)*sizeof(char));
    strcpy(cptt->ceph_conf.pool_name,tmp_str);





    tmp_object1 = json_object_get(root, "start_at");
    if(!json_is_integer(tmp_object1)){
        fprintf(stderr, "error: start_at is not string\n");
        return 1;
    }
    cptt->start_at=json_integer_value(tmp_object1);

    tmp_object1 = json_object_get(root, "works");
    if(!json_is_array(tmp_object1)){
        fprintf(stderr, "error: start_at is not string\n");
        return 1;
    }

    cptt->works_count=json_array_size(tmp_object1);
    cptt->works_info=malloc((cptt->works_count)*sizeof(work_info_t *));

    for(int i = 0; i < (cptt->works_count); i++){
        cptt->works_info[i]=(work_info_t *)malloc(sizeof(work_info_t));

        tmp_object2 = json_array_get(tmp_object1, i);
        if(!json_is_object(tmp_object2)){
            fprintf(stderr, "error: commit %d is not an object\n", i + 1);
            return -1;
        }
        tmp_object3 = json_object_get(tmp_object2, "thread_count");
        if(!json_is_integer(tmp_object3)){
            fprintf(stderr, "error: start_at is not string\n");
            return -1;
        }
        cptt->works_info[i]->workload_def.thread_count=json_integer_value(tmp_object3);

        tmp_object3 = json_object_get(tmp_object2, "total_ops");
        if(!json_is_integer(tmp_object3)){
            fprintf(stderr, "error: start_at is not string\n");
            return -1;
        }
        cptt->works_info[i]->workload_def.total_ops=json_integer_value(tmp_object3);

        tmp_object3 = json_object_get(tmp_object2, "op_type");
        if(!json_is_integer(tmp_object3)){
            fprintf(stderr, "error: start_at is not string\n");
            return -1;
        }
        cptt->works_info[i]->workload_def.op_type=json_integer_value(tmp_object3);

        tmp_object3 = json_object_get(tmp_object2, "object_size");
        if(!json_is_integer(tmp_object3)){
            fprintf(stderr, "error: start_at is not string\n");
            return -1;
        }
        cptt->works_info[i]->workload_def.object_size=json_integer_value(tmp_object3);
    }
    return 0;
}

/*****
 *
 *
 *
 *****/
void process_mgmt_request(void *request_struct_void){
    int sock=((request_struct_t *)request_struct_void)->sock;
    global_config_t *cptt=((request_struct_t *)request_struct_void)->cptt;
    int n,ret;
    json_t *root;
    json_error_t error;
    //TODO: change to more inteligent read from buffer
    char buffer[20000];
    bzero(buffer,20000);
    
    free((request_struct_t *)request_struct_void);

    n = read(sock,buffer,20000);
    if (n < 0) {
        fprintf(stderr,"ERROR reading from socket, %d, %s\n", errno, strerror(errno));
        close(sock);
        return;
    }

    if (strcmp(buffer,"Status\n")==0) {
        print_debug("Receive \"Status\" command\n");
        ret = pthread_mutex_trylock(&(cptt->test_launched_lock));
        if (ret == 0) {
            pthread_mutex_unlock(&(cptt->test_launched_lock));
            n = write(sock,"Ready\n",6);
            if (n < 0) {
                close(sock);
                fprintf(stderr,"ERROR writing to socket");
                return;
            }
            close(sock);
            return;
        } else if (ret == EBUSY) {
            n = write(sock,"Busy\n",5);
            if (n < 0) {
                close(sock);
                fprintf(stderr,"ERROR writing to socket");
                return;
            }
            close(sock);
            return;
        }
    } else if (strcmp(buffer, "Cancel\n")==0) {
        print_debug("Receive \"Cancel\" command\n");
        //TODO
        cptt->test_canceled=1;
        n = write(sock,"Ok.\n",4);
        if (n < 0) {
            close(sock);
            fprintf(stderr,"ERROR writing to socket");
            return;
        }
        close(sock);
        return;
    } else if (strcmp(buffer, "Shutdown\n")==0) {
        print_debug("Receive \"Shutdown\" command\n");
        //TODO
        cptt->shutdown=1;
        n = write(sock,"Ok.\n",4);
        if (n < 0) {
            close(sock);
            fprintf(stderr,"ERROR writing to socket");
            return;
        }
        close(sock);
        return;
    } else {
        root = json_loads(buffer,0, &error);
        if ( root == NULL){
            n = write(sock,"Uncknown command\n",17);
            if (n < 0) {
                close(sock);
                fprintf(stderr,"ERROR writing to socket");
                return;
            }
            close(sock);
            return;
        } else {
        print_debug("Receive \"start test\" command\n");
            ret = pthread_mutex_trylock(&(cptt->test_launched_lock));
            if (ret == 0) {
                if(pars_workload_definition(buffer, cptt) == 0){
                    json_decref(root);
                    cptt->start_test=1;
                    n = write(sock,"Launched\n",9);
                    if (n < 0) {
                        close(sock);
                        fprintf(stderr,"ERROR writing to socket");
                        return;
                    }
                    close(sock);
                    return;
                } else {
                    json_decref(root);
                    pthread_mutex_unlock(&(cptt->test_launched_lock));
                    n = write(sock,"Error\n",6);
                    if (n < 0) {
                        close(sock);
                        fprintf(stderr,"ERROR writing to socket");
                        return;
                    }
                    close(sock);
                    return;
                }
            } else if (ret == EBUSY) {
                n = write(sock,"Busy\n",5);
                if (n < 0) {
                    close(sock);
                    fprintf(stderr,"ERROR writing to socket");
                    return;
                }
                close(sock);
                return;
            }
        }
    }
    close(sock);
    return;
}

/*****
 *
 *
 *
 *****/
void listen_mgmt_requests(void *cptt_void) {
    int socket_desc, client_sock, c;
    struct sockaddr_in server, client;
    global_config_t *cptt=(global_config_t *)cptt_void;
    //TODO: add port, IP customization
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_desc == -1){
        fprintf(stderr,"Could not create socket");
        exit(1);
    }

    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(cptt->port_number);

    if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0){
        fprintf(stderr,"bind failed. Error");
        exit(1);
    }

    listen(socket_desc, 3);

    c = sizeof(struct sockaddr_in);
    pthread_t thread_id;

    request_struct_t *request_struct;
    while(1){
        client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c);
        if (client_sock < 0 ) {
            printf("SOCK ERROR: %s\n",strerror(errno));
            break;
        }
        request_struct=(request_struct_t *)malloc(sizeof(request_struct_t));
        request_struct->sock=client_sock;
        request_struct->cptt=cptt;
        if( pthread_create( &thread_id , NULL ,  (void *)process_mgmt_request, request_struct) < 0){
            fprintf(stderr,"could not create thread");
            exit(1);
        }
    }

    printf("Listening stopping\n");
    exit(1);
}
