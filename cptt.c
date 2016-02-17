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
//#include "cptt.h"

#define FIFO_NAME "/run/cptt-msgbus"
#define CONFIG_FILE "/etc/ceph/ceph.conf"
#define OPERATION_WRITE 1
#define OPERATION_READ 2
#define OPERATION_DELETE 3
#define PORT_NUMBER 7777
#define DEBUG 1
#define INFO 1

#define print_debug(fmt,...) do { if (DEBUG) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)
#define print_info(fmt,...) do { if (INFO) fprintf(stderr, fmt, ##__VA_ARGS__); } while (0)

#if 1 
typedef struct {
    char *mon_ip;
    char *pool_name;
    char *keyring;
} ceph_conf_t;

typedef struct {
    int node_id;
    int work_id;
    int thread_id;
    unsigned long long object_id;
    struct timespec start_time;
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
  int obj_start_num; //old
  int obj_end_num; //old
  int op_type;
  size_t object_size;
} workload_definition_t;

typedef struct {
    int node_id;
    int work_id;
    int thread_id;
    ceph_conf_t *ceph_conf;
    pthread_t worker_thread; // do we need it?
    int launched;
    int stop;
    int stopped;
    unsigned long long completed_ops;
    int op_type;
    size_t object_size;
    struct timespec start_at;
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
#endif

/*****
 * Functions definitions
 *****/
void collect_operations_statistics();
int delete_object(rados_ioctx_t io_ctx, char* object_name, size_t object_size, int result_bus, int work_id, int thread_id);
void start_worker(void *worker_info_void);
//int launch_test(int node_id, int thread_id, char *pool_name, int objects_count, int operation_type, long object_size, time_t start_at, operation_statistics_t **op_stat_arr);
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
    int ret;
    pthread_t collect_operations_statistics_thread;
    pthread_t listen_mgmt_requests_thread;
    
    global_config_t cptt;
    cptt.node_id=0;
    cptt.ceph_conf.pool_name="test_pool1";
    cptt.shutdown=0;
    cptt.test_canceled=0;
    if (pthread_mutex_init(&cptt.test_launched_lock, NULL) != 0){
        fprintf(stderr,"mutex init failed\n");
    }
    cptt.works_count=0;
    cptt.works_info=NULL;
    //TODO: parse argv. init ip, port, is daemon

    if(pthread_create(&cptt.listen_mgmt_requests_thread, NULL,(void *) &listen_mgmt_requests,&cptt)) {
        fprintf(stderr, "Error creating thread\n");
        exit(1);
    }

    // Main loop
    while(1) {
        print_debug("Waiting test\n");
        while(1) {
            if (cptt.start_test == 1){
                break;
            }
            if (cptt.shutdown == 1){
                print_debug("Shutingdown\n");
                if(pthread_join(cptt.listen_mgmt_requests_thread, NULL)) {
                    fprintf(stderr, "Error joining thread\n");
                    exit(1);
                }
                return 0;
            }
        }
        print_debug("Starting test\n");
        for(int work_id=0;work_id<cptt.works_count;work_id++) {
            print_debug("Init work: %d\n",work_id);
            cptt.works_info[work_id]->work_completed=0;
            cptt.works_info[work_id]->workers_info=(worker_info_t **)malloc(cptt.works_info[work_id]->workload_def.thread_count*sizeof(worker_info_t *));
            for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count ; thread_id++){
                print_debug("init thread %d\n",thread_id);
                cptt.works_info[work_id]->workers_info[thread_id]=(worker_info_t *)malloc(sizeof(worker_info_t));
                cptt.works_info[work_id]->workers_info[thread_id]->node_id=cptt.node_id;
                cptt.works_info[work_id]->workers_info[thread_id]->work_id=work_id;
                cptt.works_info[work_id]->workers_info[thread_id]->thread_id=thread_id;
                cptt.works_info[work_id]->workers_info[thread_id]->ceph_conf=&cptt.ceph_conf;
                //cptt.works_info[work_id]->workers_info[thread_id]->worker_thread=NULL;
                cptt.works_info[work_id]->workers_info[thread_id]->completed_ops=0;
                cptt.works_info[work_id]->workers_info[thread_id]->op_type=cptt.works_info[work_id]->workload_def.op_type;
//              cptt.works_info[work_id]->workers_info[thread_id]->start_at=0; //TODO
                cptt.works_info[work_id]->workers_info[thread_id]->ops_stats_link_list=NULL;
                cptt.works_info[work_id]->workers_info[thread_id]->launched=0;
                cptt.works_info[work_id]->workers_info[thread_id]->stopped=0;
                cptt.works_info[work_id]->workers_info[thread_id]->stop=0;
                cptt.works_info[work_id]->workers_info[thread_id]->object_size=cptt.works_info[work_id]->workload_def.object_size;
                if (pthread_create(&cptt.works_info[work_id]->workers_info[thread_id]->worker_thread, NULL, (void *)&start_worker, cptt.works_info[work_id]->workers_info[thread_id])) {
                    fprintf(stderr, "Error creating thread\n");
                    exit(1);
                }             
            }
        }

        print_debug("All threads started\n");

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
                    print_info("Total objects for work %d thread %d is %llu\n", work_id, thread_id, (cptt.works_info[work_id]->workers_info[thread_id]->completed_ops));
                    total_objects+=cptt.works_info[work_id]->workers_info[thread_id]->completed_ops;
                    if(cptt.works_info[work_id]->workers_info[thread_id]->stopped != 0){
                        stopped_threads++;
                    }
                }

                print_info("Total objects for work %d is %llu\n",work_id, total_objects);

                if(stopped_threads >= cptt.works_info[work_id]->workload_def.thread_count){
                    print_debug("Work %d is completd\n",  work_id);
                    cptt.works_info[work_id]->work_completed=1;
                    continue;
                }
                if (total_objects >= cptt.works_info[work_id]->workload_def.total_ops){
                    print_debug("write %llu objects from %d for work %d\n", total_objects, cptt.works_info[work_id]->workload_def.total_ops, work_id);
                    for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count; thread_id++){
                        cptt.works_info[work_id]->workers_info[thread_id]->stop = 1;
                    }
                    
                    cptt.works_info[work_id]->work_completed=1;
                    continue;
                }
            }
            if(completed_works >= cptt.works_count){
                print_debug("all works is completed\n");
                break;
            }

            if(cptt.shutdown == 1 || cptt.test_canceled == 1){
                for(int work_id=0; work_id<cptt.works_count;work_id++){
                    for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count; thread_id++){
                        cptt.works_info[work_id]->workers_info[thread_id]->stop = 1;
                    }
                }
            }
        }
        
        print_debug("all thread is finished\n");

	for(int work_id=0; work_id<cptt.works_count;work_id++){
            print_debug("Join threads for %d work\n", work_id);
	    for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count; thread_id++){
                print_debug("Join thread %d\n",thread_id);
                if(pthread_join(cptt.works_info[work_id]->workers_info[thread_id]->worker_thread, NULL)){
                    fprintf(stderr, "error joining thread\n");
                    exit(1);
                }
	    }
        }

        printf("all thread is joined\n");

        if(cptt.shutdown == 1){
            if(pthread_join(cptt.listen_mgmt_requests_thread, NULL)) {
                fprintf(stderr, "Error joining thread\n");
                exit(1);
            }
            return 0;
        }

        if(cptt.test_canceled == 0){
            
        }

        for(int work_id=0;work_id<cptt.works_count;work_id++) {
            print_debug("Free memory for  work: %d\n",work_id);
            for (int thread_id=0; thread_id < cptt.works_info[work_id]->workload_def.thread_count ; thread_id++){
                print_debug("Free memeory for %d thread %d work\n",thread_id, work_id);
                op_stat_list_t *next_el_in_ll=cptt.works_info[work_id]->workers_info[thread_id]->ops_stats_link_list;
                op_stat_list_t *cur_el_in_ll=NULL;
                while(1){
                    if(next_el_in_ll == NULL){
                        break;
                    }
                    print_debug("Free memory for %llu object\n", next_el_in_ll->op_stat->object_id);
                    cur_el_in_ll=next_el_in_ll;
                    free(cur_el_in_ll->op_stat);
                    next_el_in_ll=cur_el_in_ll->prev;
                    free(cur_el_in_ll);
                    cur_el_in_ll=NULL;                    
                }
                print_debug("free memory for thread %d\n", thread_id);
                free(cptt.works_info[work_id]->workers_info[thread_id]);
            }
            print_debug("free memory for work %d\n", work_id);
            free(cptt.works_info[work_id]);
        }        

        cptt.start_test=0;
        print_debug("Test finished\n");
        pthread_mutex_unlock(&cptt.test_launched_lock);
    }

    unlink(FIFO_NAME);

    return 0;
}

/*****
 *
 *
 *
 *****/
int write_object(rados_ioctx_t io_ctx, char* object_name,size_t object_size, struct timespec *start_time, long *duration, int *status){
    struct timespec stop_time;
    int ret = 0;
    char* buf = (char*)malloc(object_size*sizeof(char));

    print_debug("in funciton write\n");
    clock_gettime(CLOCK_REALTIME, start_time);
    print_debug("in funciton write, get start clock\n");
    ret = rados_write_full(io_ctx, object_name, buf, object_size);
    print_debug("in funciton write, perform write\n");
    clock_gettime(CLOCK_REALTIME, &stop_time);
    print_debug("in funciton write, get stop clock\n");

    free(buf);

    *duration=round(stop_time.tv_nsec / 1.0e6) + stop_time.tv_sec*1000 - round(start_time->tv_nsec / 1.0e6) - start_time->tv_sec*1000;
    *status=ret;

    return 0;
}
#if 0
/*****
 *
 *
 *
 *****/
int read_object(rados_ioctx_t io_ctx, char* object_name,size_t object_size, int operations_statistics_msg_bus, int work_id, int thread_id){
    long long ms_start, ms_end, ms_total;
    struct timespec start_time;
    struct timespec stop_time;
    int ret = 0;
    operation_statistics_t operation_statistics;

    char *buf=(char*)malloc(object_size*sizeof(char));

    clock_gettime(CLOCK_REALTIME, &start_time);
    ret = rados_read(io_ctx, object_name, buf, object_size, 0);
    clock_gettime(CLOCK_REALTIME, &stop_time);

    free(buf);

    if (ret < 0) {
        printf("couldn't read object! error %d\n", ret);
        ret = EXIT_FAILURE;
        return -1;
    } else {
        ms_start = round(start_time.tv_nsec / 1.0e6) + start_time.tv_sec*1000;
        ms_end = round(stop_time.tv_nsec / 1.0e6) + stop_time.tv_sec*1000;
        ms_total = ms_end - ms_start;
    }

        operation_statistics.work_id=work_id;
    operation_statistics.thread_id=thread_id;
    operation_statistics.start_time=start_time;
    operation_statistics.op_type=OPERATION_READ;
    operation_statistics.duration=ms_total;
    operation_statistics.object_size=object_size;
        operation_statistics.status=1;

    write(operations_statistics_msg_bus, &operation_statistics, sizeof(operation_statistics_t));

    return 0;
}

/*****
 *
 *
 *
 *****/
int delete_object(rados_ioctx_t io_ctx, char* object_name, size_t object_size, int operations_statistics_msg_bus, int work_id, int thread_id){
    int ret = 0;
    long long ms_start, ms_end, ms_total;
    struct timespec start_time;
    struct timespec stop_time;
    operation_statistics_t operation_statistics;

    clock_gettime(CLOCK_REALTIME, &start_time);
    ret = rados_remove(io_ctx, object_name);
    clock_gettime(CLOCK_REALTIME, &stop_time);

    if (ret < 0 && errno!=2) {
        printf("couldn't delete object! error %d,%d,%s\n", ret, errno,strerror(errno));
        ret = EXIT_FAILURE;
        return -1;
    } else {
        ms_start = round(start_time.tv_nsec / 1.0e6) + start_time.tv_sec*1000;
        ms_end = round(stop_time.tv_nsec / 1.0e6) + stop_time.tv_sec*1000;
        ms_total = ms_end - ms_start;
    }

    operation_statistics.work_id=work_id;
    operation_statistics.thread_id=thread_id;
    operation_statistics.start_time=start_time;
    operation_statistics.op_type=1;
    operation_statistics.duration=ms_total;
    operation_statistics.object_size=0;
        operation_statistics.status=1;

    write(operations_statistics_msg_bus, &operation_statistics, sizeof(operation_statistics_t));

    return 0;
}
#endif
/*****
 *
 *
 *
 *****/
//int launch_test(int node_id, int thread_id, char *pool_name, int objects_count, int operation_type, long object_size, time_t start_at, operation_statistics_t **op_stat_arr){
void start_worker(void *worker_info_void){
    worker_info_t *worker_info=(worker_info_t *)worker_info_void;
    int ret;

    rados_t rados = NULL;
    rados_ioctx_t io_ctx = NULL;

    #if 1
    //TODO: directly pass mon_ip and keyring without any configuration files
    ret = rados_create(&rados, "admin");
    if (ret < 0) {
        fprintf(stderr,"couldn't initialize rados! error %d\n", ret);
        worker_info->stopped=1;
        return;
    }
    print_debug("Rados init success\n");
    ret = rados_conf_read_file(rados, CONFIG_FILE);
    if (ret < 0) {
        fprintf(stderr, "failed to parse config file %s! error %d\n", CONFIG_FILE, ret);
        worker_info->stopped=1;
        return;
    }
    #endif

    print_debug("Rados connect success\n");
    ret = rados_connect(rados);
    if (ret < 0) {
        fprintf(stderr,"couldn't connect to cluster! error %d\n", ret);
        worker_info->stopped=1;
        return;
    }


    print_debug("Rados io create success\n");
    ret = rados_ioctx_create(rados, worker_info->ceph_conf->pool_name, &io_ctx);
    if (ret < 0) {
        fprintf(stderr,"couldn't set up ioctx! error %d. Pool=%s\n", ret, worker_info->ceph_conf->pool_name);
        rados_shutdown(rados);
        worker_info->stopped=1;
        return;
    }
    while(1) {
        #if 0
        //TODO: change to timespec comparation
        if (time(0)>=worker_info->start_at){
            break;
        }
        #else
        break;
        #endif
    }
    //TODO: change to more memory safe operations[1]
    char *object_prefix="testobject";
    char *object_name = malloc(50*sizeof(char));
    op_stat_list_t *prev_el_in_ll=NULL;
    op_stat_list_t *cur_el_in_ll=NULL;
    operation_statistics_t *op_stat=NULL;
    unsigned long long object_id=0;

    worker_info->completed_ops=0;
#if 1
    while (1){
        print_debug("Write object %llu\n",object_id);
        //TODO: change to more memory safe operations[2]
        sprintf(object_name, "cptt-n%d-w%d-t%d-o%llu", worker_info->node_id, worker_info->work_id, worker_info->thread_id, object_id);

        cur_el_in_ll=(op_stat_list_t *)malloc(sizeof(op_stat_list_t));
        op_stat=(operation_statistics_t *)malloc(sizeof(operation_statistics_t));

        cur_el_in_ll->op_stat=op_stat;
        cur_el_in_ll->prev=prev_el_in_ll;

        op_stat->node_id=worker_info->node_id;
        op_stat->work_id=worker_info->work_id;
        op_stat->thread_id=worker_info->thread_id;
        op_stat->object_id=object_id;
        op_stat->op_type=worker_info->op_type;
        op_stat->object_size=worker_info->object_size;
        print_debug("t%d: writing begin\n", worker_info->thread_id);
        if (worker_info->op_type == OPERATION_WRITE) {
            ret = write_object(io_ctx,object_name, worker_info->object_size, &(op_stat->start_time), &(op_stat->duration), &(op_stat->status));
        } else if (worker_info->op_type == OPERATION_READ) {
//           ret = read_object(io_ctx,object_name, object_size, op_stat_arr[object_id], node_id, thread_id);
        } else if (worker_info->op_type == OPERATION_DELETE) {
//           ret = delete_object(io_ctx, object_name, 0, op_stat_arr[object_id], node_id, thread_id);
        } else {
            fprintf(stderr,"Unknown operation type\n");
            if (io_ctx) {
                rados_ioctx_destroy(io_ctx);
            }
            rados_shutdown(rados);
            free(object_name);
            worker_info->stopped=1;
            return;
        }
        print_debug("t%d: writing end\n", worker_info->thread_id);
        worker_info->completed_ops++;

        if(worker_info->stop == 1){
            worker_info->ops_stats_link_list=cur_el_in_ll;
            break;
        }
        prev_el_in_ll=cur_el_in_ll;
        cur_el_in_ll=NULL;
        object_id++;
        print_debug("finish write object %llu\n",object_id);
    }
#endif
    free(object_name);
    if (io_ctx) {
        rados_ioctx_destroy(io_ctx);
    }
    rados_shutdown(rados);
    worker_info->stopped=1;
    return;
}

/*****
 *
 *
 *
 *****/
void collect_operations_statistics(){
    int ret;
    operation_statistics_t operation_statistics;
    FILE *test_results_file = fopen("result.csv","w");
    if (test_results_file == NULL) {
        fprintf(stderr,"ERROR OPEN FILE\n");
        return;
    }

    int operations_statistics_msg_bus = open(FIFO_NAME, O_RDONLY);
    if ( operations_statistics_msg_bus == -1 ) {
        fprintf(stderr,"open error: %s\n", strerror(errno));
        return;
    }

    fprintf(test_results_file,"Work ID,Thread ID,Operation type,Start time,Duration,Object size,Status\n");
    fflush(test_results_file);

    while(1){
        ret=read(operations_statistics_msg_bus,&operation_statistics,sizeof(operation_statistics_t));
        if (ret>0) {
            fprintf(test_results_file,"%d,%d,%d,%lld.%ld,%ld,%zu,%d\n",
operation_statistics.work_id,
operation_statistics.thread_id,
operation_statistics.op_type,
(long long) operation_statistics.start_time.tv_sec,
(long)round(operation_statistics.start_time.tv_nsec / 1.0e6),
operation_statistics.duration,
operation_statistics.object_size,
operation_statistics.status);
        } else {
            break;
        }
    }

    close(operations_statistics_msg_bus);
    fflush(test_results_file);
    fclose(test_results_file);
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

    root = json_loads(buff,0, &error);

    tmp_object1 = json_object_get(root, "pool_name");
    if(!json_is_string(tmp_object1)){
        fprintf(stderr, "error: pool_name is not string\n");
        return 1;
    }
    const char *tmp_str=json_string_value(tmp_object1);
    cptt->ceph_conf.pool_name=(char *)malloc(strlen(tmp_str)*sizeof(char));
    strcpy(cptt->ceph_conf.pool_name,tmp_str);
    #if 0
    tmp_object1 = json_object_get(root, "start_at");
    if(!json_is_integer(tmp_object1)){
        fprintf(stderr, "error: start_at is not string\n");
        return 1;
    }
    g_start_at=json_integer_value(tmp_object1);
    #endif
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
    server.sin_port = htons(PORT_NUMBER);

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
