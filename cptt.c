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

#define FIFO_NAME "/run/cptt-msgbus"
#define CONFIG_FILE "/etc/ceph/ceph.conf"
#define OPERATION_WRITE 1
#define OPERATION_READ 2
#define OPERATION_DELETE 3
#define PORT_NUMBER 7777

typedef struct {
    int work_id;
    int thread_id;
    struct timespec start_time;
    int  op_type;
    long duration;
    size_t object_size;
    int status;
} operation_statistics_t;

typedef struct {
  int thread_count;
  int obj_start_num;
  int obj_end_num;
  int op_type;
  size_t object_size;
} workload_definition_t;

pthread_mutex_t g_start_test_lock;
char *g_pool_name;
time_t g_start_at;
workload_definition_t **g_works;
int g_works_count;
int  g_start_test;
int g_stop_listen;
int g_shutdown;

/*****
 * Functions definitions
 *****/
void collect_operations_statistics();
int delete_object(rados_ioctx_t io_ctx, char* object_name, size_t object_size, int result_bus, int work_id, int thread_id);
int launch_test(int work_id, int thread_id, char *pool_name, int object_start_num, int object_end_num, int operation_type, long object_size, time_t start_at);
void listen_mgmt_requests();
int pars_workload_definition(char *root);
void process_mgmt_request(void *sock_void);
int read_object(rados_ioctx_t io_ctx, char* object_name,size_t object_size, int result_bus, int work_id, int thread_id);
int write_object(rados_ioctx_t io_ctx, char* object_name,size_t object_size, int result_bus, int work_id, int thread_id);

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

    //TODO: parse argv. init ip, port, is daemon

        g_start_test=0;
        g_shutdown=0;
        g_stop_listen=0;
    if (pthread_mutex_init(&g_start_test_lock, NULL) != 0)
    {
        fprintf(stderr,"mutex init failed\n");
        return 1;
    }
    mkfifo(FIFO_NAME, 0666);
    if(pthread_create(&listen_mgmt_requests_thread, NULL, &listen_mgmt_requests,NULL)) {
        fprintf(stderr, "Error creating thread\n");
        return 1;
    }

    // Main loop
    while(1) {
        while(1) {
            if (g_start_test == 1){
                break;
            }
            if (g_shutdown == 1){
                if(pthread_join(listen_mgmt_requests_thread, NULL)) {
                    fprintf(stderr, "Error joining thread\n");
                    return 1;
                }
                return 0;
            }
        }


        if(pthread_create(&collect_operations_statistics_thread, NULL, &collect_operations_statistics, NULL)) {
            fprintf(stderr, "Error creating thread\n");
            return 1;
        }
        for(int work_num=0;work_num<g_works_count;work_num++) {
            int ops_left = g_works[work_num]->obj_end_num - g_works[work_num]->obj_start_num + 1;
            int obj_start_num=g_works[work_num]->obj_start_num;
            int obj_end_num=obj_start_num;
            int threads_left=g_works[work_num]->thread_count;
            for (int i=0;i<g_works[work_num]->thread_count;i++){
                int ops_for_thread=ceil(ops_left / threads_left);
                obj_end_num+=ops_for_thread-1;
                int pid = fork();
                if (pid == 0){
                    launch_test(work_num, i, g_pool_name,  obj_start_num, obj_end_num, g_works[work_num]->op_type, g_works[work_num]->object_size, g_start_at);
                    return 0;
                }
                // TODO: collect pids of forked processes
                ops_left-=ops_for_thread;
                if (ops_left < 1)
                    break;
                threads_left--;
                obj_start_num=obj_end_num+1;
                obj_end_num=obj_start_num;
            }
        }

        while (1) {
            int status;
            pid_t done = wait(&status);
            if (done == -1) {
                if (errno == ECHILD) break;
            } else {
                if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                    return -1;
                }
            }
            if (g_shutdown == 1){
                //TODO: stop all forked processes
                g_stop_listen=1;
                if(pthread_join(collect_operations_statistics_thread, NULL)) {
                    fprintf(stderr, "Error joining thread\n");
                    return 1;
                }
                if(pthread_join(listen_mgmt_requests_thread, NULL)) {
                    fprintf(stderr, "Error joining thread\n");
                    return 1;
                }
                return 0;
            }
        }

        g_stop_listen=1;

        if(pthread_join(collect_operations_statistics_thread, NULL)) {
            fprintf(stderr, "Error joining thread\n");
            return 1;
        }

        g_stop_listen=0;
        g_start_test=0;
        pthread_mutex_unlock(&g_start_test_lock);
    }

    unlink(FIFO_NAME);

    return 0;
}

/*****
 *
 *
 *
 *****/
int write_object(rados_ioctx_t io_ctx, char* object_name,size_t object_size, int operations_statistics_msg_bus, int work_id, int thread_id){
    long ms_start, ms_end, ms_total;
    struct timespec start_time;
    struct timespec stop_time;
    int ret = 0;
    operation_statistics_t operation_statistics;

    char* buf = (char*)malloc(object_size*sizeof(char));

    clock_gettime(CLOCK_REALTIME, &start_time);
    ret = rados_write_full(io_ctx, object_name, buf, object_size);
    clock_gettime(CLOCK_REALTIME, &stop_time);

    free(buf);

    if (ret < 0) {
        printf("couldn't write object! error %d\n", ret);
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
    operation_statistics.op_type=OPERATION_WRITE;
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

/*****
 *
 *
 *
 *****/
int launch_test(int work_id, int thread_id, char *pool_name, int object_start_num, int object_end_num, int operation_type, long object_size, time_t start_at){
    int ret;

    rados_t rados = NULL;
    rados_ioctx_t io_ctx = NULL;

    #if 1
    //TODO: directly pass mon_ip and keyring without any configuration files
    ret = rados_create(&rados, "admin");
    if (ret < 0) {
        fprintf(stderr,"couldn't initialize rados! error %d\n", ret);
        return 1;
    }
    ret = rados_conf_read_file(rados, CONFIG_FILE);
    if (ret < 0) {
        fprintf(stderr, "failed to parse config file %s! error %d\n", CONFIG_FILE, ret);
        return 1;
    }
    #endif

    ret = rados_connect(rados);
    if (ret < 0) {
        fprintf(stderr,"couldn't connect to cluster! error %d\n", ret);
        return 1;
    }

    ret = rados_ioctx_create(rados, g_pool_name, &io_ctx);
    if (ret < 0) {
        fprintf(stderr,"couldn't set up ioctx! error %d. Pool=%s\n", ret, g_pool_name);
        rados_shutdown(rados);
        return 1;
    }
    while(1) {
        //TODO: change to timespec comparation
        if (time(0)>=g_start_at){
            break;
        }
    }
    int operations_statistics_msg_bus = open(FIFO_NAME, O_WRONLY);
    //TODO: change to more memory safe operations[1]
    char *object_prefix="testobject";
    char *object_name = malloc(strlen(object_prefix)+20);

        for(int object_suffix = object_start_num ; object_suffix <= object_end_num ; object_suffix++){
        //TODO: change to more memory safe operations[2]
        sprintf(object_name, "%s%d", object_prefix, object_suffix);

        if (operation_type==OPERATION_WRITE) {
            ret = write_object(io_ctx,object_name, object_size, operations_statistics_msg_bus, work_id, thread_id);
        } else if (operation_type == OPERATION_READ) {
            ret = read_object(io_ctx,object_name, object_size, operations_statistics_msg_bus, work_id, thread_id);
        } else if (operation_type == OPERATION_DELETE) {
            ret = delete_object(io_ctx, object_name, 0, operations_statistics_msg_bus, work_id, thread_id);
        } else {
            fprintf(stderr,"Unknown operation type\n");
            if (io_ctx) {
                rados_ioctx_destroy(io_ctx);
            }
            rados_shutdown(rados);
            close(operations_statistics_msg_bus);
            free(object_name);
            return 1;
        }

        if (ret < 0) {
            fprintf(stderr,"couldn't perform operation! error %d\n", ret);
            continue;
        }
    }

    free(object_name);
    close(operations_statistics_msg_bus);
    if (io_ctx) {
        rados_ioctx_destroy(io_ctx);
    }
    rados_shutdown(rados);
    return 0;
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
int pars_workload_definition(char *buff) {
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
        char *tmp_str=json_string_value(tmp_object1);
        g_pool_name=(char *)malloc(strlen(tmp_str)*sizeof(char));
        strcpy(g_pool_name,tmp_str);
        free(tmp_str);

    tmp_object1 = json_object_get(root, "start_at");
    if(!json_is_integer(tmp_object1)){
        fprintf(stderr, "error: start_at is not string\n");
        return 1;
    }
    g_start_at=json_integer_value(tmp_object1);

    tmp_object1 = json_object_get(root, "works");
    if(!json_is_array(tmp_object1)){
        fprintf(stderr, "error: start_at is not string\n");
        return 1;
    }

  g_works_count=json_array_size(tmp_object1);
  g_works=malloc((g_works_count)*sizeof(workload_definition_t *));

  for(int i = 0; i < (g_works_count); i++)
  {
    g_works[i]=(workload_definition_t *)malloc(sizeof(workload_definition_t));

    tmp_object2 = json_array_get(tmp_object1, i);
    if(!json_is_object(tmp_object2))
    {
        fprintf(stderr, "error: commit %d is not an object\n", i + 1);
        return -1;
    }
    tmp_object3 = json_object_get(tmp_object2, "thread_count");
    if(!json_is_integer(tmp_object3))
    {
      fprintf(stderr, "error: start_at is not string\n");
      return -1;
    }
    g_works[i]->thread_count=json_integer_value(tmp_object3);

    tmp_object3 = json_object_get(tmp_object2, "obj_start_num");
    if(!json_is_integer(tmp_object3))
    {
      fprintf(stderr, "error: start_at is not string\n");
      return -1;
    }
    g_works[i]->obj_start_num=json_integer_value(tmp_object3);

    tmp_object3 = json_object_get(tmp_object2, "obj_end_num");
    if(!json_is_integer(tmp_object3))
    {
      fprintf(stderr, "error: start_at is not string\n");
      return -1;
    }
    g_works[i]->obj_end_num=json_integer_value(tmp_object3);

    tmp_object3 = json_object_get(tmp_object2, "op_type");
    if(!json_is_integer(tmp_object3))
    {
      fprintf(stderr, "error: start_at is not string\n");
      return -1;
    }
    g_works[i]->op_type=json_integer_value(tmp_object3);

    tmp_object3 = json_object_get(tmp_object2, "object_size");
    if(!json_is_integer(tmp_object3))
    {
      fprintf(stderr, "error: start_at is not string\n");
      return -1;
    }
    g_works[i]->object_size=json_integer_value(tmp_object3);
  }
    return 0;
}

/*****
 *
 *
 *
 *****/
void process_mgmt_request(void *sock_void){
    int sock=*((int *)sock_void);
    int n,ret;
    json_t *root;
    json_error_t error;

    //TODO: change to more inteligent read from buffer
    char buffer[20000];
    bzero(buffer,20000);

    n = read(sock,buffer,20000);
    if (n < 0) {
        fprintf(stderr,"ERROR reading from socket\n");
        close(sock);
        return;
    }

    if (strcmp(buffer,"Status\n")==0) {
        ret = pthread_mutex_trylock(&g_start_test_lock);
        if (ret == 0) {
            pthread_mutex_unlock(&g_start_test_lock);
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
        //TODO
        n = write(sock,"Not implemented yet.\n",21);
        if (n < 0) {
            close(sock);
            fprintf(stderr,"ERROR writing to socket");
            return;
        }
        close(sock);
        return;
    } else if (strcmp(buffer, "Shutdown\n")==0) {
        //TODO
        n = write(sock,"Not implemented yet.\n",21);
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
            ret = pthread_mutex_trylock(&g_start_test_lock);
            if (ret == 0) {
                if(pars_workload_definition(buffer) == 0){
                    json_decref(root);
                    g_start_test=1;
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
                    pthread_mutex_unlock(&g_start_test_lock);
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
void listen_mgmt_requests() {
    int socket_desc, client_sock, c;
    struct sockaddr_in server, client;

    //TODO: add port, IP customization
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_desc == -1){
        fprintf(stderr,"Could not create socket");
    }

    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(PORT_NUMBER);

    if( bind(socket_desc,(struct sockaddr *)&server , sizeof(server)) < 0){
        fprintf(stderr,"bind failed. Error");
        return;
    }

    listen(socket_desc, 3);

    c = sizeof(struct sockaddr_in);
    pthread_t thread_id;

    while( (client_sock = accept(socket_desc, (struct sockaddr *)&client, (socklen_t*)&c)) ){
        if( pthread_create( &thread_id , NULL ,  process_mgmt_request, (void*) &client_sock) < 0){
            fprintf(stderr,"could not create thread");
            return;
        }
    }

    if (client_sock < 0){
        fprintf(stderr,"accept failed");
        return;
    }

    return;
}
