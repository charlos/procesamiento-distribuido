
#include <commons/log.h>
#include <stdint.h>

#ifndef FILE_SYSTEM_PROTOCOL_H_
#define FILE_SYSTEM_PROTOCOL_H_

#define	FS_HANDSHAKE        1
#define LOAD_FILE           2
#define	READ_FILE           3

#define	SUCCESS     				  	   1
#define	DISCONNECTED_CLIENT			  	-201
#define	DISCONNECTED_SERVER			   	-202

/**
 * @NAME fs_recv_operation_code
 * @DESC
 *
 */
int fs_recv_operation_code(int *, t_log *);

typedef struct {
	int16_t exec_code;
	char type;
} t_fs_handshake_req;

/**
 * @NAME fs_handshake
 * @DESC
 *
 */
int fs_handshake(int, char, t_log *);

/**
 * @NAME fs_handshake_recv_req
 * @DESC
 *
 */
t_fs_handshake_req * fs_handshake_recv_req(int *, t_log *);

/**
 * @NAME fs_handshake_send_resp
 * @DESC
 *
 */
void fs_handshake_send_resp(int *, int);

typedef struct {
	int16_t exec_code;
	char * path;
	char type;
	void * buffer;
} t_fs_load_file_req;

/**
 * @NAME fs_load_file
 * @DESC
 *
 */
int fs_load_file(int, char *, char, int, void *, t_log *);

/**
 * @NAME fs_load_file_recv_req
 * @DESC
 *
 */
t_fs_load_file_req * fs_load_file_recv_req(int *, t_log *);

/**
 * @NAME fs_load_file_send_resp
 * @DESC
 *
 */
void fs_load_file_send_resp(int *, int);

typedef struct {
	int16_t exec_code;
	char * path;
} t_fs_read_file_req;

typedef struct {
	int16_t exec_code;
	uint32_t buffer_size;
	void * buffer;
} t_fs_read_file_resp;

/**
 * @NAME fs_read_file
 * @DESC
 *
 */
t_fs_read_file_resp * fs_read_file(int, char *, t_log *);

/**
 * @NAME fs_read_file_recv_req
 * @DESC
 *
 */
t_fs_read_file_req * fs_read_file_recv_req(int *, t_log *);

/**
 * @NAME fs_read_file_send_resp
 * @DESC
 *
 */
void fs_read_file_send_resp(int *, int, int, void *);

#endif /* FILE_SYSTEM_PROTOCOL_H_ */
