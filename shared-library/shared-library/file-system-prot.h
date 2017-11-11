#include <commons/collections/list.h>
#include <commons/collections/node.h>
#include <commons/log.h>
#include <stdint.h>

#ifndef FILE_SYSTEM_PROTOCOL_H_
#define FILE_SYSTEM_PROTOCOL_H_

#define NODE_NAME_LENGTH 	10
#define IP_PORT_LENGTH		25
#define	FS_HANDSHAKE        1
#define UPLOAD_FILE         2
#define	GET_METADATA_FILE   3
#define DATANODE 			'd'
#define YAMA				'y'
#define WORKER 				'w'
#define BINARY 				'b'
#define TEXT 				't'
#define	SUCCESS     				  	   1
#define	ERROR							-200
#define	DISCONNECTED_CLIENT			  	-201
#define	DISCONNECTED_SERVER			   	-202
#define	ALREADY_CONNECTED  				-203 // data-node already connected
#define	ENOSPC						    -204 // no space left on device
#define ENOENT							-205 // no such file or directory
#define	ENOTDIR						    -206 // not a directory
#define	EEXIST						    -207 // file exists
#define	CORRUPTED_FILE					-208 // corrupted file
#define	DISCONNECTED_NODE 				-209 // disconnected node
#define	UNSUPPORTED_FILE_TYPE     		-210 // unsupported file type
#define	UNSTEADY_FS			     		-211 // unsteady fs

/**
 * @NAME fs_recv_operation_code
 * @DESC
 *
 */
int fs_recv_operation_code(int *, t_log *);

typedef struct {
	int16_t exec_code;
	char type;
	char * node_name;
	char * node_ip_port;
	uint32_t blocks;
} t_fs_handshake_req;

/**
 * @NAME fs_handshake
 * @DESC
 *
 */
int fs_handshake(int, char, char *, char *, int, t_log *);

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
	uint32_t file_size;
	void * buffer;
} t_fs_upload_file_req;

/**
 * @NAME fs_upload_file
 * @DESC
 *
 */
int fs_upload_file(int, char *, char, int, void *, t_log *);

/**
 * @NAME fs_upload_file_recv_req
 * @DESC
 *
 */
t_fs_upload_file_req * fs_upload_file_recv_req(int *, t_log *);

/**
 * @NAME fs_upload_file_send_resp
 * @DESC
 *
 */
void fs_upload_file_send_resp(int *, int);

typedef struct {
	char node[NODE_NAME_LENGTH];
	char ip_port[IP_PORT_LENGTH];
	int32_t node_block;
} t_fs_copy_block;

typedef struct {
	uint32_t file_block;
	uint32_t size;
	t_list * copies_list;
} t_fs_file_block_metadata;

typedef struct {
	char * path;
	uint32_t file_size;
	char type;
	t_list * block_list;
} t_fs_metadata_file;

typedef struct {
	int16_t exec_code;
	t_fs_metadata_file * metadata_file;
} t_fs_get_md_file_resp;

typedef struct {
	int16_t exec_code;
	char * path;
} t_fs_get_md_file_req;

/**
 * @NAME fs_get_metadata_file
 * @DESC
 *
 */
t_fs_get_md_file_resp * fs_get_metadata_file(int, char *, t_log *);

/**
 * @NAME fs_get_metadata_file_recv_req
 * @DESC
 *
 */
t_fs_get_md_file_req * fs_get_metadata_file_recv_req(int *, t_log *);

/**
 * @NAME fs_get_metadata_file_send_resp
 * @DESC
 *
 */
void fs_get_metadata_file_send_resp(int *, int, t_fs_metadata_file *);

#endif /* FILE_SYSTEM_PROTOCOL_H_ */
