/*
 * memory_protocol.h
 *
 *  Created on: 22/4/2017
 *      Author: Dante Romero
 */

#include <commons/log.h>
#include <stdint.h>
#include <stdlib.h>

#ifndef SHARED_LIBRARY_SADICA_PROTOCOL_H_
#define SHARED_LIBRARY_SADICA_PROTOCOL_H_

#define	FS_HANDSHAKE_OC					 1
#define FS_VALIDATE_FILE_OC     		 2
#define FS_CREATE_FILE_OC     			 3
#define	FS_DELETE_FILE_OC 				 4
#define	FS_READ_FILE_OC			 		 5
#define	FS_WRITE_FILE_OC				 6

#define	SUCCESS							 1
#define ISREG 							 2 // it's a regular file
#define ISDIR 							 3 // it's a directory
#define	ERROR						   	-200
#define	DISCONNECTED_CLIENT			  	-201
#define	DISCONNECTED_SERVER			   	-202
#define	ENOSPC							-203 // no space left on device
#define	ENOENT							-204 // no such file or directory
#define ISNOTREG						-205 // it isn't a regular file


/**	╔═════════════════════════════╗
	║ FS - RECEIVE OPERATION CODE ║
	╚═════════════════════════════╝ **/

/**
 * @NAME fs_recv_ope_cod
 * @DESC
 *
 * @PARAMS
 */
int fs_recv_ope_cod(int *, t_log *);



/**	╔════════════════╗
	║ FS - HANDSHAKE ║
	╚════════════════╝ **/

/**
 * @NAME fs_handshake
 * @DESC
 *
 * @PARAMS
 */
int fs_handshake(int *, t_log *);

/**
 * @NAME fs_handshake_resp
 * @DESC
 *
 * @PARAMS
 */
void fs_handshake_resp(int *, int);



/**	╔════════════════════╗
	║ FS - VALIDATE FILE ║
	╚════════════════════╝ **/

typedef struct {
	int16_t exec_code;
	char * path;
} t_v_file_req;

/**
 * @NAME fs_validate_file
 * @DESC
 *
 * @PARAMS
 */
int fs_validate_file(int, char *, t_log *);

/**
 * @NAME v_file_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_v_file_req * v_file_recv_req(int *, t_log *);

/**
 * @NAME v_file_send_resp
 * @DESC
 *
 * @PARAMS
 */
void v_file_send_resp(int *, int);



/**	╔══════════════════╗
	║ FS - CREATE FILE ║
	╚══════════════════╝ **/

typedef struct {
	int16_t exec_code;
	char * path;
} t_c_file_req;

/**
 * @NAME fs_create_file
 * @DESC
 *
 * @PARAMS
 */
int fs_create_file(int, char *, t_log *);

/**
 * @NAME c_file_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_c_file_req * c_file_recv_req(int *, t_log *);

/**
 * @NAME c_file_send_resp
 * @DESC
 *
 * @PARAMS
 */
void c_file_send_resp(int *, int);



/**	╔══════════════════╗
	║ FS - DELETE FILE ║
	╚══════════════════╝ **/

typedef struct {
	int16_t exec_code;
	char * path;
} t_d_file_req;

/**
 * @NAME fs_delete_file
 * @DESC
 *
 * @PARAMS
 */
int fs_delete_file(int, char *, t_log *);

/**
 * @NAME d_file_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_d_file_req * d_file_recv_req(int *, t_log *);

/**
 * @NAME d_file_send_resp
 * @DESC
 *
 * @PARAMS
 */
void d_file_send_resp(int *, int);



/**	╔═══════════╗
	║ FS - READ ║
	╚═══════════╝ **/

typedef struct {
	int16_t exec_code;
	char * path;
	uint32_t offset;
	uint32_t size;
} t_fs_read_req;

typedef struct {
	int16_t exec_code;
	uint32_t buffer_size;
	void * buffer;
} t_fs_read_resp;

/**
 * @NAME fs_read
 * @DESC
 *
 * @PARAMS
 */
t_fs_read_resp * fs_read(int, char *, int, int, t_log *);

/**
 * @NAME fs_read_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_fs_read_req * fs_read_recv_req(int *, t_log *);

/**
 * @NAME fs_read_send_resp
 * @DESC
 *
 * @PARAMS
 */
void fs_read_send_resp(int *, int, int, void *);



/**	╔════════════╗
	║ FS - WRITE ║
	╚════════════╝ **/

typedef struct {
	int16_t exec_code;
	char * path;
	uint32_t offset;
	uint32_t size;
	void * buffer;
} t_fs_write_req;

/**
 * @NAME fs_write
 * @DESC
 *
 * @PARAMS
 */
int fs_write(int, char *, int, int, int, void *, t_log *);

/**
 * @NAME fs_write_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_fs_write_req * fs_write_recv_req(int *, t_log *);

/**
 * @NAME fs_write_send_resp
 * @DESC
 *
 * @PARAMS
 */
void fs_write_send_resp(int *, int);

#endif /* SHARED_LIBRARY_SADICA_PROT_H_ */
