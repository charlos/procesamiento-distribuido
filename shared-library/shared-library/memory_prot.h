/*
 * memory_protocol.h
 *
 *  Created on: 22/4/2017
 *      Author: Dante Romero
 */

#include <commons/log.h>
#include <stdint.h>
#include <stdlib.h>

#ifndef SHARED_LIBRARY_MEMORY_PROTOCOL_H_
#define SHARED_LIBRARY_MEMORY_PROTOCOL_H_

#define	HANDSHAKE_OC					1
#define INIT_PROCESS_OC     			2
#define	ASSIGN_PAGE_OC 					3
#define	DELETE_PAGE_OC					4
#define	READ_OC			 				5
#define	WRITE_OC						6
#define	END_PROCESS_OC					7

#define	SUCCESS							1
#define	ERROR							-200
#define	DISCONNECTED_CLIENT				-201
#define	DISCONNECTED_SERVER				-202
#define	ENOSPC							-203
#define	OUT_OF_FRAME				  	-204
#define	PAGE_FAULT						-205
#define	SEGMENTATION_FAULT				-206
#define UNDEFINED_STACK_SIZE			-207


/**	╔═════════════════════════════════╗
	║ MEMORY - RECEIVE OPERATION CODE ║
	╚═════════════════════════════════╝ **/

/**
 * @NAME recv_operation_code
 * @DESC
 *
 * @PARAMS
 */
int recv_operation_code(int *, t_log *);



/**	╔════════════════════╗
	║ MEMORY - HANDSHAKE ║
	╚════════════════════╝ **/

typedef struct {
	int16_t exec_code;
	char type;
	uint8_t stack_size;
} t_handshake_request;

/**
 * @NAME handshake
 * @DESC
 *
 * @PARAMS
 */
int handshake(int, char, int, t_log *);

/**
 * @NAME handshake_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_handshake_request * handshake_recv_req(int *, t_log *);

/**
 * @NAME handshake_resp
 * @DESC
 *
 * @PARAMS
 */
void handshake_resp(int *, int);



/**	╔═══════════════════════╗
	║ MEMORY - INIT PROCESS ║
	╚═══════════════════════╝ **/

typedef struct {
	int16_t exec_code;
	uint32_t pid;
	uint32_t pages;
} t_init_process_request;

/**
 * @NAME memory_init_process
 * @DESC
 *
 * @PARAMS
 */
int memory_init_process(int, int, int, t_log *);

/**
 * @NAME init_process_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_init_process_request * init_process_recv_req(int *, t_log *);

/**
 * @NAME init_process_send_resp
 * @DESC
 *
 * @PARAMS
 */
void init_process_send_resp(int *, int);



/**	╔════════════════╗
	║ MEMORY - WRITE ║
	╚════════════════╝ **/

typedef struct {
	int16_t exec_code;
	uint32_t pid;
	uint32_t page;
	uint32_t offset;
	uint32_t size;
	void * buffer;
} t_write_request;

/**
 * @NAME memory_write
 * @DESC
 *
 * @PARAMS
 */
int memory_write(int, int, int, int, int, int, void *, t_log *);

/**
 * @NAME write_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_write_request * write_recv_req(int *, t_log *);

/**
 * @NAME write_send_resp
 * @DESC
 *
 * @PARAMS
 */
void write_send_resp(int *, int);



/**	╔═══════════════╗
	║ MEMORY - READ ║
	╚═══════════════╝ **/

typedef struct {
	int16_t exec_code;
	uint32_t pid;
	uint32_t page;
	uint32_t offset;
	uint32_t size;
} t_read_request;

typedef struct {
	int16_t exec_code;
	uint32_t buffer_size;
	void * buffer;
} t_read_response;

/**
 * @NAME memory_read
 * @DESC
 *
 * @PARAMS
 */
t_read_response * memory_read(int, int, int, int, int, t_log *);

/**
 * @NAME read_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_read_request * read_recv_req(int *, t_log *);

/**
 * @NAME read_send_resp
 * @DESC
 *
 * @PARAMS
 */
void read_send_resp(int *, int, int, void *);



/**	╔═══════════════════════╗
	║ MEMORY - ASSIGN PAGES ║
	╚═══════════════════════╝ **/

typedef struct {
	int16_t exec_code;
	uint32_t pid;
	uint32_t pages;
} t_assign_pages_request;

/**
 * @NAME memory_assign_pages
 * @DESC
 *
 * @PARAMS
 */
int memory_assign_pages(int, int, int, t_log *);

/**
 * @NAME assign_pages_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_assign_pages_request * assign_pages_recv_req(int *, t_log *);

/**
 * @NAME assign_pages_send_resp
 * @DESC
 *
 * @PARAMS
 */
void assign_pages_send_resp(int *, int);



/**	╔══════════════════════╗
	║ MEMORY - DELETE PAGE ║
	╚══════════════════════╝ **/

typedef struct {
	int16_t exec_code;
	uint32_t pid;
	uint32_t page;
} t_delete_page_request;

/**
 * @NAME memory_delete_page
 * @DESC
 *
 * @PARAMS
 */
int memory_delete_page(int, int, int, t_log *);

/**
 * @NAME delete_page_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_delete_page_request * delete_page_recv_req(int *, t_log *);

/**
 * @NAME delete_page_send_resp
 * @DESC
 *
 * @PARAMS
 */
void delete_page_send_resp(int *, int);



/**	╔═══════════════════════════╗
	║ MEMORY - FINALIZE PROCESS ║
	╚═══════════════════════════╝ **/

typedef struct {
	int16_t exec_code;
	uint32_t pid;
} t_finalize_process_request;

/**
 * @NAME memory_finalize_process
 * @DESC
 *
 * @PARAMS
 */
int memory_finalize_process(int, int, t_log *);

/**
 * @NAME finalize_process_recv_req
 * @DESC
 *
 * @PARAMS
 */
t_finalize_process_request * finalize_process_recv_req(int *, t_log *);

/**
 * @NAME finalize_process_send_resp
 * @DESC
 *
 * @PARAMS
 */
void finalize_process_send_resp(int *, int);

#endif /* SHARED_LIBRARY_MEMORY_PROT_H_ */
