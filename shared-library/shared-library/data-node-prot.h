
#include <commons/log.h>
#include <stdint.h>

#ifndef DATA_NODE_PROTOCOL_H_
#define DATA_NODE_PROTOCOL_H_

#define BLOCK_SIZE 			1048576

#define	GET_BLOCK           1
#define SET_BLOCK           2

#define	SUCCESS     				  	   1
#define	DISCONNECTED_CLIENT			  	-201
#define	DISCONNECTED_SERVER			   	-202

/**
 * @NAME dn_recv_operation_code
 * @DESC
 *
 */
int dn_recv_operation_code(int *, t_log *);

typedef struct {
	int16_t exec_code;
	int32_t block;
} t_dn_get_block_req;

typedef struct {
	int16_t exec_code;
	void * buffer;
} t_dn_get_block_resp;

/**
 * @NAME dn_get_block
 * @DESC
 *
 */
int dn_get_block(int, int, t_log *);

/**
 * @NAME dn_get_block_recv_req
 * @DESC
 *
 */
t_dn_get_block_req * dn_get_block_recv_req(int *, t_log *);

/**
 * @NAME dn_get_block_send_resp
 * @DESC
 *
 */
void dn_get_block_send_resp(int *, int, void *);

typedef struct {
	int16_t exec_code;
	int32_t block;
	void * buffer;
} t_dn_set_block_req;

/**
 * @NAME dn_set_block
 * @DESC
 *
 */
int dn_set_block(int, int, void *, t_log *);

/**
 * @NAME dn_set_block_recv_req
 * @DESC
 *
 */
t_dn_set_block_req * dn_set_block_recv_req(int *, t_log *);

/**
 * @NAME dn_set_block_send_resp
 * @DESC
 *
 */
void dn_set_block_send_resp(int *, int);

#endif /* DATA_NODE_PROTOCOL_H_ */
