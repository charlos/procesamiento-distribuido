/*
 * master-prot.h
 *
 *  Created on: 24/9/2017
 *      Author: utnso
 */

#ifndef MASTER_PROT_H_
#define MASTER_PROT_H_

int transform_res_send(int master_socket, int result);
int transform_res_recv(int worker_socket, int * result);

#endif /* MASTER_PROT_H_ */
