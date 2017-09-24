/*
 * master-prot.c
 *
 *  Created on: 24/9/2017
 *      Author: utnso
 */


int transform_res_send(int master_socket, int result) {
	int status = socket_send(master_socket, &result, sizeof(int), 0);
	return status;
}
int transform_res_recv(int worker_socket, int * result) {
	int status = socket_recv(worker_socket, result, sizeof(int));
	return status;
}
