/*
 * datanode.h
 *
 *  Created on: 11/9/2017
 *      Author: utnso
 */

#ifndef DATA_NODE_SRC_DATANODE_H_
#define DATA_NODE_SRC_DATANODE_H_

#include <stdint.h>

typedef struct {
	char * node_name;
	char * worker_ip;
	char * worker_port;
	char * fs_ip;
	char * fs_port;
	char * data_bin_path;
	char * logfile;
} t_dn_conf;

#endif /* DATA_NODE_SRC_DATANODE_H_ */
