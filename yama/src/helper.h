/*
 * helper.h
 *
 *  Created on: 3/10/2017
 *      Author: utnso
 */

#ifndef HELPER_H_
#define HELPER_H_

	typedef struct{
		int port;
		fd_set* master;
	//	fd_set lectura;
	}t_struct;

	t_struct* create_struct();

#endif /* HELPER_H_ */
