/*
 * generales.h
 *
 *  Created on: 13/9/2017
 *      Author: utnso
 */

#ifndef GENERALES_H_
#define GENERALES_H_

#include <commons/log.h>
#include <commons/string.h>



char* obtener_nombre_proceso(char*);
void crear_logger(char*, t_log**, bool, t_log_level);
#endif /* GENERALES_H_ */
