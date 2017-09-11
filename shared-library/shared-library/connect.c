/*
 * connect.c
 *
 *  Created on: 9/4/2017
 *      Author: utnso
 */

int connect_send(char* mensaje) {
	char* timeSend = temporal_get_string_time();
	printf("SEND: %s\n", mensaje);
	printf("Hora de envio del mensaje: %s\n", timeSend);
}
