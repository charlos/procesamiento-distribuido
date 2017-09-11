/*
 ============================================================================
 Name        : master.c
 Author      : Carlos Flores
 Version     :
 Copyright   : GitHub @Charlos
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "master.h"

int main(void) {
	char* hora = temporal_get_string_time();
	printf("Hora actual: %s\n", hora); /* prints !!!Hello World!!! */
	connect_send(hora);
	return EXIT_SUCCESS;
}
