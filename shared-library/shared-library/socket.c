/*
 * connect.c
 *
 *  Created on: 9/4/2017
 *      Author: utnso
 */

#include <commons/config.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include "socket.h"

int open_socket(int backlog, int port) {

	struct sockaddr_in server;
	int yes = 1;
	int listenning_socket = socket(AF_INET, SOCK_STREAM, 0);

	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(port);

	setsockopt(listenning_socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

	if (bind(listenning_socket, (struct sockaddr *) &server, sizeof(server)) == -1) {
		fprintf("socket - error en el bind() (error de tipo: %d)\n", errno);
		close(listenning_socket);
	}

	if (listen(listenning_socket, backlog) == -1) {
		fprintf("socket - error en el listen() (error de tipo: %d)\n", errno);
		close(listenning_socket);
	};

	return listenning_socket;
}

int close_socket(int listenning_socket) {
	return close(listenning_socket);
}

int accept_connection(int listenning_socket) {
	struct sockaddr_in client;
	int c = sizeof(struct sockaddr_in);
	int client_sock = accept(listenning_socket, (struct sockaddr *) &client, (socklen_t *) &c);
	return client_sock;
}

int connect_to_socket(char * server_ip, char * server_port) {
	struct addrinfo hints;
	struct addrinfo * server_info;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if (getaddrinfo(server_ip, server_port, &hints, &server_info) != 0) {
		fprintf(stderr, "socket - error en getaddrinfo()\n");
		//exit(1);
	}
	int server_socket = socket(server_info->ai_family, server_info->ai_socktype, server_info->ai_protocol);
	if (server_socket == -1) {
		fprintf(stderr, "socket - error en socket()\n");
		//exit(1);
	}
	if (connect(server_socket, server_info->ai_addr, server_info->ai_addrlen) == -1) {
		close(server_socket);
		fprintf(stderr, "socket - error en connect()\n");
		server_socket = -1;
		//exit(1);
	}
	freeaddrinfo(server_info);
	return server_socket;
}

int socket_send(int * server_socket, void * buffer, int buffer_size, int flags) {
	return send(* server_socket, buffer, buffer_size, flags);
}

int socket_recv(int * client_socket, void * buffer, int buffer_size) {
	return recv(* client_socket, buffer, buffer_size, MSG_WAITALL);
}

int socket_write(int * client_socket, void * response, int response_size) {
	return write(* client_socket, response, response_size);
}

int close_client(int client_socket) {
	return close(client_socket);
}
