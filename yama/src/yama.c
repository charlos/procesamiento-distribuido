/*
 ============================================================================
 Name        : yama.c
 Author      : Carlos Flores
 Version     :
 Copyright   : GitHub @Charlos
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "yama.h"

void closure(t_fs_file_block_metadata *);

int main(void) {

	char * fs_ip = "127.0.0.1";
	char * fs_port = "5003";
	int file_system_socket = connect_to_socket(fs_ip, fs_port);

	char * path = "/user/juan/datos/datos_personales.csv";


	TDE_create();

	recibir_solicitudes_master();

	return EXIT_SUCCESS;
}

void recibir_solicitudes_master(){

	t_struct *estructura;
	estructura = create_struct();
	int listening_socket;
	listening_socket = open_socket(20, estructura->port);
	int nuevaConexion, fd_seleccionado, recibido, set_fd_max, i;
	uint8_t operation_code;
	char* buffer;
	int status;
	fd_set lectura;
	pthread_attr_t attr;
	set_fd_max = listening_socket;
	FD_ZERO(&lectura);
	FD_ZERO((estructura->master));
	FD_SET(listening_socket, (estructura->master));
	while(1){
		lectura = *(estructura->master);
		select(set_fd_max +1, &lectura, NULL, NULL, NULL);
		for(fd_seleccionado = 0 ; fd_seleccionado <= set_fd_max ; fd_seleccionado++){
			if(FD_ISSET(fd_seleccionado, &lectura)){
				if(fd_seleccionado == listening_socket){
					if((nuevaConexion = accept_connection(listening_socket)) == -1){
						log_error(logger, "Error al aceptar conexion");
					} else {
						log_trace(logger, "Nueva conexion: socket %d", nuevaConexion);
						FD_SET(nuevaConexion, (estructura->master));
						if(nuevaConexion > set_fd_max)set_fd_max = nuevaConexion;

					}
				} else {
					t_info_socket_solicitud* info_solicitud = malloc(sizeof(t_info_socket_solicitud));

					info_solicitud->file_descriptor = fd_seleccionado;
					info_solicitud->set = estructura->master;
					info_solicitud->lectura = &lectura;

					status = connection_recv(fd_seleccionado, &operation_code, &buffer);

					info_solicitud->operation_code = operation_code;
					info_solicitud->buffer = buffer;

					if(status <= 0 ){
						//si es una desconexion no atiendo el pedido
						FD_CLR(fd_seleccionado, estructura->master);

					}else{
						atender_solicitud_master(info_solicitud);
					}
				}
			}
		}
	}
}


void atender_solicitud_master(t_info_socket_solicitud* info_solicitud){
	switch(info_solicitud->operation_code){
	case OC_TRANSFORMACIONES:
		inicializar();
		etapa_transformacion();
		break;
	case OC_RESULTADO_TRANSFORMACION:
		atender_resultado_transformacion();
		break;
	case OC_RESULTADO_REDUCCION_LOCAL:
		atender_resultado_reduccion_local();
		break;
	case OC_RESULTADO_REDUCCION_GLOBAL:
		atender_resultado_reduccion_global();
		break;
	case OC_RESULTADO_ALMACENAMIENTO_FINAL:
		atender_resultado_almacenamiento_final();
		break;
	default:
		log_error(logger, "Codigo de operacion desconocido");
		//logear ERROR
	}
}

void inicializar(){
	TDE_add();
}

void etapa_transformacion(){
	// se conecta al FS para obtener info del archivo
	t_list* list_infoArchivo;
	list_infoArchivo = solicitar_info_archivo();
	t_list* lista_transformaciones;
	lista_transformaciones = planificar_transformacion(list_infoArchivo);

	enviar_lista_transformacion(lista_transformaciones);
}

void atender_resultado_transformacion(){
	if(tansformacion_ok()){
		TDE_add();
		if(es_ultima_transformacion_del_nodo()){
			etapa_reduccion_local();
		}
	} else {

	}
}

void etapa_reduccion_local(){
	char* nombre = obtener_nombre_aleatorio();
	t_orden_reducciones_locales orden_rl = create_orden_reducciones_locales();
	orden_rl = obtener_reducciones_locales(nombre);

	enviar_orden_reduccion_local(orden_rl);
}

void atender_resultado_reduccion_local(){
	if(reduccion_local_ok()){
		TDE_add();
		if(es_ultima_reduccion_local_del_nodo()){
			etapa_reduccion_global();
		}
	} else {

	}
}

void etapa_reduccion_global(){
	char* nombre = obtener_nombre_aleatorio();
	t_orden_reducciones_globales orden_rg = create_orden_reducciones_globales();
	orden_rg = obtener_reducciones_globales(nombre);

	enviar_orden_reduccion_global(orden_rg);
}

void atender_resultado_reduccion_global(){
	if(reduccion_global_ok()){
		TDE_add();
		if(es_ultima_reduccion_global_del_nodo()){
			etapa_almacenamiento_final();
		}
	} else {

	}
}

void etapa_almacenamiento_final(){
	char* nombre = obtener_nombre_aleatorio();
	t_orden_almacenamiento_final orden_almacenamiento = create_orden_almacenamiento_final();
	orden_almacenamiento = obtener_almacenamiento_final(nombre);

	enviar_orden_almacenamiento_final(orden_almacenamiento);
}

void atender_resultado_almacenamiento_final(){
	if(almacenamiento_final_ok()){
		TDE_add();

	} else {

	}
}

void closure(t_fs_file_block_metadata * block_md) {
	free(block_md);
}
