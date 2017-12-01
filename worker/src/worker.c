/*
 ============================================================================
 Name        : worker.c
 Author      : Carlos Flores
 Version     :
 Copyright   : GitHub @Charlos
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "worker.h"

int listenning_socket;
int pipe_padreAHijo[2];
int pipe_hijoAPadre[2];
int new_socket;
pid_t pid;
int status;
t_worker_conf* worker_conf;
t_log* logger;
//FILE *fptr;
void * data_bin_mf_ptr;

int main(int argc, char * argv[]) {

	if (argc != 2) {
		printf(
				"ERROR: Cantidad de parametros invalida. Deben indicar ruta del archvo de configuración");
		exit(0);
	}

	void* buffer;
	//int child_status;
	//int buffer_size;
	uint8_t task_code;
	char* script_filename = string_new();
	string_append(&script_filename,PATH);
	string_append(&script_filename,"script.sh");

	//Crear log
	crear_logger("./worker", &logger, true, LOG_LEVEL_TRACE);
	log_trace(logger, "Proceso Worker iniciando");

	//Cargar archivo de configuración del nodo
	load_properties(argv[1]);

	listenning_socket = open_socket(SOCKET_BACKLOG, (worker_conf->worker_port));
	while (1) {
		uint16_t exec_code_recv;
		new_socket = accept_connection(listenning_socket);
		log_trace(logger, "Nuevo socket N° %d", new_socket);
		//Recibo el código de operación para saber que tarea recibir
		int received_bytes = socket_recv(&new_socket, &task_code, sizeof(uint8_t));

		if (received_bytes <= 0) {
			log_error(logger, "WORKER - Problema al recibir código de operación >> disconnected");
			return 1;
		}
		switch (task_code) {
			case TRANSFORM_OC:{
				buffer = transform_req_recv(new_socket, logger);
				t_request_transformation* request = (t_request_transformation*)buffer;
				exec_code_recv = request->exec_code;
				send_recv_status(new_socket, exec_code_recv);
				break;
			}
			case REDUCE_LOCALLY_OC:{
				buffer  = local_reduction_req_recv(new_socket, logger);
				t_request_local_reduction* request = (t_request_local_reduction*)buffer;
				exec_code_recv = request->exec_code;
				send_recv_status(new_socket, exec_code_recv);
				break;
			}
			case REDUCE_GLOBAL_OC:{
				buffer = global_reduction_req_recv(new_socket, logger);
				t_request_global_reduction * request = (t_request_global_reduction*) buffer;
				exec_code_recv = request->exec_code;
				send_recv_status(new_socket, exec_code_recv);
				break;
			}
			case STORAGE_OC:{
				buffer = final_storage_req_recv(new_socket, logger);
				t_request_storage_file * request = (t_request_storage_file*) buffer;
				exec_code_recv = request->exec_code;
				send_recv_status(new_socket, exec_code_recv);
				break;
			}
			case REDUCE_GLOBAL_OC_N:{
				// worker NO designado
				// TODO Mejorar protocolo de comunicacion aca. Esta cabeza
				t_request_local_reducion_filename* filename_struct = local_reduction_file_req_recv(new_socket, logger);
				filename_struct->fd = new_socket;
				buffer = filename_struct;
				exec_code_recv=filename_struct->exec_code;
				break;
			}
			default:
				log_error(logger,"WORKER - Código de tarea inválido: %d", task_code);
				break;
		}

		if(exec_code_recv == SUCCESS){

			int status_child1;
			pid_t pid_final;
		     if (pid = fork()) {
		             waitpid(pid, &status_child1, NULL);
					 //Se cierra el socket en el padre
		             close(new_socket);
		     } else if (!pid) {
		             if (pid_final = fork()) {
		                     exit(0);
		             } else if (!pid_final) {
		            	 log_trace(logger, "Proceso Hijo PID %d (PID del padre: %d)",getpid(),getppid());

		            	 int child_status = processRequest(task_code, buffer);
		            	 //child_status = SUCCESS;
		            	 int resp_status;
		            	 if(task_code != REDUCE_GLOBAL_OC_N){
		            		 resp_status = task_response_send(new_socket,task_code, child_status, logger);
		            		 if(resp_status==SUCCESS){
		            			 log_trace(logger, "WORKER - El resultado de la etapa fue enviado a Master correctamente");
		            		 }else{
		            			 // log_error(logger,"WORKER - Error al enviar resultado de la etapa %d a Master: %d",task_code, new_socket);
		            		 }
		            	 }
		            	 // En la ejecución del hijo libero el buffer recibido cuando se termino de utilizar
		            	 free_request(task_code, buffer);
		            	 log_trace(logger, "WORKER - Fin de Proceso Hijo PID %d",getpid());
		            	 //cierro el hijo
		            	 exit(0);
		             } else {
		                     /* error */
		             }
		     } else {
		             /* error */
		     }





/*
		   if ((pid=fork()) == 0 ){
			  log_trace(logger, "Proceso Hijo PID %d (PID del padre: %d)",getpid(),getppid());

			  int child_status = processRequest(task_code, buffer);
			  //child_status = SUCCESS;
			  int resp_status;
			  if(task_code != REDUCE_GLOBAL_OC_N){
				  resp_status = task_response_send(new_socket,task_code, child_status, logger);
				  if(resp_status==SUCCESS){
					  log_trace(logger, "WORKER - El resultado de la etapa fue enviado a Master correctamente");
				  }else{
					 // log_error(logger,"WORKER - Error al enviar resultado de la etapa %d a Master: %d",task_code, new_socket);
				  }
			  }
			  // En la ejecución del hijo libero el buffer recibido cuando se termino de utilizar
			  free_request(task_code, buffer);
			  log_trace(logger, "WORKER - Fin de Proceso Hijo PID %d",getpid());
			  //cierro el hijo
			  exit(0);
		   }
		   else {
			    //Se cierra el socket en el padre
				close(new_socket);
		   }
		   */
		}else{
			//Worker no recibió bien el pedido de parte de Master
			log_error(logger,"WORKER - Error al recibir pedido: %d", exec_code_recv);
		    //Se cierra el socket en el padre
			close(new_socket);
		}

	}

	close_socket(listenning_socket);

	return EXIT_SUCCESS;
}
