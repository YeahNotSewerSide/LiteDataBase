#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <iphlpapi.h>
#include <stdio.h>
#include <iostream>
#include <thread>         // std::thread
#include <chrono>
#include <mutex>          // std::mutex
using namespace std;

#pragma comment(lib, "Ws2_32.lib")
#include "DataBase.h"

#define DEFAULT_PORT "666\0"
#define DEFAULT_SIZE_PACKET sizeof(long)
#define MAX_THREADS 10
unsigned int pool_size = 10;
unsigned int sockets_count = 0;
SOCKET* sockets_pool = new SOCKET[pool_size];

DB* dbs;
unsigned int dbs_count = 0;

const char ALREADY_EXISTS[] = "Already exists\0";

struct addrinfo* result = NULL, * ptr = NULL, hints ;

char* root_path;
char* parse_main_path(char* path) {
	char* parsed;
	size_t counter = strlen(path);
	while (counter >= 0) {
		if (path[counter] == '\\') {
			break;
		}
		counter--;
	}
	counter++;
	parsed = new char[counter + 1];
	memcpy(parsed, path, counter);
	parsed[counter] = '\0';
	return parsed;
}


mutex mtx_new_connection;           // mutex for critical section
mutex new_db;
mutex mtx_column;

void work_with_db(SOCKET* sock,bool* alive) {
	*alive = true;
	SOCKET socket;
	memcpy(&socket,sock,sizeof(SOCKET));
	fd_set fds;
	
	struct timeval tv;
	FD_ZERO(&fds);
	FD_SET(socket, &fds);
	tv.tv_sec = 60;
	tv.tv_usec = 0;

	unsigned int packet_size=1;
	int iResult = 1;
	char* packet;
	
	char* type=NULL;
	char* data= NULL;
	
	
	while (iResult>0) {
		if (select(socket, &fds, NULL, NULL, &tv)== SOCKET_ERROR) {
			break;
		}
		iResult = recv(socket, (char*)&packet_size, sizeof(int), 0);
		if (iResult <= 0) {
			break;
		}
		packet = new char[packet_size];
		if (select(socket, &fds, NULL, NULL, &tv) == SOCKET_ERROR) {
			break;
		}
		iResult = recv(socket, packet, packet_size, 0);
		
		/*for (int i=0; i < packet_size; i++) {
			cout << packet[i] << endl;
		}*/
		// \x11\x00\x00\x00
		if (strcmp(packet,"ndb\0")==0) {//New Data base || ndb\0 name\0   \x10\x00\x00\x00   \x10\x00\x00\x00
										//				   cmd   db name  count of columns	  count of rows
			new_db.lock();
			DB* buf = new DB[dbs_count+1];			
			if (dbs_count > 0) {				
				memcpy(buf, dbs, dbs_count * sizeof(DB));				
				delete[] dbs;				
			}
			
			dbs = buf;
			dbs[dbs_count].init(&packet[4], *(int*)(&packet[packet_size-8]), *(int*)(&packet[packet_size - 4]));
			dbs_count++;
			new_db.unlock();
			
		}
		else if (strcmp(packet, "ico\0") == 0) {//init column || ico\0 \x00\x00\x00\x00  \x00\x00\x00\x00		name\0			type\0
												//				 cmd    number of db		column number	   column name    type of column
			new_db.lock();
			if (*(int*)(&packet[4]) >= dbs_count || *(int*)(&packet[4]) < 0) {
				break;
				
			}
			dbs[*(int*)(&packet[4])].init_column(*(unsigned int*)(&packet[8]),&packet[12],&packet[12+strlen(&packet[12])+1]);
			new_db.unlock();
		}
		else if(strcmp(packet, "aco\0") == 0){// append column || aco\0 \x00\x00\x00\x00  name\0			type\0  
											  //				  cmd    number of db	  column name    type of column	
			if (*(int*)(&packet[4]) >= dbs_count || *(int*)(&packet[4]) < 0) {
				break;
				
			}
			new_db.lock();
			dbs[*(int*)(&packet[4])].append_column(&packet[8],&packet[8+strlen(&packet[8])+1]);
			new_db.unlock();
		}
		else if (strcmp(packet, "sva\0") == 0) {// set value || sva\0 \x00\x00\x00\x00  name_of_column\0  \x00\x00\x00\x00  data
												//				    cmd    number of db		name_of_column	  cell number	    data
			if (*(int*)(&packet[4]) >= dbs_count || *(int*)(&packet[4]) < 0) {
				break;
				
			}
			new_db.lock();
			dbs[*(int*)(&packet[4])].set_value(&packet[8], *(unsigned int*)(&packet[8+strlen(&packet[8]) + 1]), &packet[8+strlen(&packet[8]) + 5]);
			new_db.unlock();
		}
		else if (strcmp(packet, "gro\0") == 0) {// get whole row || gro\0 \x00\x00\x00\x00    \x00\x00\x00\x00
												//				    cmd        number of db	   row
			size_t size = 0;
			for (unsigned int i=0; i < dbs[*(int*)(&packet[4])].get_count_of_columns(); i++) {
				size = size + dbs[*(int*)(&packet[4])].get_size(i, *(int*)(&packet[8]));
			}

			char* data; 
			iResult = send(socket, (char*)& size, sizeof(size), 0);
			if (iResult > 0) {
				data = new char[size];
				size_t spec_counter = 0;
				for (unsigned int i=0; i < dbs[*(int*)(&packet[4])].get_count_of_columns(); i++) {
					memcpy(&data[spec_counter], dbs[*(int*)(&packet[4])].get_value(i, *(int*)(&packet[8])), dbs[*(int*)(&packet[4])].get_size(i, *(int*)(&packet[8])));
					spec_counter += dbs[*(int*)(&packet[4])].get_size(i, *(int*)(&packet[8]));
				}
				iResult = send(socket, data, size, 0);
				delete[] data;
			}


		}
		else if (strcmp(packet, "gva\0") == 0) {// get value || gva\0 \x00\x00\x00\x00  name_of_column\0  \x00\x00\x00\x00  
												//				cmd    number of db		name_of_column	  cell number
			if (*(int*)(&packet[4]) >= dbs_count || *(int*)(&packet[4]) < 0) {
				break;
				
			}
			unsigned int size=0;
			char* data;
			new_db.lock();
			new_db.unlock();
			if (dbs[*(int*)(&packet[4])].cell_is_empty(&packet[8], *(unsigned int*)(&packet[packet_size - 4]))) {
				data = new char[1];
				data[0] = '\0';
				iResult = send(socket, data, 1, 0);
				delete[] data;
				
			}
			else {
				data = (char*)dbs[*(int*)(&packet[4])].get_value(&packet[8], *(unsigned int*)(&packet[packet_size - 4]));
				type = dbs[*(int*)(&packet[4])].get_type(&packet[8]);
				if (strcmp(type, types.str) == 0) {
					size = strlen(data) + 1;
				}
				else if (strcmp(type, types.integer) == 0 || strcmp(type, types.uinteger) == 0 || strcmp(type, types._float) == 0) {
					size = sizeof(int);
				}
				else if (strcmp(type, types._long) == 0 || strcmp(type, types.ulong) == 0 || strcmp(type, types._double) == 0) {
					size = sizeof(long long);
				}
				if (strcmp(type, types.boolean) == 0) {
					size = 1;
				}
				iResult = send(socket, (char*)& size, 4, 0);
				if (iResult > 0) {
					iResult = send(socket, data, size, 0);
				}
			}
						
			
		}
		else if (strcmp(packet, "gty\0") == 0) {// get type || gty\0 \x00\x00\x00\x00  name_of_column\0   
												//			   cmd    number of db		name_of_column	 
			
			data = dbs[*(int*)(&packet[4])].get_type(&packet[8]);
			iResult = send(socket, data, strlen(data)+1, 0);
		}
		else if (strcmp(packet, "gbn\0") == 0) {// get db name || gbn\0 \x00\x00\x00\x00
												//				  cmd    number of db
			
			data = dbs[*(int*)(&packet[4])].get_name();
			iResult = send(socket, data, strlen(data) + 1, 0);
		}
		
		else if (strcmp(packet, "dum\0") == 0) {//dump all dbs
			new_db.lock();
			for (unsigned int i = 0; i < dbs_count;i++) {
				dbs[i].dump(root_path);
			}
			new_db.unlock();
			
		}
		
		delete[] packet;
	}
	closesocket(socket);
	*alive = false;
	return;
	
	//delete[] packet;
}

void connection_handler() {
	SOCKET sock;
	thread* threads_pool = new thread[MAX_THREADS];
	bool* threads_pool_alive = new bool[MAX_THREADS];
	for (int i = 0; i < MAX_THREADS; i++) {
		threads_pool_alive[i] = false;
	}
	//unsigned int working_threads = 0;
	unsigned int available_thread = 0;
	while (true) {
		//if (working_threads > 0) {
			available_thread = 0;
			while (available_thread < MAX_THREADS) {
				if (!threads_pool_alive[available_thread]) {
					
					//working_threads--;
					break;
				}
				available_thread++;
			}
		//}
		//else {
			this_thread::sleep_for(chrono::milliseconds(10));
		//}

		if (sockets_count > 0&&available_thread!=MAX_THREADS) {
			memcpy(&sock, sockets_pool, sizeof(SOCKET));
			mtx_new_connection.lock();
			sockets_count--;
			memcpy(sockets_pool,&sockets_pool[1],sizeof(SOCKET)*sockets_count);
			mtx_new_connection.unlock();
			//thread thread(work_with_db, &sock,&threads_pool_alive[available_thread]);	
			if (threads_pool[available_thread].joinable()) {
				threads_pool[available_thread].join();
			}
			threads_pool[available_thread] = thread(work_with_db,&sock,&threads_pool_alive[available_thread]);
			//working_threads++;
		}
		
	}
	
}



int main(int argc, char* argv[])
{
	char* port;
	root_path = parse_main_path(argv[0]);
	port = (char*)& DEFAULT_PORT;
	char* filename;
	int offset = 1;
	if (argc >= 3 && strcmp(argv[1],"-p\0")==0) {
		port = argv[2];
		if (argc == 3) {
			offset = 0;
		}
		else {
			offset = 3;
		}
	}
	bool res;
	if (argc > 1 && offset != 0 ) {
		dbs = new DB[argc - offset];

		for (int i = 0; i < (argc - offset); i++) {
			if (!dbs[i].load(argv[offset + i])) {
				filename = new char[strlen(root_path) + strlen(argv[offset + i]) + 1];
				memcpy(filename, root_path, strlen(root_path));
				memcpy(&filename[strlen(root_path)], argv[offset + i], strlen(argv[offset + i]) + 1);
				res = dbs[i].load(filename);
				delete[] filename;
				if (!res) {
					cout << argv[offset + i] << " Not found!" << endl;					
					return 1;
				}
				else {
					
					cout << "DB " << argv[offset + i] << " Loaded" << endl;
				}
			}
			else {
				cout << "DB " << argv[offset + i] << " Loaded" << endl;
			}
			
		}
	}
	

	WSADATA wsaData;
	int iResult;

	// Initialize Winsock
	iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
	if (iResult != 0) {
		printf("WSAStartup failed: %d\n", iResult);
		return 1;
	}
	ZeroMemory(&hints, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	hints.ai_flags = AI_PASSIVE;
	iResult = getaddrinfo(NULL, port, &hints, &result);
	if (iResult != 0) {
		printf("getaddrinfo failed: %d\n", iResult);
		WSACleanup();
		return 1;
	}

	SOCKET ListenSocket = INVALID_SOCKET;
	// Create a SOCKET for the server to listen for client connections
	ListenSocket = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
	//thread(connection_handler,&ListenSocket);
	if (ListenSocket == INVALID_SOCKET) {
		printf("Error at socket(): %ld\n", WSAGetLastError());
		freeaddrinfo(result);
		WSACleanup();
		return 1;
	}

	// Setup the TCP listening socket
	iResult = bind(ListenSocket, result->ai_addr, (int)result->ai_addrlen);
	if (iResult == SOCKET_ERROR) {
		printf("bind failed with error: %d\n", WSAGetLastError());
		freeaddrinfo(result);
		closesocket(ListenSocket);
		WSACleanup();
		return 1;
	}
	freeaddrinfo(result);
	if (listen(ListenSocket, SOMAXCONN) == SOCKET_ERROR) {
		printf("Listen failed with error: %ld\n", WSAGetLastError());
		closesocket(ListenSocket);
		WSACleanup();
		return 1;
	}
	SOCKET ClientSocket;
	ClientSocket = INVALID_SOCKET;


	// Accept a client socket
	thread handler(connection_handler);
	while (true) {
		
		ClientSocket = accept(ListenSocket, NULL, NULL);
		cout << "New connection" << endl;
		if (ClientSocket == INVALID_SOCKET) {
			printf("accept failed: %d\n", WSAGetLastError());
			continue;
		}
		mtx_new_connection.lock();
		if (sockets_count == pool_size-1) {
			SOCKET* buf = new SOCKET[pool_size+2];
			memcpy(buf,sockets_pool,sockets_count*sizeof(SOCKET));
			delete[] sockets_pool;
			sockets_pool = buf;

		}
		memcpy(&sockets_pool[sockets_count],&ClientSocket, sizeof(SOCKET));
		sockets_count++;
		mtx_new_connection.unlock();

		
	}
}


