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
const char nothing[] = "Nothing Found\0";

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
mutex* mutexes;


unsigned int number_of_db(char* db_name) {
	unsigned int number = 0;
	
	while(number<dbs_count){
		if (strcmp(dbs[number].get_name(), db_name) == 0) {
			return number;
		}
		number++;
	}
	return dbs_count;
}


void work_with_db(SOCKET* sock,bool* alive) {
	*alive = true;
	SOCKET socket;
	memcpy(&socket,sock,sizeof(SOCKET));
	fd_set fds;
	
	struct timeval tv;
	FD_ZERO(&fds);
	FD_SET(socket, &fds);
	tv.tv_sec = 100000;
	tv.tv_usec = 0;

	unsigned int packet_size=1;
	int iResult = 1;
	char* packet;
	
	char* type=NULL;
	char* data= NULL;
	
	unsigned int number_db;
	
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
			
			if (number_of_db(&packet[4]) != dbs_count) {
				//new_db.unlock();
				goto LOOP_END;
			}

			DB* buf = new DB[dbs_count+1];	
			
			if (dbs_count > 0) {				
				memcpy(buf, dbs, dbs_count * sizeof(DB));				
				delete[] dbs;				
			}
			mutex* buf_mutex = new mutex[dbs_count + 1];
			//buf_mutex[dbs_count].unlock();
			if (dbs_count > 0) {
				memcpy(buf_mutex, mutexes, dbs_count * sizeof(mutex));
				delete[] mutexes;
			}
			
			dbs = buf;
			mutexes = buf_mutex;
			dbs[dbs_count].init(&packet[4], *(unsigned int*)(&packet[packet_size-9]), *(unsigned int*)(&packet[packet_size - 5]),*(bool*)(&packet[packet_size-1]));
			dbs_count++;
			new_db.unlock();
			
		}
		else if (strcmp(packet, "ico\0") == 0) {//init column || ico\0   name\x00        \x00\x00\x00\x00		name\0			type\0
												//				 cmd    name of db		column number	   column name    type of column
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {				
				goto LOOP_END;
			}
			unsigned int column_number_offset = strlen(&packet[4]) + 1 +4 ;
			unsigned int column_name_offset = column_number_offset + 4;
			unsigned int type_of_column_offset = column_name_offset + strlen(&packet[column_name_offset]) + 1;
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			dbs[number_db].init_column(*(unsigned int*)(&packet[column_number_offset]),&packet[column_name_offset],&packet[type_of_column_offset]);
			mutexes[number_db].unlock();
			//new_db.unlock();
		}
		else if(strcmp(packet, "aco\0") == 0){// append column || aco\0   name\x00           name\0			type\0  
											  //				  cmd    name of db	    column name    type of column	
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {
				goto LOOP_END;
			}
			unsigned int column_name_offset = strlen(&packet[4]) + 1 + 4;
			unsigned int type_of_column_offset = column_name_offset + strlen(&packet[column_name_offset]) + 1;
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			dbs[number_db].append_column(&packet[column_name_offset],&packet[type_of_column_offset]);
			mutexes[number_db].unlock();
			//new_db.unlock();
		}
		else if ((strcmp(packet, "sva\0") == 0)) {// set value || sva\0	    name\x00  name_of_column\0  \x00\x00\x00\x00       data
												//				cmd       name of db		name_of_column	  cell number	    data
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {
				goto LOOP_END;
			}

			unsigned int column_name_offset = strlen(&packet[4]) + 1 + 4;
			unsigned int cell_number_offset = column_name_offset + strlen(&packet[column_name_offset]) + 1;


			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			if (strcmp(packet, "sva\0") == 0) {
				dbs[number_db].set_value(&packet[column_name_offset], *(unsigned int*)(&packet[cell_number_offset]), &packet[cell_number_offset + 4]);
			}
			mutexes[number_db].unlock();
			//new_db.unlock();
		}
		else if ((strcmp(packet, "ins\0") == 0) || (strcmp(packet, "app\0") == 0)) { //ins\0 name_of_db row_number data
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {
				goto LOOP_END;
			}
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			if ((strcmp(packet, "ins\0") == 0)) {
				unsigned int row_number_offset = 4 + strlen(&packet[4]) + 1;
				unsigned int data_offset = row_number_offset + 4;
				
				dbs[number_db].insert_row(*(unsigned int*)(&packet[row_number_offset]), (unsigned char*)&packet[data_offset]);
				
			}
			else {
				unsigned int data_offset = 4 + strlen(&packet[4]) + 1;
				
				dbs[number_db].append((unsigned char*)&packet[data_offset]);
				
			}
			mutexes[number_db].unlock();
			//new_db.unlock();
		}
		else if (strcmp(packet, "gro\0") == 0) {// get whole row || gro\0 \x00\x00\x00\x00    \x00\x00\x00\x00
												//				    cmd        number of db	   row
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {
				goto LOOP_END;
			}
			unsigned int row_number = *(unsigned int*)(&packet[packet_size-4]);

			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			size_t size = 0;
			for (unsigned int i=0; i < dbs[number_db].get_count_of_columns(); i++) {
				size = size + dbs[number_db].get_size(i, row_number);
			}

			//char* data; 
			iResult = send(socket, (char*)& size, sizeof(size), 0);
			if (iResult > 0) {
				data = new char[size];//!!!!!!!!!!!
				size_t spec_counter = 0;
				for (unsigned int i=0; i < dbs[number_db].get_count_of_columns(); i++) {
					memcpy(&data[spec_counter], dbs[number_db].get_value(i, row_number), dbs[number_db].get_size(i, row_number));
					spec_counter += dbs[number_db].get_size(i, row_number);
				}
				iResult = send(socket, data, size, 0);
				delete[] data;
			}
			mutexes[number_db].unlock();


		}
		else if (strcmp(packet, "whe\0") == 0) {// where || whe\0   name_of_db\0    name_of_column\0     data
												//			cmd    name of db		name_of_column
			/*if (*(int*)(&packet[4]) >= dbs_count || *(int*)(&packet[4]) < 0) {
				break;
			}*/
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {
				goto LOOP_END;
			}
			unsigned int column_name_offset = strlen(&packet[4]) + 1 + 4;
			unsigned int data_offset = column_name_offset + strlen(&packet[column_name_offset]) + 1;

			size_t size = 4;
			//char* data;
			unsigned int row = 0;
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			bool ok = true;
			try{
				row = dbs[number_db].where(&packet[column_name_offset], (unsigned char*)&packet[data_offset]);
			}
			catch (int e) {
				size = 0;
				iResult = send(socket, (char*)& size, sizeof(size), 0);
				ok = false;
			}
			
			if (ok) {
				for (unsigned int i = 0; i < dbs[number_db].get_count_of_columns(); i++) {
					size = size + dbs[number_db].get_size(i, row);
				}

				//char* data;
				iResult = send(socket, (char*)&size, sizeof(size), 0);
				if (iResult > 0) {
					data = new char[size];//!!!!!!!!!!!
					memcpy(data, (unsigned int*)&row, sizeof(row));
					size_t spec_counter = 4;
					for (unsigned int i = 0; i < dbs[number_db].get_count_of_columns(); i++) {
						memcpy(&data[spec_counter], dbs[number_db].get_value(i, row), dbs[number_db].get_size(i, row));
						spec_counter += dbs[number_db].get_size(i, row);
					}
					iResult = send(socket, data, size, 0);
					delete[] data;
				}
			}
			mutexes[number_db].unlock();
			//new_db.unlock();

		}
		else if (strcmp(packet, "whn\0") == 0) {
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {
				goto LOOP_END;
			}
			unsigned int column_name_offset = strlen(&packet[4]) + 1 + 4;
			unsigned char mode = packet[column_name_offset+strlen(&packet[column_name_offset])+1];
			unsigned int data_offset = column_name_offset + strlen(&packet[column_name_offset]) + 2;
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			unsigned int* data = dbs[number_db].wheren(&packet[column_name_offset],(unsigned char*)&packet[data_offset],mode);
			//new_db.unlock();
			mutexes[number_db].unlock();

			size_t size = data[0] * sizeof(unsigned int);

			iResult = send(socket, (char*)&size, sizeof(size), 0);
			iResult = send(socket, (char*)data, size, 0);

			delete[] data;

		}
		else if (strcmp(packet, "gva\0") == 0) {// get value || gva\0 \x00\x00\x00\x00  name_of_column\0  \x00\x00\x00\x00  
												//				cmd    number of db		name_of_column	  cell number
			number_db = number_of_db(&packet[4]);
			if (number_db == dbs_count) {
				goto LOOP_END;
			}
			unsigned int column_name_offset = strlen(&packet[4]) + 1 + 4;
			unsigned int cell_number = *(unsigned int*)(&packet[packet_size-4]);

			unsigned int size=0;
			//char* data;
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			if (dbs[number_db].cell_is_empty(&packet[column_name_offset], cell_number)) {
				data = new char[1];
				data[0] = '\0';
				iResult = send(socket, data, 1, 0);
				delete[] data;
				
			}
			else {
				data = (char*)dbs[number_db].get_value(&packet[column_name_offset], cell_number);
				type = dbs[number_db].get_type(&packet[column_name_offset]);
				size = dbs[number_db].get_size(&packet[column_name_offset],cell_number);
				iResult = send(socket, (char*)& size, 4, 0);
				if (iResult > 0) {
					iResult = send(socket, data, size, 0);
				}
			}
			mutexes[number_db].unlock();
			//new_db.unlock();
			
		}
		else if (strcmp(packet, "gty\0") == 0) {// get type || gty\0 \x00\x00\x00\x00  name_of_column\0   
												//			   cmd    number of db		name_of_column	 
			number_db = number_of_db(&packet[4]);
			unsigned int column_name_offset = strlen(&packet[4]) + 1 + 4;
			data = dbs[number_db].get_type(&packet[column_name_offset]);
			iResult = send(socket, data, strlen(data)+1, 0);
		}
		else if (strcmp(packet, "gbn\0") == 0) {// get db name || gbn\0 \x00\x00\x00\x00
												//				  cmd    number of db
			
			data = dbs[*(int*)(&packet[4])].get_name();
			iResult = send(socket, data, strlen(data) + 1, 0);
		}

		else if (strcmp(packet, "exi\0") == 0) {// exist || exi\0 \x00\x00\x00\x00 name_of_column\0 value
			data = new char[1];//!!!!!!!!!!!
			number_db = number_of_db(&packet[4]);
			unsigned int column_name_offset = strlen(&packet[4]) + 1 + 4;
			unsigned int data_offset = column_name_offset + strlen(&packet[column_name_offset]) + 1;

			mutexes[number_db].lock();
			if (dbs[number_db].exist(&packet[column_name_offset], (unsigned char*)& packet[data_offset])) {
				data[0] = (char)1;
			}
			else {
				data[0] = '\0';
			}
			mutexes[number_db].unlock();
			iResult = send(socket, data, 1, 0);
			delete[] data;
		}
		else if (strcmp(packet, "apr\0") == 0) {//append row || apr\0 \x00\x00\x00\x00
			number_db = number_of_db(&packet[4]);
			unsigned int count_rows = *(unsigned int*)&packet[packet_size - 4];
			mutexes[number_db].lock();
			dbs[number_db].append_rows(count_rows);
			mutexes[number_db].unlock();

		}
		else if (strcmp(packet, "dum\0") == 0) {//dump all dbs
			new_db.lock();
			for (unsigned int i = 0; i < dbs_count;i++) {
				dbs[i].dump(root_path);
			}
			new_db.unlock();			
		}
		else if (strcmp(packet, "pop\0") == 0) {//pop row || pop\0 name\0 \0\0\0\0
			number_db = number_of_db(&packet[4]);
			unsigned int row = *(unsigned int*)(&packet[packet_size - 4]);
			size_t size = 0;
			Cell* popped_row;
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			try {
				popped_row = dbs[number_db].pop(row);
			}
			catch(int e){
				//new_db.unlock();
				mutexes[number_db].unlock();
				iResult = send(socket, (char*)&size, 8, 0);
				goto LOOP_END;
			}
			for (unsigned int i = 0; i < dbs[number_db].get_count_of_columns(); i++) {
				size += popped_row[i].get_size(dbs[number_db].get_type(i));
			}
			//new_db.unlock();
			mutexes[number_db].unlock();
			iResult = send(socket, (char*)&size, 8, 0);
			if (size != 0) {

				data = new char[size];//!!!!!

				size_t spec_counter = 0;
				for (unsigned int i = 0; i < dbs[number_db].get_count_of_columns(); i++) {
					memcpy(&data[spec_counter], popped_row[i].get_value(), popped_row[i].get_size(dbs[number_db].get_type(i)));
					spec_counter += popped_row[i].get_size(dbs[number_db].get_type(i));
				}

				
				if (iResult > 0) {
					iResult = send(socket, data, size, 0);
				}

				delete[] data;
				for (unsigned int i = 0; i < dbs[number_db].get_count_of_columns(); i++) {
					popped_row[i].clear();
				}
			}
			delete[] popped_row;
			
		}
		else if (strcmp(packet, "gcr\0") == 0) {
			number_db = number_of_db(&packet[4]);
			
			size_t size = 4;
			
			unsigned int count;
			new_db.lock();
			new_db.unlock();

			mutexes[number_db].lock();
			count = dbs[number_db].get_count_of_rows();
			mutexes[number_db].unlock();
			
			iResult = send(socket, (char*)&size, 8, 0);
			iResult = send(socket, (char*)&count, size, 0);
		}
		
		LOOP_END:delete[] packet;
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
	
	unsigned int available_thread = 0;
	while (true) {
		
			available_thread = 0;
			while (available_thread < MAX_THREADS) {
				if (!threads_pool_alive[available_thread]) {
					
					
					break;
				}
				available_thread++;
			}
		
			this_thread::sleep_for(chrono::milliseconds(10));
		

		if (sockets_count > 0&&available_thread!=MAX_THREADS) {
			memcpy(&sock, sockets_pool, sizeof(SOCKET));
			mtx_new_connection.lock();
			sockets_count--;
			memcpy(sockets_pool,&sockets_pool[1],sizeof(SOCKET)*sockets_count);
			mtx_new_connection.unlock();
			
			if (threads_pool[available_thread].joinable()) {
				threads_pool[available_thread].join();
			}
			threads_pool[available_thread] = thread(work_with_db,&sock,&threads_pool_alive[available_thread]);
			
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
		dbs_count = argc - offset;
		mutexes = new mutex[dbs_count];
		for (int i = 0; i < (argc - offset); i++) {
			//mutexes[i].unlock();
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
	
	/*for (unsigned int i = 0; i <= dbs[0].get_count_of_rows(); i++) {
		if (dbs[0].cell_is_empty((unsigned int)0, i)) {
			std::cout << i;
			break;
		}
	}*/

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
		//cout << "New connection" << endl;
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


