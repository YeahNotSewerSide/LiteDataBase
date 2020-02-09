#include <iostream>
#include <fstream>

class Cell {
	private:
		unsigned char* value;
		char* type;
		bool empty;
		
	public:
		
		void set_value(unsigned char* value) {
			if (!empty) {
				if (strcmp(this->type, "string\0") == 0) {
					delete[] value;
				}
				
			}
			if (strcmp(this->type, "string\0")==0) {
				this->value = new unsigned char[strlen((char*)value)+1];
				strcpy_s((char*)this->value, strlen((char*)value)+1, (char*)value);
			}
			
			if (strcmp(this->type, "integer\0")==0 || strcmp(this->type, "uinteger\0")==0 || strcmp(this->type, "float\0")==0) {
				this->value = new unsigned char[sizeof(int)];
				memcpy(this->value, value, sizeof(int));
			}
			if (strcmp(this->type, "long\0")==0 || strcmp(this->type, "ulong\0")==0|| strcmp(this->type, "double\0")==0) {
				this->value = new unsigned char[sizeof(long)];
				memcpy(this->value, value, sizeof(long));
			}
			if (strcmp(this->type, "boolean\0") == 0) {
				this->value = new unsigned char[1];
				memcpy(this->value, value, 1);
			}

			empty = false;
		}
		
		unsigned char* get_value() {
			return value;
		}

		bool is_empty() {
			return empty;
		}
		
		void set_type(char* type) {
			this->type = type;
		}

		Cell(char* type, void* value) {
			set_type(type);
			this->set_value((unsigned char*)value);
			this->empty = false;
			
		}
		Cell(char* type) {
			set_type(type);
			//this->set_value((void*) 1);
			this->empty = true;
		}
		Cell() {
			this->empty = true;
		}
		void clear() {
			if (!this->empty) {
				
				if (strcmp(this->type, "string\0") == 0) {
					delete[] value;
				}
				
			}
			
		}
		
};

class Column {	
	private:
		char* name;
		char* type;
		
		unsigned int n_rows;
		Cell* cells;
		bool inited;
	public:
		Column() {
			inited = false;
		}
		void init(char* name,char* type, unsigned int n_rows) {
			if (inited) {
				return;
			}
			this->name = new char[strlen(name)+1];
			this->type = new char[strlen(type)+1];
			strcpy_s(this->name, strlen(name)+1,name);
			strcpy_s(this->type, strlen(type)+1, type);
			this->n_rows = n_rows;
			
			this->cells = new Cell[this->n_rows];
			for (unsigned int i = 0; i < this->n_rows; i++) {
				this->cells[i].set_type(this->type);
			}
			inited = true;
		}

		void set_name(char* name) {
			delete[] name;
			this->name = new char[strlen(name) + 1];
			strcpy_s(this->name, strlen(name) + 1, name);
		}

		bool is_inited() {
			return inited;
		}

		bool cell_empty(unsigned int cell) {
			return cells[cell].is_empty();
		}
		void clear() {
			for (unsigned int i = 0; i < n_rows; i++) {
				cells[i].clear();
			}
			delete[] cells;
			n_rows = 0;
		}
		void uninit() {
			if (!inited) {
				return;
			}
			delete[] name;
			delete[] type;
			this->clear();
			inited = false;
		}

		bool set_value(unsigned int cell, void* value) {
			if (cell >= n_rows || cell < 0) {
				return false;
			}
			cells[cell].set_value((unsigned char*)value);
			return false;
		}

		unsigned char* get_value(unsigned int cell) {
			if (cell >= n_rows || cell < 0) {
				return false;
			}
			return cells[cell].get_value();

		}

		void append_cell(unsigned int count) {
			
			Cell* buf = new Cell[this->n_rows+count];
			unsigned int counter=n_rows*sizeof(Cell);
			
			if (counter != 0) {
				memcpy(buf, cells, counter);
			}
			n_rows = n_rows + count;
			for (unsigned int i = n_rows - count; i < n_rows;i++) {
				buf[i].set_type(type);
			}
			delete[] cells;
			cells = buf;
		}

		char* get_name() {
			return name;
		}
		char* get_type() {
			return type;
		}
	

};



class DB {
	private:
		unsigned int count_of_rows;
		unsigned int count_of_columns;
		char* name;
		Column* columns;
		bool inited;
	public:
		DB(char* name, unsigned int count_of_columns, unsigned int count_of_rows) {
			this->name = new char[strlen(name) + 1];
			strcpy_s(this->name, strlen(name) + 1, name);
			this->count_of_columns = count_of_columns;
			this->count_of_rows = count_of_rows;
			this->columns = new Column[this->count_of_columns];
			inited = true;
		}

		DB() {
			inited = false;
		}

		bool init_column(unsigned int column, char* name,char* type) {
			if (column <0 || column >= count_of_columns) {
				return false;
			}
			this->columns[column].init(name,type,count_of_rows);
			return true;
		}
		unsigned char* get_value(unsigned int column, unsigned int row) {
			if (column<0 || column >= count_of_columns) {
				return false;
			}
			return this->columns[column].get_value(row);
		}
		bool set_value(unsigned int column, unsigned int row, void* value) {
			if (column<0 || column >= count_of_columns) {
				return false;
			}
			return this->columns[column].set_value(row, value);
		}

		bool set_value(char* column_name, unsigned int row, void* value) {
			for (unsigned int i=0; i < count_of_columns; i++) {
				if (strcmp(column_name, columns[i].get_name()) == 0) {
					return this->columns[i].set_value(row, value);
				}
			}
			return false;
		}

		void* get_value(char* column_name, unsigned int row) {
			for (unsigned int i=0; i < count_of_columns; i++) {
				if (strcmp(column_name, columns[i].get_name()) == 0) {
					return this->columns[i].get_value(row);
				}
			}
			return false;
		}

		void append_column(char* column_name, char* type) {
			Column* buf = new Column[this->count_of_columns + 1];
			unsigned int counter = sizeof(Column)*count_of_columns;			
			memcpy(buf, columns, counter);
			delete[] columns;
			buf[count_of_columns].init(column_name, type, count_of_rows);
			columns = buf;
			count_of_columns += 1;
		}
		void append_rows(unsigned int count) {
			for (unsigned int i = 0; i < count_of_columns; i++) {
				columns[i].append_cell(count);
			}
		}

		void clear_db() {
			//deletes all entries from all columns
			count_of_rows = 0;
			for (unsigned int i = 0; i < count_of_columns; i++) {
				columns[i].clear();
			}
		}

		void uninit_column(unsigned int column, char* name, char* type) {
			if (column < 0 || column >= count_of_columns) {
				return;
			}
			columns[column].uninit();
			columns[column].init(name,type,count_of_rows);

		}

		void delete_column(unsigned int column) {
			columns[column].uninit();
			if (column < 0 || column >= count_of_columns) {
				return;
			}
											 
			Column* buf = new Column[this->count_of_columns - 1];
			unsigned int counter = column* sizeof(Column);
			
			if (counter > 0) {
				memcpy(buf, columns, counter);
			}
			counter = sizeof(Column)*(count_of_columns-column-1);
			
			if (counter > 0) {
				memcpy(&buf[column], &columns[column+1], counter);
			}
			delete[] columns;
			columns = buf;
			count_of_columns -= 1;
		}


		bool dump(char* path) {
			std::ofstream file;
			char* filename = new char[strlen(name)+6+strlen(path)];
			memcpy(filename, path, strlen(path));
			memcpy(&filename[strlen(path)],name, strlen(name));
			filename[strlen(path)+strlen(name)] = '.';
			filename[strlen(path) + strlen(name) + 1] = 'd';
			filename[strlen(path) + strlen(name) + 2] = 'a';
			filename[strlen(path) + strlen(name) + 3] = 't';
			filename[strlen(path) + strlen(name) + 4] = 'a';
			filename[strlen(path) + strlen(name) + 5] = '\0';
			file.open(filename, std::ios::out | std::ios::binary);
			if (!file.is_open()) {
				return false;
			}
			file << (unsigned char*)name<<'\0';
			for (int i = 0; i < sizeof(int); i++) {
				file << ((unsigned char*)& count_of_columns)[i];
			}
			for (int i = 0; i < sizeof(int); i++) {
				file << ((unsigned char*)& count_of_rows)[i];
			}

			for (unsigned int i = 0; i < count_of_columns; i++) {
				file << (unsigned char)columns[i].is_inited();
				if (!columns[i].is_inited()) {
					continue;
				}
				file << (unsigned char*)columns[i].get_name()<<'\0';
				file << (unsigned char*)columns[i].get_type()<<'\0';
				for (unsigned int n = 0; n < count_of_rows; n++) {
					file << (unsigned char)columns[i].cell_empty(n);
					if (columns[i].cell_empty(n)) {
						continue;
					}
					
					if (strcmp(columns[i].get_type(), "string\0")==0) {
						file << columns[i].get_value(n)<<'\0';
						
					}
					if (strcmp(columns[i].get_type(), "integer\0") == 0 || strcmp(columns[i].get_type(), "uinteger\0") == 0 || strcmp(columns[i].get_type(), "float\0") == 0) {
						for (int a = 0; a < sizeof(int); a++) {
							file << this->get_value(i,n)[a];
						}
					}
					if (strcmp(columns[i].get_type(), "long\0") == 0 || strcmp(columns[i].get_type(), "ulong\0") == 0 || strcmp(columns[i].get_type(), "double\0") == 0) {
						for (int a = 0; a < sizeof(long); a++) {
							file << this->get_value(i, n)[a];
						}
					}
					if (strcmp(columns[i].get_type(), "boolean\0") == 0) {
						file << *this->get_value(i, n);
					}
				}
				
			}
			file.close();
			delete[] filename;
			return true;
		}

		bool is_inited() {
			return inited;
		}

		bool load(char* path) {
			if (inited) {
				return false;
			}
			std::ifstream file;
			file.open(path, std::ios::binary);
			if (!file.is_open()) {
				return false;
			}

			char* ch = new char[1];//!!!!!!!!!!!!!!!!!!!!!!!
			unsigned int len=0;
			ch[0] = 'a';
			while (ch[0]!='\0') {
				file.read(ch,1);
				len++;				
			}
			this->name = new char[len];//!!!!!!!!!!!!!!!!!!!!!!!
			file.seekg(0,std::ios_base::beg);
			file.read(name,len);
						
			file.read((char*)&count_of_columns, 4);	
			this->columns = new Column[this->count_of_columns];//!!!!!!!!!!!!!!!!!!!!!!!
			file.read((char*)& count_of_rows, 4);
			bool column_inited;
			unsigned char* column_name;
			unsigned char* column_type;

			for (unsigned int i = 0; i < count_of_columns; i++) {
				file.read((char*)&column_inited,1);
				if (!column_inited) {
					continue;
				}
				len = 0;
				ch[0] = 'a';
				while (ch[0] != '\0') {
					file.read(ch, 1);
					len++;
				}
				column_name = new unsigned char[len];//!!!!!!!!!!!!!!!!!!!!!!!
				file.seekg((unsigned long)file.tellg()-len, std::ios_base::beg);
				file.read((char*)column_name, len);

				len = 0;
				ch[0] = 'a';
				while (ch[0] != '\0') {
					file.read(ch, 1);
					len++;
				}
				column_type = new unsigned char[len];//!!!!!!!!!!!!!!!!!!!!!!!
				file.seekg((unsigned long)file.tellg() - len, std::ios_base::beg);
				file.read((char*)column_type, len);

				columns[i].init((char*)column_name, (char*)column_type,count_of_rows);

				delete[] column_name;
				delete[] column_type;
				for (unsigned int n = 0; n < count_of_rows; n++) {
					file.read((char*)& column_inited, 1);
					if (column_inited) {
						continue;
					}
					if (strcmp(columns[i].get_type(), "string\0") == 0) {
						len = 0;
						ch[0] = 'a';
						while (ch[0] != '\0') {
							file.read(ch, 1);
							len++;
						}
						column_name = new unsigned char[len];//!!!!!!!!!!!!!!!!!!!!!!!
						file.seekg((unsigned long)file.tellg() - len, std::ios_base::beg);
						file.read((char*)column_name, len);
						
					}
					if (strcmp(columns[i].get_type(), "integer\0") == 0 || strcmp(columns[i].get_type(), "uinteger\0") == 0 || strcmp(columns[i].get_type(), "float\0") == 0) {
						column_name = new unsigned char[sizeof(int)];//!!!!!!!!!!!!!!!!!!!!!!!
						file.read((char*)column_name, sizeof(int));
					}
					if (strcmp(columns[i].get_type(), "long\0") == 0 || strcmp(columns[i].get_type(), "ulong\0") == 0 || strcmp(columns[i].get_type(), "double\0") == 0) {
						column_name = new unsigned char[sizeof(long)];//!!!!!!!!!!!!!!!!!!!!!!!
						file.read((char*)column_name, sizeof(long));
					}
					if (strcmp(columns[i].get_type(), "boolean\0") == 0) {
						column_name = new unsigned char[sizeof(bool)];//!!!!!!!!!!!!!!!!!!!!!!!
						file.read((char*)column_name, sizeof(bool));
					}
					columns[i].set_value(n, column_name);
					delete[] column_name;

				}
				
			
			}
			delete[] ch;
			inited = true;
		}

		~DB() {
			for (unsigned int i = 0; i < count_of_columns; i++) {				
				columns[i].uninit();
			}
			delete[] columns;
		}

};


int main()
{
	DB db;
	db.load((char*)"C:\\Users\\User\\Desktop\\GitHub\\TwoDimensionDynamicArray\\x64\\Debug\\Name.data\0");
	/*char* name = (char*)"Name\0";
	char* type = (char*)"integer\0";
	int input = 666;
	unsigned char* num = new unsigned char[sizeof(int)];
	num[0] = input & 0xFF;
	num[1] = (input >> 8) & 0xFF;
	num[2] = (input >> 16) & 0xFF;
	num[3] = (input >> 24) & 0xFF;
	DB db(name,4,10);
	db.init_column(0, (char*)"First\0", type);
	db.init_column(1, (char*)"Second\0", (char*)"string\0");
	db.init_column(2, (char*)"Third\0", (char*)"string\0");
	db.init_column(3, (char*)"Fourth\0", (char*)"integer\0");
	db.set_value((char*)"First\0", 1, num);	
	db.set_value((char*)"Second\0", 1, (void*)"Hello\0");
	db.set_value((char*)"Third\0", 1, (void*)"I'm third!\0");
	int a = *(int*)db.get_value((char*)"First\0", 1);
	char* string = (char*)db.get_value((char*)"Second\0", 1);
	db.append_column((char*)"Fifth\0", type);
	bool b = db.dump((char*)"C:\\Users\\User\\Desktop\\GitHub\\TwoDimensionDynamicArray\\x64\\Debug\\\0");
	db.clear_db();*/
	int a = *(int*)db.get_value((char*)"First\0", 1);
	std::cout << std::endl;
}


