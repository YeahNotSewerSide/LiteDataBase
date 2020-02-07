
#include <iostream>

class Cell {
	private:
		unsigned char* value;
		char* type;
		bool empty;
		
	public:
		
		void set_value(unsigned char* value) {
			if (!empty) {
				delete[] value;
			}
			if (strcmp(this->type, "string\0")==0) {
				this->value = new unsigned char[strlen((char*)value)+1];
				strcpy_s((char*)this->value, strlen((char*)value)+1, (char*)value);
			}
			
			if (strcmp(this->type, "integer\0")==0 || strcmp(this->type, "uinteger\0")==0 || strcmp(this->type, "float\0")==0) {
				this->value = new unsigned char[sizeof(int)];
				memcpy(&this->value,&value, sizeof(int));
			}
			if (strcmp(this->type, "long\0")==0 || strcmp(this->type, "ulong\0")==0|| strcmp(this->type, "double\0")==0) {
				this->value = new unsigned char[8];
				memcpy(&this->value, &value, 8);
			}
			empty = false;
		}
		
		void* get_value() {
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
		
};

class Column {	
	private:
		char* name;
		char* type;
		
		unsigned int n_rows;
		Cell* cells;
	public:
		Column() {

		}
		void init(char* name,char* type, unsigned int n_rows) {
			this->name = new char[strlen(name)+1];
			this->type = new char[strlen(type)+1];
			strcpy_s(this->name, strlen(name)+1,name);
			strcpy_s(this->type, strlen(type)+1, type);
			this->n_rows = n_rows;
			
			this->cells = new Cell[this->n_rows];
			for (int i = 0; i < this->n_rows; i++) {
				this->cells[i].set_type(type);
			}
		}

		bool set_value(unsigned int cell, void* value) {
			if (cell >= n_rows || cell < 0) {
				return false;
			}
			cells[cell].set_value((unsigned char*)value);
			return false;
		}

		void* get_value(unsigned int cell) {
			if (cell >= n_rows || cell < 0) {
				return false;
			}
			return cells[cell].get_value();

		}

		void append_cell(unsigned int count) {
			//n_rows = n_rows + count;
			Cell* buf = new Cell[this->n_rows+count];
			unsigned int counter=0;
			for (unsigned int i=0; i < n_rows; i++) {
				counter += sizeof(cells[i]);
			}
			memcpy(buf, cells, counter);
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
	public:
		DB(char* name, unsigned int count_of_columns, unsigned int count_of_rows) {
			this->name = new char[strlen(name) + 1];
			strcpy_s(this->name, strlen(name) + 1, name);
			this->count_of_columns = count_of_columns;
			this->count_of_rows = count_of_rows;
			this->columns = new Column[this->count_of_columns];
			
		}
		bool init_column(unsigned int count, char* name,char* type) {
			if (count<0 || count >= count_of_columns) {
				return false;
			}
			this->columns[count].init(name,type,count_of_rows);
			return true;
		}
		void* get_value(unsigned int column, unsigned int row) {
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
			unsigned int counter = 0;
			for (unsigned int i = 0; i < count_of_columns; i++) {
				counter += sizeof(columns[i]);
			}
			memcpy(buf, columns, counter);
			
			delete[] columns;
			columns = buf;
			this->init_column(count_of_columns,column_name,type);
			count_of_columns += 1;

		}
		void append_rows(unsigned int count) {
			for (unsigned int i = 0; i < count_of_columns; i++) {
				columns[i].append_cell(count);
			}
		}


};





int main()
{
	
	char* name = (char*)"Name\0";
	char* type = (char*)"integer\0";
	void* num = (void*)666;
	DB db(name,4,10);
	db.init_column(0, (char*)"First\0", type);
	db.init_column(1, (char*)"Second\0", (char*)"string\0");
	db.init_column(2, (char*)"Third\0", (char*)"long\0");
	db.init_column(3, (char*)"Fourth\0", (char*)"uint\0");
	db.set_value((char*)"First\0", 1, num);
	db.set_value((char*)"Second\0", 1, (void*)"Hello\0");
	int a = (int)db.get_value((char*)"First\0", 1);
	char* string = (char*)db.get_value((char*)"Second\0", 1);

	std::cout << std::endl;
}


