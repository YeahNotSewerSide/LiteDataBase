# DataBase
DataBase.h - header file with core of DB.

DataBase.cpp - server for DB, uses new threads for every new user
# ToDo

Add more functionality(pop,insert)

# Args
  -p <port> - specify port for db server(always must be first argument)
  
  If you want to load dbs from dumps just write:
  
  DataBase.exe "path_to_dump\DataBase.data" or
  
  DataBase.exe DataBase.data if DataBase.data in the same folder with DataBase.exe
  
  Ex:
  
  DataBase.exe -p 6969 "path_to_dump\DataBase.data" MyNewSuperDB.data
  
  DataBase.exe MyNewSuperDB.data
  
Now it's only for Windows.
