/**************************
Mike Rawding
CSC541 (Databases) Project
SUNY Polytechnic University

November 30, 2015

More info at
http://web.cs.sunyit.edu/~rawdinm/Databases/Project/DatabasesProject.html

Contact
MRawding@gmail.com

Not For Distribution 
**************************/

#include <SQLAPI.h> //see www.sqlapi.com
#include <samisc.h> //see www.sqlapi.com
#include <stdio.h> //user io
#include <stdlib.h> //misc
#include <iostream> //user io
#include <fstream> //read BLob input
#include <string> //std::string
#include <utility> //misc
#include <boost/algorithm/string/case_conv.hpp> //tolower
#include <boost/lexical_cast.hpp> //numerals to string
#include <chrono>  //benchmarking


/*--Definitions--*/

struct query;
enum command {INSERT, SELECT, UPDATE, DEL, TRANSFER, EXIT};

//inserts tuple into aux
void insert(SAConnection&, std::string);
//selects and desplays results from both aux and main 
void select(SAConnection&, std::string, std::string);
//runs delete or update query on both aux and main
void delUp(SAConnection&, std::string, std::string);
//transfers all tuples from aux to main. clear aux
void transfer(SAConnection&);

//gets input from user
std::string getSQLStatement();
//formats input to query struct
query parseSQLStatement(std::string);
//formats results for evenly spaced display
std::string fixedLength(std::string);
//desplays field from result
void displayField(SAField&);
//parses primary key from insert statement
std::string getPrimaryKey(const std::string&);
//checks main for primary key collision
bool primaryCollision(SAConnection&, const std::string&);
//reads blob file into SAString
void readBlob(std::string, SAString&);


struct query {
	command c;
	std::string q1 = "";
	std::string q2 = "";
};

/*--MAIN--*/
int main(int argc, char* argv[]) {
	//connect to database
	SAConnection con;
	try {
		con.Connect("", "user", "password", SA_MySQL_Client);
	}
	catch (SAException &x) {
		// show error message
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
	}

	//run queries
	while(1) {
		std::string userText = getSQLStatement();
		auto startTime = std::chrono::system_clock::now();
		query input = parseSQLStatement(userText);
		
		switch (input.c) {
		case INSERT: insert(con, input.q1); break;
		case SELECT: select(con, input.q1, input.q2); break;
		case UPDATE: delUp(con, input.q1, input.q2); break;
		case DEL: delUp(con, input.q1, input.q2); break;
		case TRANSFER: transfer(con); break;
		case EXIT: exit(1);//exit point
		}
		auto endTime = std::chrono::system_clock::now();
		std::chrono::duration<double> queryTime = endTime - startTime;
		std::cout << "Query took: " << queryTime.count() << "s" << std::endl;
	}

	con.Disconnect();
	return 0;
}//main

/*--USER INPUT--*/
std::string getSQLStatement() {
	std::string input;
	std::cout << "Enter a query: " << std::endl;
	std::getline(std::cin, input);
	return input;
}//getSQLStatement
query parseSQLStatement(std::string input) {
	query result;
	boost::to_lower(input);
	std::string command = input.substr(0, 6);

	if (command.compare("insert") == 0) {
		result.c = INSERT;
		result.q1 = input.replace(12, 12, "541project.transactions_aux");
		return result;
	}
	if (command.compare("select") == 0) {
		result.c = SELECT;
		result.q1 = result.q2 = input;
		std::size_t fromPos = input.find("from");
		result.q1.replace(fromPos + 5, 12, "541project.transactions_aux");
		result.q2.replace(fromPos + 5, 12, "541project.transactions_main");
		return result;
	}
	if (command.compare("update") == 0) {
		result.c = UPDATE;
		result.q1 = result.q2 = input;
		result.q1.replace(7, 12, "541project.transactions_aux");
		result.q2.replace(7, 12, "541project.transactions_main");
		return result;
	}
	if (command.compare("delete") == 0) {
		result.c = DEL;
		result.q1 = result.q2 = input;
		std::size_t fromPos = input.find("from");
		result.q1.replace(fromPos + 5, 12, "541project.transactions_aux");
		result.q2.replace(fromPos + 5, 12, "541project.transactions_main");
		return result;
	}
	if (command.compare("transf") == 0) {
		result.c = TRANSFER;
		return result;
	}
	if (command.compare("exit") == 0){
		result.c = EXIT;
		return result;
	}
	return result;
}//parseSQLStatement

/*--INSERT--*/
void insert(SAConnection& con, std::string query) {
	if (primaryCollision(con, query)) {
		std::cout << "Primary Key Collision" << std::endl;
		return;
	}
	SAString blobData;
	readBlob(query.substr(query.find("c:"),
		     query.find(";") - 1 - query.find("c:")),
		     blobData);
	query.replace(query.find("c:"), query.find(";") - 1 - query.find("c:"), ":1");
	try {
		SACommand cmd(&con, SAString(query.c_str()));
		cmd.Param("1").setAsBLob() = blobData;
		cmd.Execute();
	}
	catch (SAException &x) {
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(2)Tried: " << query << std::endl;
	}
}//insert
bool primaryCollision(SAConnection& con, const std::string& query) {
	std::string primaryKey = getPrimaryKey(query);
	std::string testQuery = "select * from 541project.transactions_main\
							where id = " + primaryKey + ";";
	try {
		SACommand cmd(&con, SAString(testQuery.c_str()));
		cmd.Execute();
		return(cmd.FetchNext());
	}
	catch (SAException &x) {
		// show error message
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(1)Tried: " << testQuery << std::endl;
	}
	return true;
}//primaryCollision
std::string getPrimaryKey(const std::string& query) {
	std::size_t valuePos = query.find("values");
	std::size_t paranPos = query.substr(valuePos).find("(");
	std::size_t pkPos = valuePos + paranPos + 1;
	std::string primaryKey = query.substr(pkPos++, 1);
	while (query.at(pkPos) > 47 && query.at(pkPos) < 58) {
		primaryKey += query.substr(pkPos++, 1);
	}
	return primaryKey;
}//getPrimaryKey
void readBlob(std::string path, SAString& blobData) {
	std::streampos size;
	char* memblock;
	std::ifstream file(path, std::ios::in | std::ios::binary | std::ios::ate);
	if (file.is_open()) {
		size = file.tellg();
		memblock = new char[size];
		file.seekg(0, std::ios::beg);
		file.read(memblock, size);
		file.close();
		blobData = SAString(memblock, size);
	}
	else {
		std::cout << "Could not read BLob" << std::endl;
	}
}//readBlob

/*--SELECT--*/
void select(SAConnection& con, std::string q1, std::string q2) {
	SACommand cmd1;
	SACommand cmd2;
	
	//query aux
	try {
		cmd1.setConnection(&con);
		cmd1.setCommandText(SAString(q1.c_str()));
		cmd1.setOption("HandleResult") = "store";
		cmd1.Execute();
	}
	catch (SAException &x) {
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(3)Tried: " << q1 << std::endl;
	}
	int columns = cmd1.FieldCount();
	//column headers
	for (int i = 1; i <= columns; i++) {
		std::cout << cmd1.Field(i).Name().GetMultiByteChars() << "    ";
	}
	std::cout << std::endl;
	//results from aux
	while (cmd1.FetchNext()) {
		for (int i = 1; i <= columns; i++) {
			displayField(cmd1[i]);
		}
		std::cout << std::endl;
	}

	//query main
	try {
		cmd2.setConnection(&con);
		cmd2.setCommandText(SAString(q2.c_str()));
		cmd2.setOption("HandleResult") = "store";
		cmd2.Execute();
	}
	catch (SAException &x) {
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(4)Tried: " << q2 << std::endl;
	}
	//results from main
	while (cmd2.FetchNext()) {
		for (int i = 1; i <= columns; i++) {
			displayField(cmd2[i]);
		}
		std::cout << std::endl;
	}
}//select
void displayField(SAField& field) {
	std::string display = "";
	if (field.FieldType() == SA_dtLong) {
		display = boost::lexical_cast<std::string>(field.asLong());
	}
	else if (field.FieldType() == SA_dtLongBinary) {
		display = "blob";
	}
	else if (field.FieldType() == SA_dtDateTime) {
		struct tm dateTime = (struct tm)field.asDateTime();
		display = boost::lexical_cast<std::string>(dateTime.tm_year + 1900);
		display += "-";
		display += boost::lexical_cast<std::string>(dateTime.tm_mon + 1);
		display += "-";
		display += boost::lexical_cast<std::string>(dateTime.tm_mday);
	}
	else if (field.FieldType() == SA_dtString) {
		display = field.asString();
	}
	else if (field.FieldType() == SA_dtDouble) {
		display = boost::lexical_cast<std::string>(field.asDouble());
	}
	else display = "error";
	std::cout << fixedLength(display);
}//displayField
std::string fixedLength(std::string in) {
	int length = 10;
	while (in.length() < 10) in += " ";
	if (in.length() > 10) in = in.substr(0, 10);
	return in;
}//fixedLength

/*--DELUP--*/
void delUp(SAConnection& con, std::string q1, std::string q2) {
	SACommand cmd1;
	SACommand cmd2;
	//query aux
	try {
		cmd1.setConnection(&con);
		cmd1.setCommandText(SAString(q1.c_str()));
		cmd1.setOption("HandleResult") = "store";
		cmd1.Execute();
	}
	catch (SAException &x) {
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(6)Tried: " << q1 << " on aux" << std::endl;
	}
	//query main
	try {
		cmd2.setConnection(&con);
		cmd2.setCommandText(SAString(q2.c_str()));
		cmd2.setOption("HandleResult") = "store";
		cmd2.Execute();
	}
	catch (SAException &x) {
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(6)Tried: " << q2 << "on main" << std::endl;
	}
}//delUp

/*--Transfer--*/
void transfer(SAConnection& con) {
	SACommand cmd1;
	SACommand cmd2;
	//transfer from aux to main
	try {
		cmd1.setConnection(&con);
		cmd1.setCommandText("insert into 541project.transactions_main\
							select * from 541project.transactions_aux;");
		cmd1.setOption("HandleResult") = "store";
		cmd1.Execute();
	}
	catch (SAException &x) {
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(7)Tried: insert into 541project.transactions_main\
				      select * from 541project.transactions_aux;" << std::endl;
	}
	//delete all in aux
	try {
		cmd2.setConnection(&con);
		cmd2.setCommandText("delete from 541project.transactions_aux;");
		cmd2.setOption("HandleResult") = "store";
		cmd2.Execute();
	}
	catch (SAException &x) {
		printf("ERROR:\n%s\n", x.ErrText().GetMultiByteChars());
		std::cout << "(8)Tried: delete from 541.transaction_aux;" << std::endl;
	}
}
