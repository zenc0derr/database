#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>

//struct to get the commands
typedef struct
{
    char *buffer;
    size_t buffer_length; // The size_t type is used to represent the size of objects in memory https://jameshfisher.com/2016/11/29/size_t_iterator/
    ssize_t input_length; // ssize_t is able to represent the number -1.which is returned by several system calls and library functions as a way to indicate error https://jameshfisher.com/2017/02/22/ssize_t/
} InputBuffer;

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

//For Non-SQL Commands
typedef enum
{
    META_COMMAND_SUCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

//For SQL Statements
typedef enum
{
    PREPARE_SUCESS,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PreparedResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
} Row;

typedef struct
{
    StatementType type;
    Row row_to_insert;
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute) //https://stackoverflow.com/questions/65236045/what-does-struct0-mean

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);

//serialized
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

//table
const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES  100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
} Table;

void print_row(Row* row){
    printf("(%d %s %s)\n",row->id, row->username, row->email);
}

Table* new_table(){
    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }

    return table;
}

void free_table(Table* table){
    for(int i=0;table->pages[i];i++){
        free(table->pages[i]);
    }

    free(table);
}

void* row_slot(Table* table, uint32_t row_num){
    uint32_t page_num = row_num/ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if(page==NULL){
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset* ROW_SIZE;
    return page+byte_offset;
}

//read input from terminal
void read_input(InputBuffer* input_buffer){
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    if (bytes_read <= 0)
    {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
    //printf("%c", input_buffer->buffer[bytes_read - 1]);
}

//free space of the buffer
void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table* table)
{
    if(strcmp(input_buffer->buffer,".exit")==0){
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    }else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PreparedResult prepared_insert(InputBuffer *input_buffer, Statement *statement){
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(input_buffer->buffer," ");
    char* id_string = strtok(NULL," ");
    char* username = strtok(NULL," ");
    char* email = strtok(NULL," ");

    if(id_string==NULL||username==NULL||email==NULL){
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if(id<0){
        return PREPARE_NEGATIVE_ID;
    }
    if(strlen(username)>COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    if(strlen(email)>COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username,username);
    strcpy(statement->row_to_insert.email,email);

    return PREPARE_SUCESS;
    


}

PreparedResult prepared_result(InputBuffer *input_buffer, Statement *statement){
    if(strncmp(input_buffer->buffer, "insert", 6)==0){
        return prepared_insert(input_buffer,statement);
    }

    else if (strncmp(input_buffer->buffer, "select", 6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

//function to delcare a new InputBuffer type
InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void print_promt(){
    printf("db> ");
}

void serialize_row(Row* source, void* destination){
    memcpy(destination + ID_OFFSET, &(source->id),ID_SIZE); //https://www.geeksforgeeks.org/memcpy-in-cc/
    memcpy(destination + USERNAME_OFFSET, &(source->username),USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email),EMAIL_SIZE);
}

void deserialize_row(void* Source, Row* destination){
    memcpy(&(destination->id),Source+ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), Source+USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email),Source+EMAIL_OFFSET, EMAIL_SIZE);
}

ExecuteResult execute_insert(Statement* statement,Table* table){
    if(table->num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);

    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    Row row;
    for(uint32_t i=0;i<table->num_rows;i++){
        deserialize_row(row_slot(table, i),&row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch(statement->type){
        case(STATEMENT_INSERT):
            return execute_insert(statement, table);
        case(STATEMENT_SELECT):
            return execute_select(statement,table);
    }
}


int main(int argc, char const *argv[])
{   
    Table* table = new_table();
    InputBuffer* input_buffer = new_input_buffer(); //creating a new input buffer
    while(true){
        print_promt();
        read_input(input_buffer);

        if(input_buffer->buffer[0]=='.'){
            switch(do_meta_command(input_buffer,table)){
                case(META_COMMAND_SUCESS):
                    continue;
                case(META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized Command\n");
                    continue;
            }
        }

        Statement statement;

        switch (prepared_result(input_buffer,&statement))
        {
            case(PREPARE_SUCESS):
                break;
            case(PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case(PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n",input_buffer->buffer);
                continue;
            case(PREPARE_STRING_TOO_LONG):
                printf("String is too long..\n");
                continue;
            case(PREPARE_NEGATIVE_ID):
                printf("ID must be positive");
                continue;
        }

        switch (execute_statement(&statement, table))
        {
            case(EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case(EXECUTE_TABLE_FULL):
                printf("Table Full Exeception");
                break;
        }
    }
    return 0;
    
}