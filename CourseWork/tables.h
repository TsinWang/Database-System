#ifndef TABLES_H
#define TABLES_H

#include "dt.h"

// Data Types, Records, and Schemas
typedef enum DataType {
  DT_INT = 0,
  DT_STRING = 1,
  DT_FLOAT = 2,
  DT_BOOL = 3
} DataType;

typedef struct Value {  // Values of a data type are represented using the Value struct.
  DataType dt;
  union v {
    int intV;
    char *stringV; // string is diff in the record, there is no 0 byte in the end.
                   // For example, for strings of length 4 should occupy 4 bytes in the data field of the record.
    float floatV;
    bool boolV;
  } v;
} Value;

typedef struct RID {
  int page;
  int slot;
} RID;

typedef struct Record
{
  RID id;  // rid consisting of a page number and slot number
  char *data; // concatenation(串联) of the binary representation of its attributes according to the schema
} Record;

// information of a table schema: its attributes, datatypes, 
typedef struct Schema
{
  int numAttr;     // A schema consists of a number of attributes
  char **attrNames;  // we record the name
  DataType *dataTypes; //  data type
  int *typeLength;  // we record the size of the strings
  int *keyAttrs; // Furthermore, a schema can have a key defined.
                 // The key is represented as an array of integers that are the positions of the attributes of the key
                 //  For example, consider a relation R(a,b,c) where a then keyAttrs would be [0].
  int keySize;
} Schema;

// TableData: Management Structure for a Record Manager to handle one relation
typedef struct RM_TableData
{
  char *name;
  Schema *schema;
  void *mgmtData;
} RM_TableData;

#define MAKE_STRING_VALUE(result, value)				\
  do {									\
    (result) = (Value *) malloc(sizeof(Value));				\
    (result)->dt = DT_STRING;						\
    (result)->v.stringV = (char *) malloc(strlen(value) + 1);		\
    strcpy((result)->v.stringV, value);					\
  } while(0)


#define MAKE_VALUE(result, datatype, value)				\
  do {									\
    (result) = (Value *) malloc(sizeof(Value));				\
    (result)->dt = datatype;						\
    switch(datatype)							\
      {									\
      case DT_INT:							\
	(result)->v.intV = value;					\
	break;								\
      case DT_FLOAT:							\
	(result)->v.floatV = value;					\
	break;								\
      case DT_BOOL:							\
	(result)->v.boolV = value;					\
	break;								\
      }									\
  } while(0)


// debug and read methods
extern Value *stringToValue (char *value);
extern char *serializeTableInfo(RM_TableData *rel);
extern char *serializeTableContent(RM_TableData *rel);
extern char *serializeSchema(Schema *schema);
extern char *serializeRecord(Record *record, Schema *schema);
extern char *serializeAttr(Record *record, Schema *schema, int attrNum);
extern char *serializeValue(Value *val);

#endif
