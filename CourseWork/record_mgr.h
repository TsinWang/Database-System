#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  void *mgmtData;
} RM_ScanHandle;

// table and manager
extern RC initRecordManager (void *mgmtData);  // T1
extern RC shutdownRecordManager ();             // T1
extern RC createTable (char *name, Schema *schema);  // T1
extern RC openTable (RM_TableData *rel, char *name); // T1
extern RC closeTable (RM_TableData *rel); // T1
extern RC deleteTable (char *name); // T1
extern int getNumTuples (RM_TableData *rel);  // T1

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);  // T1
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);  // T1
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan); // T1

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

#endif // RECORD_MGR_H
