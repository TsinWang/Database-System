#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>

#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct ScanHandle
{
    int TotalTuples; // store scan table total tuples
    int curTuple;  // store current tuple position
    
    RID id;
    Expr * cond;
}ScanHandle;

typedef struct RM_TableINFO // used
{
    int numTuples;           // count the total tuples in the table
    int first_free_page;     // count the page is able to be insert record
    
    SM_FileHandle fh;
   
} RM_TableINFO;

RM_TableINFO *info;  //used

int first_free_page = 1;

/*
 *  START_SCAN before call the next method, we initial some value in this method. Locate to the first
 *  record slot to start scan.
 */
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
  
    char *temp;
    temp = (char *)calloc(sizeof(char), 4096);
    readBlock(0, &info->fh, temp);
    
    int totaltuples = 0;
    memcpy(&totaltuples, temp, sizeof(int));
    printf("\n----Total tuples is %d", totaltuples);
    ScanHandle *sh = (ScanHandle *) malloc(sizeof(ScanHandle));
    
    sh->id.page = 1; // set page as the first record page
    sh->id.slot = 0;  // set slot as the first record slot
    sh->TotalTuples = (((RM_TableINFO *)rel->mgmtData)->numTuples);
    sh->curTuple = 0;
    sh->cond = cond;
    
    scan->rel = rel;
    scan->mgmtData = sh;
    
    free(temp);
    
    return RC_OK;
}


/*
 *  NEXT we call this method to find next record if the rercod match the condtion, we travel all records
 *  in one table from the head to tail if needed.
 */
RC next (RM_ScanHandle *scan, Record *record)
{
    //RID rid;
    int recordSize = 0;
    int TupelsPerPage = 0;
    int maxPage = 0;
    int slot = 0;

    Value *val;
    ScanHandle *sh;
    Record *rec = (Record *) malloc(sizeof(Record));

    
    SM_PageHandle sm;
    sm = (SM_PageHandle)calloc(sizeof(char), 4096);
    
    sh = (ScanHandle *)scan->mgmtData;
    
    recordSize = getRecordSize(scan->rel->schema);
    
    rec->data = (char *)malloc(recordSize);
    
    slot = sh->id.slot;  // set slot = id.slot
    
    int page = sh->id.page;   // set page = id.page
 
    recordSize = getRecordSize(scan->rel->schema) + sizeof(int);
    TupelsPerPage = 256;
    
    maxPage = info->fh.totalNumPages - 1;
   
    if (slot >= 4096) {   // if slot > pagesize, go to the next page and set slot as first record position
        page++;
        slot = 0;
        sh->id.slot = 0;
    }
        while (1) {
            if ( sh->curTuple > sh->TotalTuples) { // judge if the next record is valid
                return RC_RM_NO_MORE_TUPLES;
            }
          
            getRecord(scan->rel, sh->id, rec);  // get the record
            evalExpr(rec, scan->rel->schema, sh->cond, &val);  // judge if the record match the condition
     
            if (val->v.boolV == TRUE) {  // if the record matched the condition
                readBlock(page, &info->fh, sm); // get it from the disk
                char* pos2;
                pos2 = sm;
                pos2 += sizeof(int)+slot;
                memcpy(record->data, pos2, recordSize);
                
                slot+= recordSize;  // slot move to the next record
                
                sh->id.page = page;
                sh->id.slot = slot;
                sh->curTuple++;
                scan->mgmtData = sh;
        
                break;
            }
            slot+= recordSize;  // if the record not match the condition,
                                // also let slot move to the next record
            if (slot >= 4096) {
                page++;
                slot =0;
            }
            sh->id.page = page;
            sh->id.slot = slot;
            sh->curTuple++;
            scan->mgmtData = sh;
        }
    
    free(sm);
    
    
    return RC_OK;
}


/*
 *  CLOSE_SCAN
 */
RC closeScan (RM_ScanHandle *scan)
{
    free(scan);
    scan->mgmtData = NULL;
    scan->rel = NULL;
   
    return RC_OK;
}


/*
 *  MALLOC_SCHEMA
 */
static Schema *mallocSchema(int numAttr, int keySize){
    Schema *sc;
    int i;
    
    // alloc memory to Schema
    sc = (Schema *)malloc(sizeof(Schema));
    
    sc->numAttr = numAttr;
    sc->attrNames = (char **)malloc(sizeof(char*) * numAttr);
    sc->dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
    sc->typeLength = (int *)malloc(sizeof(int) * numAttr);
    sc->keySize = keySize;
    sc->keyAttrs = (int *)malloc(sizeof(int) * keySize);
    
    for(i = 0; i < numAttr; i++)
    {
        sc->attrNames[i] = (char *) malloc(sizeof(char *));
    }
    
    return sc;
}



/*
 *  CREATE_SCHEMA create a schema when we got enough info
 */
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{

    Schema *schema;
    int i;

    // alloc memory to the Schema
    schema = mallocSchema(numAttr, keySize);
    
    for (i = 0; i< numAttr; i++) {
        strcpy(schema->attrNames[i], attrNames[i]);
    }

    schema->numAttr = numAttr;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    schema->keySize = keySize;

    return schema;
}




/*
 *  FREE_SCHEMA
 */
RC freeSchema (Schema *schema)
{
    free(schema->attrNames);
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);
    
    return RC_OK;
}




/*
 *  CREATE_RECORD create a reocrd and alloc this record memorys
 */
RC createRecord (Record **record, Schema *schema)
{
    int recordSize;
    recordSize = getRecordSize(schema);
   
    Record *r = (Record *)malloc(sizeof(Record));
    char *data =(char *)malloc(sizeof(char) * recordSize);
    r->data = data;
    
    *(record) = r;
    
    return RC_OK;
}

/*
 *  FREE_RECORD free the record
 */
RC freeRecord (Record *record)
{
    free(record);
    return RC_OK;
}

/*
 *  GET_ATTR get the attrbute and write them into valuve->v
 */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    int i;
    
    Value *val;
    val = (Value *)malloc(sizeof(Value));
    
    DataType *type;
    type = (DataType *)malloc(sizeof(DataType));
    type = schema->dataTypes;
    
    char *recordOffset = record->data;
    
    for (i=0; i<attrNum; i++)
    {
        
        if (type[i] == DT_STRING) {
            recordOffset+= schema->typeLength[i] * sizeof(char);
        }else if (type[i] == DT_INT){
            recordOffset+= sizeof(int);
        }else if (type[i] == DT_FLOAT){
            recordOffset+= sizeof(float);
        }else if(type[i] == DT_BOOL){
            recordOffset += sizeof(bool);
        }
    }
     val->dt = schema->dataTypes[attrNum];
    switch(schema->dataTypes[attrNum])
    {
        case DT_STRING:
            val->v.stringV= (char*) malloc (schema->typeLength[attrNum]*sizeof(char)+1);
            memcpy(val->v.stringV ,recordOffset, schema->typeLength[attrNum] * sizeof(char));
            val->v.stringV[schema->typeLength[i]* sizeof(char)]='\0'; // String end with '\0'
            break;
        case DT_INT:
            memcpy(&val->v.intV, (int*)recordOffset, sizeof(int));
            break;
        case DT_BOOL:
            memcpy(&val->v, recordOffset, sizeof(bool));
            break;
        case DT_FLOAT:
            memcpy(&val->v, recordOffset, sizeof(float));
            break;
        default:
            break;
    }
     *value = val;

    return RC_OK;
    
}



/*
 *  SET_ATTR  write attribute into record->data
 */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    int i;
    
    char *recordOffset= record->data;
    DataType *type;
    type = (DataType *)malloc(sizeof(DataType));
    value->dt = schema->dataTypes[attrNum];
    
    for (i=0; i<attrNum; i++)
    {
        type[i] = schema->dataTypes[i];

        if (type[i] == DT_STRING) {
            recordOffset+= (schema->typeLength[i])*sizeof(char);
        }else if (type[i] == DT_INT){
            recordOffset+= sizeof(int);
        }else if (type[i] == DT_FLOAT){
            recordOffset+= sizeof(float);
        }else if(type[i] == DT_BOOL){
            recordOffset += sizeof(bool);
        }
    }
  
    switch(value->dt)
    {
        case DT_STRING:
            memcpy(recordOffset, value->v.stringV, (schema->typeLength[i])*sizeof(char));
            break;
        case DT_INT:
            memcpy(recordOffset, &value->v.intV, sizeof(int));
            break;
        case DT_BOOL:
            memcpy(recordOffset, &value->v.boolV, sizeof(bool));
            break;
        case DT_FLOAT:
            memcpy(recordOffset, &value->v.floatV, sizeof(float));
            break;
        default:
            break;
    }
    
    free(type);
    return RC_OK;

}



/*
 *  INIT_RECORD_MANAGER initial manager
 */
RC initRecordManager (void *mgmtData)
{
    return RC_OK;
}




/*
 *  SHUTDOWN_RECORD_MANAGER  shutdown manager
 */
RC shutdownRecordManager ()
{
    return RC_OK;
}


/*
 *  CREATE_TABLE create the table use one file to store schema information, another one to store schema
 */
RC createTable (char *name, Schema *schema)
{

    int i = 0;
    int attrNum = schema->numAttr;
    int keyNum = schema->keySize;
    int numTuples = 0;  // ninital total tuples number = 0
    first_free_page = 1; // set first store record page = 1
    
    info =(RM_TableINFO *)malloc(sizeof(RM_TableINFO));
    
    char *data;
    data = (char *)malloc(sizeof(char)*4096);
    char *curPos = data;
    
    //保存 schema
    
    char *RecordOffset;
    RecordOffset = curPos;
    
    char *cp = curPos;
    int *ip = (int *)cp;
    ip[0] = numTuples;
    curPos+=sizeof(int);
    ip[1] = first_free_page;
    
    char *curName;  // create a new_name file，to store schema info
    FILE *pFile;

    char *new_name;
    int length;
    length = (int)strlen(name);
    new_name = (char *)malloc(length+sizeof(char)+1);
    memcpy(new_name, name, length);
    new_name[length] = '1';
    new_name[length+1] ='\0';
    
    pFile = fopen(new_name, "w+");// create a text for schema storage
    rewind(pFile);
    
    fwrite(&numTuples, sizeof(int), 1, pFile);
    fwrite(&first_free_page, sizeof(int), 1, pFile);
    fwrite(&(schema->numAttr), sizeof(int), 1, pFile);
    fwrite(&(schema->keySize), sizeof(int), 1, pFile);
   
    curName = (char *)malloc(sizeof(char));
    for (i =0 ; i < attrNum; i++) {
       memcpy(curName,schema->attrNames[i],sizeof(char));
       fwrite(curName, sizeof(char), 1, pFile);
    }
   
       fwrite(schema->dataTypes, sizeof(DataType), attrNum, pFile);
       fwrite(schema->typeLength, sizeof(int), attrNum, pFile);
       fwrite(schema->keyAttrs, sizeof(int), keyNum, pFile);

    fclose(pFile);
    
    SM_FileHandle fh;  // use storagemanagement to create one page to store soem info at the 0 page
    if (createPageFile(name)==RC_OK){
        if(openPageFile(name, &fh)== RC_OK){
           if(writeBlock(0, &fh, data) == RC_OK){
               if (closePageFile(&fh) == RC_OK){
                   free(data);
                   return RC_OK;
               }
            }
         }
    }
    
    
    free(new_name);
    free(curName);
    free(data);
    
    return RC_READ_NON_EXISTING_PAGE;
    
}




/*
 *  OPEN_TABLE open table from the file, get numtuples information and first free page info from the head of the file
 */
RC openTable (RM_TableData *rel, char *name)
{
    int i;
    
    char *new_name;
    int length;
    length = (int)strlen(name);
    new_name = (char *)malloc(length+sizeof(char)+1);
    memcpy(new_name, name, length);
    new_name[length] = '1';
    new_name[length+1] ='\0';
 
     int numTuples, first_free_page, numAttrs, keySize;

    FILE *pFile;
    pFile = fopen(new_name, "r+"); // open the new_name file to get the schema info
    char *curName;
    Schema *schema;
    int temp;
    
    
    fread(&temp, sizeof(int), 1, pFile); // get numtuples
    numTuples = temp;
    
    fread(&temp, sizeof(int), 1, pFile); // get first free page
    first_free_page = temp;
    
    fread(&temp, sizeof(int), 1, pFile);  // get numAttrs
    numAttrs = temp;
    
    fread(&temp, sizeof(int), 1, pFile);   // get keySize
    keySize = temp;
    
    schema = mallocSchema(numAttrs, keySize);
    
    curName = (char *)malloc(sizeof(char));
    for (i =0 ; i < numAttrs; i++) {
        fread(curName, sizeof(char), 1, pFile);   // get AttrName
        strcpy(schema->attrNames[i], curName);
    }
    
    fread(schema->dataTypes, sizeof(DataType), numAttrs, pFile);  // get datatypes

    fread(schema->typeLength, sizeof(int), numAttrs, pFile); // get typelength
 
    fread(schema->keyAttrs, sizeof(int), keySize, pFile); // get keyattrs
 
    fclose(pFile);
    
    rel->name = (char *)malloc(sizeof(char));
    strcpy(rel->name, name);  // set rel->name = name
    
    rel->schema = createSchema(numAttrs, schema->attrNames, schema->dataTypes, schema->typeLength, schema->keySize, schema->keyAttrs);  // set rel->schema
    
    rel->mgmtData = info;
    
    char *temp_1;
    temp_1 = (char *)calloc(sizeof(char), 4096);
    
    openPageFile(rel->name, &info->fh);
    readBlock(0, &info->fh, temp_1);
    
    char *RecordOffset;
    RecordOffset = temp_1;
    
    char *cp = RecordOffset;
    int *ip = (int *)cp;
    numTuples = ip[0];  // get the number of tuples info from the head of file

    info->numTuples = numTuples;

    RecordOffset+= sizeof(int);
    
    
    first_free_page = ip[1];  // get the first valid page info from the head of file
    info->first_free_page = first_free_page;
    
    
    appendEmptyBlock(&info->fh); // add 1 pages to store record

    free(temp_1);
    free(new_name);
  
    return RC_OK;
}




/*
 *  CLOSE_TABLE close the table and write the numtuples and first_free_page info back to file
 */
RC closeTable (RM_TableData *rel)
{
    
    int *first_free_page;
    int *numTuples;
    int rc;
    
    char *head;
    head = (char *)malloc(sizeof(char)*4096);
    
    openPageFile(rel->name, &info->fh);
    rc = readBlock(0, &info->fh, head);


    first_free_page = &info->first_free_page;
    numTuples = &info->numTuples;
    
    char *pos;
    pos = head;
    memcpy(pos, numTuples, sizeof(int));
    pos+=sizeof(int);
    memcpy(pos, first_free_page, sizeof(int));
    
    writeBlock(0, &info->fh, head); // write back numtuples and firsst free page info
    closePageFile(&info->fh);
    
    free(head);
    
    rel->name =NULL;   // free rel info
    rel->schema = NULL;
    rel->mgmtData = NULL;
    
    return RC_OK;
}




/*
 *  DELETE_TABLE destory the file which handle the table
 */
RC deleteTable (char *name)
{
    destroyPageFile(name);
    free(info);
    info = NULL;
    
    return RC_OK;
}




/*
 *  GET_NUM_TUPLES get the number of tuples in one table
 */
int getNumTuples (RM_TableData *rel)
{
    int curTableTuples;
    
    curTableTuples = (((RM_TableINFO *)rel->mgmtData)->numTuples);
    
    return curTableTuples;
}



/*
 *  GET_RECORD_SIZE count the size of a record
 */
int getRecordSize (Schema *schema)
{
    
    int size = 0;
    int i;
    
    DataType *dt;
    
    dt = schema->dataTypes;
    
    for (i = 0; i < schema -> numAttr; i++) {
        switch (dt[i]) {
            case DT_STRING:
                size += (schema->typeLength[i]) * sizeof(char);
                break;
            case DT_INT:
                size += sizeof(int);
                break;
            case DT_FLOAT:
                size += sizeof(float);
                break;
            case DT_BOOL:
                size += sizeof(bool);
                break;
            default:
                break;
        }
        
    }
    return size;
}


/*
 *  FIND_EMPTY_SLOT this is the method to find valid slot in one table
 */
int findEmptySlot(SM_PageHandle *sm, int PageCapacity, int WholeRecordSize){
    
    int i;
    int slotstate = 0;
    int slotcount = 0;
    
    char *offset;
 
    offset = *sm;
  
    for (i = 0; i < PageCapacity; i++) {  // if slot < max slot per page
        slotstate = *(int *)offset;   // move recordSize, then read the first int value to judge insertable
                                      // 0 means valid, 0 means unvalid
        if (slotstate == 0) {
            break;
        }
        offset+= WholeRecordSize;   // WholeRecordSize = size of statement + size of record
        slotcount += WholeRecordSize;
        
    }

    return slotcount;
}



/*
 *  INSERT_RECORD find the empty slot in one page then insert the record into those slot
 */
RC insertRecord (RM_TableData *rel, Record *record)
{
 
    int PageCapacity;
    int WholeRecordSize;
    int val_slot = 0;
    int slotmarked = 1;
    int numTuples = 0;
    int rc;
    
    openPageFile(rel->name, &info->fh);
    
    WholeRecordSize = sizeof(int) + getRecordSize(rel->schema);
    PageCapacity = 4096/WholeRecordSize;
    
    numTuples =info->numTuples;
    
    SM_PageHandle pool;
    pool = (char *)calloc(sizeof(char), 4096);
    
    char *head;
    head = (char *)calloc(sizeof(char), 4096);
    
    openPageFile(rel->name, &info->fh);
    rc = readBlock(first_free_page, &info->fh, pool); // read the page

    val_slot = findEmptySlot(&pool, PageCapacity, WholeRecordSize); // find the valid slot
    
    record->id.slot = val_slot;
    record->id.page = info->first_free_page;

    if (val_slot + WholeRecordSize >= 4096) { // if the slot is the more than the capability of one page
                                              // open the next page
        info->first_free_page++;
        first_free_page++;
        appendEmptyBlock(&info->fh);
        readBlock(first_free_page, &info->fh, pool);
        val_slot = 0;
        
        record->id.slot = val_slot;
        record->id.page = info->first_free_page;
    }
    
    info->fh.totalNumPages = info->first_free_page + 1;
    
    char *insert;     // insert operation, write the empty slot
    insert = pool;
    insert+= val_slot;
    
        memcpy(insert, &slotmarked , sizeof(int));
        insert+=sizeof(int);
    
        memcpy(insert, record->data , getRecordSize(rel->schema));
        insert+=getRecordSize(rel->schema);
    
    insert-= val_slot;
    insert-=sizeof(int);
    insert-=getRecordSize(rel->schema);
    
    rc = writeBlock(first_free_page, &info->fh, insert);

    numTuples++;
    info->numTuples = numTuples;
    first_free_page = info->first_free_page;
    
    closePageFile(&info->fh);

    (((RM_TableINFO *)rel->mgmtData)->numTuples) = numTuples; // record tuples change
    
    free(head);
    free(pool);
    
    return RC_OK;
}




/*
 *  DELETE_RECORD delete records from the table
 */
RC deleteRecord (RM_TableData *rel, RID id)
{
    
    int free_slot;
    int free_page;
    int recordSize;
    int slotmark = 0;
    
    char *temp;
    temp = (char *)calloc(sizeof(char), 4096);
    
    char *replace;
    replace = (char *)calloc(sizeof(char), getRecordSize(rel->schema));
    
    free_slot = id.slot;
    free_page = id.page;
    
    recordSize = getRecordSize(rel->schema);
    
    openPageFile(rel->name, &info->fh);
    readBlock(free_page, &info->fh, temp);
  
    char *delete;
    delete = temp;
    
    delete += free_slot;
    memcpy(delete, &slotmark, sizeof(int)); // let the slot statement = 0
    delete += sizeof(int);
    
    memcpy(delete, replace, recordSize);
    delete+=recordSize;
    
    writeBlock(free_page, &info->fh, temp); // write empty info into slot
    closePageFile(&info->fh);
    
    free(temp);
    free(replace);
    
    (((RM_TableINFO *)rel->mgmtData)->numTuples)--;
    
    return RC_OK;
}



/*
 *  UPDATE_RECORD cover the raw slot information
 */
RC updateRecord (RM_TableData *rel, Record *record)
{
    int update_slot;
    int update_page;
    int recordSize;
    int slotmark = 1;
    
    //printf("\n Update Record Beging ! \n");
    char *temp;
    temp = (char *)calloc(sizeof(char), 4096);
    
    update_slot = record->id.slot;
    update_page = record->id.page;
    
    recordSize = getRecordSize(rel->schema);
    
    openPageFile(rel->name, &info->fh);
    readBlock(update_page, &info->fh, temp);
    //printf("\n Update Record Beging ! \n");
    char *update;
    update = temp;
    
    update+=update_slot;
    
    memcpy(update, &slotmark, sizeof(int)); // set slot staement = 1
    update+=sizeof(int);
    
    memcpy(update, record->data, recordSize);
    update+=recordSize;
    
    writeBlock(update_page, &info->fh, temp); // cover the pre information
    
    closePageFile(&info->fh);
    free(temp);
    
    return RC_OK;
    
}




/*
 *  GET_RECORD get the record information
 */
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    int slot;
    int page;
    int IsrecordSize;
    char *temp_pool;
    temp_pool = (char *)calloc(sizeof(char) , 4096);

    IsrecordSize = getRecordSize(rel->schema);
    slot = id.slot;
    page = id.page;
    
    info = rel->mgmtData;
   
    openPageFile(rel->name, &info->fh);
    readBlock(page, &info->fh, temp_pool);  // read relative page
    
    char *take = temp_pool;
    take+= slot+sizeof(int);
    memcpy(record->data, take, IsrecordSize); // get the slot information
    
    record->id.page = id.page;
    record->id.slot = id.slot;
    
     free(temp_pool);
    
    return RC_OK;

}
