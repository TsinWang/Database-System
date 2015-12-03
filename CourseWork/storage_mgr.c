

#include "dberror.h"
#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>

#define PAGE_SIZE 4096


/*************** manipulating page files *****************/

/*
 * Initiate the storage manager.
 */


void initStorageManager (void)
{
    printf("Storage Manager Init Success !");
}

/*
 * Create a new page file.
 * New page file should be empty.
 */

 RC createPageFile (char *fileName)
{
    int pf;
    FILE *fp = NULL;
    fp = fopen(fileName, "w+");//Open the file which can be read & written

    
    if( fp == NULL) {
        printf("***File Init Failed!***");
        //free(memo);
        return RC_FILE_HANDLE_NOT_INIT;   // Check if file is exist
    } // Check if file is written correctly
    if (fp) {
        size_t sz = sizeof(char)*4096;
        char* buf = (char*)malloc(sz);
        if (buf == NULL)
        memset(buf, 0, sz);
        pf = fwrite (buf , sizeof(char), 4096, fp);
        if (pf != 4096)
        return RC_WRITE_FAILED;
        free(buf);
        fclose(fp);
        return RC_OK;
    }else{
        return RC_READ_NON_EXISTING_PAGE;
    }
        
}

/*
 * Open a page file.
 * If we can open it, caculate its total page number, update fHandle info.
 * If we can/t open it, return RC_FILE_NOT_FOUND.
 */

 RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{

    long sz = 0;
    int numberOfPages = 0;
    FILE *fp = NULL;
    fp=fopen(fileName, "rb+");
    if (fp) {
        //get the number of page
        fseek(fp, 0L, SEEK_END);// seek to end of file
        sz = ftell(fp);// get current file pointer
        fseek(fp, 0L, SEEK_SET);// seek back to beginning of file
        numberOfPages = sz/4096;
        
        //initialize the SM_FileHandle
        fHandle->fileName = fileName;
        fHandle->totalNumPages = numberOfPages;
        fHandle->curPagePos = 0;
        fHandle->mgmtInfo = fp;
    }else{
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

 RC closePageFile (SM_FileHandle *fHandle)
{
    
    fclose(fHandle->mgmtInfo); // close file
    //fHandle->fileName = NULL;
    //fHandle->curPagePos = -9999;
    
//    printf("***Close File Success!***");
    return RC_OK;
}

 RC destroyPageFile (char *fileName)
{
    
    if (remove(fileName) != 0) // check if file is removed
        return RC_FILE_NOT_FOUND;
    else
        printf("***Remove File Success!***");
    
    return RC_OK;
}


/****************** reading blocks from disc ********************/


/*
 * This method read block through calculating Page_Number * PAGE_SIZE in a file.
 * This method also estimate if we can success read block.
 */
 RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pf;
   
    if (pageNum < fHandle->totalNumPages && pageNum >= 0) {
        if (fHandle->mgmtInfo != NULL) {
        if(0 == fseek(fHandle->mgmtInfo, pageNum*4096*sizeof(char), SEEK_SET)){
        pf = fread(memPage, sizeof(char), 4096, fHandle->mgmtInfo);
        
        fHandle->curPagePos = pageNum;
        if (pf != 4096)
        {fputs ("Reading error",stderr); exit (3);}
            }
        }else{
            return  RC_FILE_HANDLE_NOT_INIT;
        }
    }else{
        return  RC_READ_NON_EXISTING_PAGE;
    }
     return RC_OK;
}


/*
 * Get current block position
 */
 int getBlockPos (SM_FileHandle *fHandle)
{
    if (fHandle->curPagePos >= 0) { // Check if value is illega.
        // If not, get current position.
        printf("***Get File Address Success!***");
        return fHandle->curPagePos;
    }else{
        printf("***Current Page Position is illegal, set it as 0 ***");
        return 0;
    }
}

/*
 * Call readBlcok method to read first block (position = 0)
 */
 RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fHandle->curPagePos = 0;  //Set current position as first block
    
    readBlock(fHandle->curPagePos, fHandle, memPage);
    printf("***Read 1st Block Success!***");
    
    return RC_OK;
}


/*
 * Call readBlcok method to read previous block (pre position = position -1).
 */

 RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pf;
    int preBlockPos = fHandle->curPagePos - 1;
    if (preBlockPos >= 0) {
        pf = readBlock (preBlockPos, fHandle, memPage);
        //fHandle->curPagePos = preBlockPos;
    }else{
        return RC_READ_NON_EXISTING_PAGE;
    }
    return RC_OK;
}


/*
 * Call readBlcok method to read current block(position = position).
 */
 RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    readBlock(fHandle->curPagePos, fHandle, memPage);
    printf("***Read Current Block Success!***");
    
    return RC_OK;
}



/*
 * Call readBlcok method to read next block(pre position = position +1).
 */
 RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pf;
    int nextBlockPos = fHandle->curPagePos + 1;
    if (nextBlockPos < fHandle->totalNumPages) {
        pf = readBlock (nextBlockPos, fHandle, memPage);
        //fHandle->curPagePos = nextBlockPos;
    }else{
        return RC_READ_NON_EXISTING_PAGE;
    }
    return RC_OK;
}


/*
 * Call readBlcok method to read last block(pre position = last page position).
 */
 RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pf;
    int lastBlockPos = fHandle->totalNumPages - 1;
    pf = readBlock (lastBlockPos, fHandle, memPage);
    return RC_OK;
}

/********************* writing blocks to a page file ************************/

/*
 * This method write content memPage into the position pageNum * PAGE_SIZE.
 * This method also estimate if we can success write something into block.
 */
 RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pf;
    if (pageNum < fHandle->totalNumPages && pageNum >= 0){
        if (sizeof(memPage) <= 4096*sizeof(char)) {
            if (fHandle->mgmtInfo != NULL){
            if(0 == fseek(fHandle->mgmtInfo, pageNum*4096*sizeof(char), SEEK_SET)){
            pf = fwrite (memPage , sizeof(char), 4096, fHandle->mgmtInfo);
            fHandle->curPagePos = pageNum;
            if (pf != 4096) {fputs ("writing error",stderr); exit (4);}
                return RC_OK;
                }
            }else{
                return  RC_FILE_HANDLE_NOT_INIT;
            }
        }
    }else{
        return  RC_READ_NON_EXISTING_PAGE;
    }
    return RC_WRITE_FAILED;
}


/*
 * Call writeBlock method to write memPage into current position(position = current position).
 */
 RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    int pf;
    RC ret = RC_WRITE_FAILED;
    int curBlockPos = fHandle->curPagePos;
    ret = writeBlock (curBlockPos, fHandle, memPage);
    return ret;
}


/*
 * This method call method writeBlock.
 * This method can add one empty block into the end of file.
 * The new last page should be filled with zero bytes.
 */
 RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    int pf;
    size_t sz = sizeof(char)*4096;
    char* buf = (char*)malloc(sz);
    if (buf == NULL) {fputs("Memory error",stderr); exit (2);}
    memset(buf, 0, sz);
    
    if (fHandle->mgmtInfo != NULL){
        if(0 == fseek(fHandle->mgmtInfo, 0, SEEK_END)){
            pf = fwrite (buf, sizeof(char), 4096, fHandle->mgmtInfo);
            if (pf != 4096) {fputs ("writing error",stderr); exit (4);}
            return  RC_OK;
        }
    }else{
        return  RC_FILE_HANDLE_NOT_INIT;
    }
    free(buf);
    return RC_WRITE_FAILED;
}


/*
 * This method call method appendEmptyBlock.
 * It can add any block into the end of file.
 * This method also estimate if we can success add blocks.
 */
 RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
     int pf;
    int i;
    int add = 0;
    if (numberOfPages > fHandle->totalNumPages) {
        add = numberOfPages - fHandle->totalNumPages; // Add empty page numberofPages - total number pages times
        for (i = 0; i < add; i++) {
            pf= appendEmptyBlock(fHandle);
            if(pf == RC_OK) fHandle->totalNumPages ++;
        }
         
    }else{
        return  RC_OK;
    }
    return RC_WRITE_FAILED;
}
