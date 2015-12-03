#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>


#include "buffer_mgr.h"
#include "storage_mgr.h"


int POOL_CUR_NO = 0;
int GLOBAL_FLAG_FIFO = 1;
int GLOBAL_FLAG_LRU = 1;

//Extend the pagehandle structure, store more info into it
typedef struct PageFrame{
    BM_PageHandle ph;
    int flag;      // set a flag to determine 
    int curBufferPool;
    int fixcount;  // count each page frame fixount
    bool isDirty;  // count each page if dirty
}PageFrame;

//create a structure to store read, write info
typedef struct IO_COUNT{
    int TotalRead;  // record the total read times
    int TotalWrite;  // record the total write times
}IO_COUNT;

IO_COUNT record[50];		//create an array to record each buffer pool IO manipulation


/*************************************** Buffer Pool Function *******************************************/

// MOD
PageFrame* initPageFrame(int RecordPos) //initial a new page Frame
{
    PageFrame *pf;
    BM_PageHandle *pph;
    
    pf = (PageFrame*) malloc (sizeof(PageFrame));
    pf->fixcount = 0;         // initial fixcount as 0
    pf->isDirty = FALSE;      // initial page dirty as false
    pf->flag = 0;             // initial flag as 0
    pf->curBufferPool = RecordPos;   // set current buffer pool
    pf->ph.data = (char*)malloc(sizeof(char)*4096);
    pf->ph.pageNum = NO_PAGE;
    
//    pph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
//    pph->pageNum = NO_PAGE;   // intial the page number as -1
//    pph->data = (char*) malloc(4096); // alloc a memory to stay the data
//    pf->ph = *pph;
    
    return pf;
}


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData )
{
    int i = 0;
    
    //Initial  IOrecord
    record[POOL_CUR_NO].TotalRead = 0;
    record[POOL_CUR_NO].TotalWrite = 0;
    
    //Initial the  page frame, and store them as an array
    PageFrame *WholePageFrame;
    WholePageFrame = (PageFrame*)malloc(sizeof(PageFrame)*numPages);
    
    //initial each page frame
    for(i=0; i<numPages; i++)
    {
        WholePageFrame[i] = *initPageFrame(POOL_CUR_NO);
    }
    //Compelet the initial operation of buffer pool
    bm->pageFile = (char *)pageFileName;
    //printf("page File Name is %s \n", bm->pageFile);
    bm->numPages = numPages;
    //printf("page number is %d \n", bm->numPages);
    bm->strategy = strategy;  // set strategy
   // printf("page stratagy is %d \n", bm->strategy);
    bm->mgmtData = WholePageFrame; // mgmData point to the head of array
    
    POOL_CUR_NO++; // after finish a buffer pool, pool count + 1
    
    return RC_OK;
}


// destroys a buffer pool. This method should free up all resources associated
// with buffer pool. For example, it should free the memory allocated for page
// frames. If the buffer pool contains any dirty pages, then these pages should
// be written back to disk before destroying the pool. It is an error to
// shutdown a buffer pool that has pinned pages

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    int i;
    PageFrame *pf;
   // SM_FileHandle fileHandle;
    
//Get the handle of Buffer Pool
    pf = bm->mgmtData;
    bool B = TRUE;
    
    for (i = 0; i< bm->numPages; i++) {
        if (pf[i].fixcount!= 0) {
            return RC_WRITE_FAILED;
            
        }else if (pf[i].fixcount == 0 && pf[i].isDirty == true){
            B = FALSE;
            
        }else if (pf[i].fixcount== 0 && pf[i].isDirty == false){
            B = FALSE;
        }
    }
    
    free(pf);
    return RC_OK;
}

/****  Causes all dirty pages with fix count 0 from buffer pool to be written to disk ****/  //MOD
RC forceFlushPool(BM_BufferPool *const bm)
{
    int i;
    PageFrame *pf;
    pf = bm->mgmtData;
    
    //Open the file which will be flush to
    SM_FileHandle fileHandle;
//    SM_PageHandle pagehandle;
    openPageFile(bm->pageFile, &fileHandle);
    
    for(i= 0; i< bm->numPages; i++)
    {
        //check all the pages, if if is dirty, write it back
        if( pf[i].isDirty == TRUE && pf[i].fixcount== 0 )
        {
            ensureCapacity((pf->ph.pageNum)+1, &fileHandle);
            writeBlock(pf[i].ph.pageNum, &fileHandle, pf[i].ph.data);
            pf[i].isDirty = FALSE;
       //     record[pf->curBufferPool].TotalWrite++;
        }
    }
    closePageFile(&fileHandle);
    return RC_OK;
}




/*********************************** Page Management Functions ********************************************/

//marks a page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i;
    int n;
    PageFrame *pf;
    pf = bm->mgmtData;
    
    n = page->pageNum;
    for(i = 0; i<bm->numPages; i++)
    {
        if(pf != NULL && pf[i].ph.pageNum == page->pageNum){
            pf[i].isDirty = true;
            printf("Mark dirty success!\n");
            return RC_OK;
        }
    }
    
    return false;
}


//Unpins the page.The pageNum field of page should be used to figure out which page to unpin // MOD
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i;
    PageFrame *pf;
    pf = bm->mgmtData;
    SM_FileHandle fh;
    
    printf("unpin pageNum %d\n",page->pageNum);
    for (i = 0; i < bm->numPages; i++) {
        
        if (pf[i].ph.pageNum == page->pageNum && pf[i].fixcount> 0){  // find the page
            if (pf[i].isDirty == true ) {   // if dirty, write it back to disk
                strcpy(pf[i].ph.data, page->data);
                
                openPageFile(bm->pageFile, &fh);
                ensureCapacity(pf[i].ph.pageNum+1, &fh);
                writeBlock(page->pageNum, &fh, pf[i].ph.data);
                record[pf->curBufferPool].TotalWrite++;       //update record info
                printf("unpin finish writing!!! total %d, %d: %s\n", fh.totalNumPages, pf[i].ph.pageNum, pf[i].ph.data);
                closePageFile(&fh);

            }
            pf[i].fixcount--;          // fixcount decrease
            printf("pf[%d] fixcount:%d \n",i,pf[i].fixcount);
            printf("Enter unpin loop->if!\n");
            return RC_OK;
        }
    }
    printf("Unpin page not success!\n");
    return RC_OK;
}



// write the current content of the page back to the page file on disk // MOD
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i;
    PageFrame *pf;
    SM_FileHandle fileHandle;
    
    pf = bm->mgmtData;

    if(page->pageNum == NO_PAGE || pf == NULL)   // check if data is illega
        return false;
    
    for (i = 0; i < bm->numPages; i++) {
        if (pf[i].ph.pageNum == page->pageNum){  // find the page , writ it back to disk
           
            openPageFile(bm->pageFile, &fileHandle);
            ensureCapacity(page->pageNum, &fileHandle);
            writeBlock(pf[i].ph.pageNum, &fileHandle, pf[i].ph.data);
            closePageFile(&fileHandle);
            
            record[pf->curBufferPool].TotalWrite++;
            printf("force page success!\n");
            return RC_OK;
            }
    }
   return RC_OK;

}






// pins the page with page number pageNum. The buffer manager is responsible to set the pageNum
// field of the page handle passed to the method. Similarly, the data field should point to the page
// frame the page is stored in
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    int i,n, pos, postemp;
    PageFrame *pf, *pftemp, *pf2;
    page->data = (char*) malloc (sizeof(char)* 4096);
    pf = bm->mgmtData;
    pf2 = (PageFrame*)malloc(sizeof(PageFrame));
    SM_FileHandle fh;
    
    openPageFile(bm->pageFile, &fh);
    
    switch (bm->strategy) {
        case RS_FIFO:  // FIFO strategy
            printf("------FIFO \n");
            printf("---%d \n", pageNum);
            printf("---%d \n", page->pageNum);
            
            for (i = 0; i < bm->numPages; i++) {
                // if find the same page in buffer pool
                if (pf[i].ph.pageNum == pageNum) {
                    strcpy(page->data, pf[i].ph.data);
                    page->pageNum = pageNum;
                    
                    pf[i].fixcount++;
                    pf[i].flag = GLOBAL_FLAG_FIFO;
                    printf("pf[%d] IsDirty: %d, Fixcount: %d",i, pf[i].isDirty, pf[i].fixcount);
                    printf("------Find the same page\n");
                    return RC_OK;
                }
                
                // if find page number = -1
                if (pf[i].ph.pageNum == NO_PAGE ) {
                    pf[i].ph.pageNum = pageNum;
                    page->pageNum = pageNum;
                    pf[i].flag = GLOBAL_FLAG_FIFO;
                    
                    ensureCapacity(pageNum, &fh);
                   
                    readBlock(pageNum, &fh, pf[i].ph.data);
                    record[pf->curBufferPool].TotalRead++;
                    strcpy(page->data, pf[i].ph.data);
                    closePageFile(&fh);
                    
                    GLOBAL_FLAG_FIFO++;
                    pf[i].fixcount++;
                    printf("pf[%d] IsDirty: %d, Fixcount: %d",i, pf[i].isDirty, pf[i].fixcount);
                    printf("------Add to NO_PAGE\n");
                    return RC_OK;
                }

            }
              // if can't find the page in buffer pool, replace the lowest flag one
               n  =  0;
               pos = pf[0].flag;
                pos=65535;
                for (i = 0; i < bm->numPages; i++) {
                    postemp = pf[i].flag;
                    if (postemp < pos && pf[i].fixcount == 0) {
                        pos = postemp;
                        n = i;
                    }
                }
            
            printf("found position : %d\n", n);
            printPoolContent(bm);
            
                if (pf[n].fixcount == 0) {
                    pf[n].fixcount++;
                    pf[n].ph.pageNum = pageNum;
                   
                    page->pageNum = pageNum;
                    pf[n].flag = GLOBAL_FLAG_FIFO;
                    printf("!!!pin page before readblock  %d: %s\n", pageNum, page->data);
                    
                    printf("pin page: %d : %d\n", fh.totalNumPages, pageNum);
//                   ensureCapacity(pageNum+1, &fh);
                    
                    if (pageNum >= fh.totalNumPages) {
                        memset(page->data, 0, 4096);
                        memset(pf[n].ph.data, 0, 4096);
                    }
                    else {
                        printf("read!!\n");
//                      readBlock(pageNum, &fh, pf[n].ph.data);
//                      strcpy(page->data, pf[i].ph.data);
                        readBlock(pageNum, &fh, page->data);
                        strcpy(pf[n].ph.data, page->data);
                    }
                    
                    printf("!!!pin page readblock  %d: %s\n", pageNum, page->data);
                    
                    
                    closePageFile(&fh);
                    GLOBAL_FLAG_FIFO++;
                    record[pf->curBufferPool].TotalRead++;
                    
                    printf("------Add into low flag \n");
                    printf("pf[%d] IsDirty: %d, Fixcount: %d",n, pf[n].isDirty, pf[n].fixcount);
                    return RC_OK;
                }
            
            closePageFile(&fh);
            break;
            
            
        case RS_LRU:  // LRU strategy
        {
            printf("------LRU \n");
            printf("---%d \n", pageNum);
            printf("---%d \n", page->pageNum);
            
            for (i = 0; i < bm->numPages; i++) {
                // if find the same page in buffer pool
                if (pf[i].ph.pageNum == NO_PAGE ) {
                    pf[i].ph.pageNum = pageNum;
                    page->pageNum = pageNum;
                    pf[i].flag = GLOBAL_FLAG_LRU;
                    
                    ensureCapacity(pageNum, &fh);
                    
                    readBlock(pageNum, &fh, pf[i].ph.data);
                    record[pf->curBufferPool].TotalRead++;
                    strcpy(page->data, pf[i].ph.data);
                    closePageFile(&fh);
                    
                    GLOBAL_FLAG_LRU++;
                    pf[i].fixcount++;
                    printf("pf[%d] IsDirty: %d, Fixcount: %d",i, pf[i].isDirty, pf[i].fixcount);
                    printf("------Add to NO_PAGE\n");
                    return RC_OK;
                }else if(pf[i].ph.pageNum == pageNum && pageNum != NO_PAGE) { // if find the same page in buffer pool
                    strcpy(page->data, pf[i].ph.data);
                    page->pageNum = pageNum;
                    
                    pf[i].fixcount++;
                    pf[i].flag = GLOBAL_FLAG_LRU;
                    
                    GLOBAL_FLAG_LRU++;
                    printf("pf[%d] IsDirty: %d, Fixcount: %d",i, pf[i].isDirty, pf[i].fixcount);
                    printf("------Find the same page\n");
                    return RC_OK;
                }
            }
             // if can't find the page in buffer pool, replace the lowest flag one
            n  =  0;
            pos = pf[0].flag;
            pos=65535;
            for (i = 0; i < bm->numPages; i++) {
                postemp = pf[i].flag;
                if (postemp < pos && pf[i].fixcount == 0) {
                    pos = postemp;
                    n = i;
                }
            }
            
            printf("found position : %d\n", n);
            printPoolContent(bm);
            
            if (pf[n].fixcount == 0) {
                pf[n].fixcount++;
                pf[n].ph.pageNum = pageNum;
                
                page->pageNum = pageNum;
                pf[n].flag = GLOBAL_FLAG_LRU;
                printf("!!!pin page before readblock  %d: %s\n", pageNum, page->data);
                
                printf("pin page: %d : %d\n", fh.totalNumPages, pageNum);
                //                   ensureCapacity(pageNum+1, &fh);
                
                if (pageNum >= fh.totalNumPages) {
                    memset(page->data, 0, 4096);
                    memset(pf[n].ph.data, 0, 4096);
                }
                else {
                    printf("read!!\n");
                    //                      readBlock(pageNum, &fh, pf[n].ph.data);
                    //                      strcpy(page->data, pf[i].ph.data);
                    readBlock(pageNum, &fh, page->data);
                    strcpy(pf[n].ph.data, page->data);
                }
                
                printf("!!!pin page readblock  %d: %s\n", pageNum, page->data);
                
                
                closePageFile(&fh);
                GLOBAL_FLAG_LRU++;
                record[pf->curBufferPool].TotalRead++;
                
                printf("------Add into low flag \n");
                printf("pf[%d] IsDirty: %d, Fixcount: %d",n, pf[n].isDirty, pf[n].fixcount);
                return RC_OK;
            }
            break;
        }
        default:
            break;
    }
    
    return RC_OK;
    }


/************************************* Statistics Functions *******************************************/



//function returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame. An empty page frame is represented using the constant NO_PAGE.  //MOD
PageNumber *getFrameContents(BM_BufferPool *const bm)
{

    int i, pn;
    PageFrame *pf;
    pf = bm->mgmtData;
    pn = bm->numPages;
    int *record1;

    record1 = (int*)malloc(sizeof(int)*pn);
    if(pf == NULL)
        return false;
    
    for (i = 0 ; i < bm->numPages; i++) {
        record1[i] = pf[i].ph.pageNum;         // get an array about each page frame page number
    }
    return record1;
    
}

//returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty. Empty page frames are considered as clean.  //MOD
bool *getDirtyFlags(BM_BufferPool *const bm)
{
   // int num = bm->numPages;
    int i, pos, pn;
    PageFrame *pf;
    pf = bm->mgmtData;
    pn = bm->numPages;
    bool *record2;
    
    record2 = (bool*)malloc(sizeof(bool)*pn);
    if(pf == NULL)
        return false;
    
    for (i = 0 ; i < bm->numPages; i++) {
        record2[i] = pf[i].isDirty;          // get an array about each page frame if dirty
    }
    return record2;
}

//returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. Return 0 for empty page frames.   //MOD
int *getFixCounts(BM_BufferPool *const bm)
{
  
    int i, pn;
    PageFrame *pf;
    pf = bm->mgmtData;
    pn = bm->numPages;
    int *record3;
    
    record3 = (int*)malloc(sizeof(int)*pn);

    if(pf == NULL)
        return false;
    
    for (i = 0 ; i < bm->numPages; i++) {
        record3[i] = pf[i].fixcount;        // get an array about each page frame fixcount
    }
    return record3;
    
}

// returns the number of pages that have been read from disk since a buffer pool has been initialized.
// code is responsible to initializing this statistic at pool creating time and update whenever a page
// is read from the page file into a page frame.  //MOD
int getNumReadIO(BM_BufferPool *const bm)
{
    int num;
    PageFrame *pf;
    pf = bm->mgmtData;
    
    num = record[pf->curBufferPool].TotalRead; // get buffer total read number
    return num;
}

//returns the number of pages written to the page file since the buffer pool has been initialized.  //MOD
int getNumWriteIO(BM_BufferPool *const bm)
{
    int num;
    PageFrame *pf;
    pf = bm->mgmtData;
    
    num = record[pf->curBufferPool].TotalWrite; // get buffer total write number
    return num;
}
