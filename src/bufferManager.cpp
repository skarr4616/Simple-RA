#include "global.h"

BufferManager::BufferManager()
{
    logger.log("BufferManager::BufferManager");
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName
 * @param pageIndex
 * @return Page
 */
Page BufferManager::getPage(string tableName, int pageIndex)
{
    logger.log("BufferManager::getPage");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
        return this->getFromPool(pageName);
    else
        return this->insertIntoPool(tableName, pageIndex);
}

/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName
 * @return true
 * @return false
 */
bool BufferManager::inPool(string pageName)
{
    logger.log("BufferManager::inPool");
    for (auto page : this->pages)
    {
        if (pageName == page.pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName
 * @return Page
 */
Page BufferManager::getFromPool(string pageName)
{
    logger.log("BufferManager::getFromPool");
    for (auto page : this->pages)
        if (pageName == page.pageName)
            return page;
}

/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure.
 *
 * @param tableName
 * @param pageIndex
 * @return Page
 */
Page BufferManager::insertIntoPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool");
    Page page(tableName, pageIndex);
    if (this->pages.size() >= BLOCK_COUNT)
        pages.pop_front();
    pages.push_back(page);
    return page;
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements.
 *
 * @param tableName
 * @param pageIndex
 * @param rows
 * @param rowCount
 */
void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writePage");
    Page page(tableName, pageIndex, rows, rowCount);
    page.writePage();
}

/**
 * @brief Deletes file names fileName
 *
 * @param fileName
 */
void BufferManager::deleteFile(string fileName)
{
    if (remove(fileName.c_str()))
        logger.log("BufferManager::deleteFile: Err");
    else
        logger.log("BufferManager::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the tableName and pageIndex.
 *
 * @param tableName
 * @param pageIndex
 */
void BufferManager::deleteFile(string tableName, int pageIndex)
{
    logger.log("BufferManager::deleteFile");
    string fileName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
}

/* Matrix Functions */

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when a new row is to be appended to any existing page.
 *
 * @param tableName
 * @param pageIndex
 * @param rows
 */
void BufferManager::appendRowToPage(string tableName, int pageIndex, vector<int> row)
{
    logger.log("BufferManager::appendRowToPage");
    Page page(tableName, pageIndex, {row}, 1);
    page.appendRowToPage();
}

/**
 * @brief Function called to get the page pointer from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName
 * @param pageIndex
 * @return Page*
 */
Page *BufferManager::getPagePtr(string tableName, int pageIndex)
{
    logger.log("BufferManager::getPagePtr");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
        return this->getPtrFromPool(pageName);
    else
        return this->insertPtrIntoPool(tableName, pageIndex);
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page pointer. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName
 * @return Page*
 */
Page *BufferManager::getPtrFromPool(string pageName)
{
    logger.log("BufferManager::getPtrFromPool");
    for (auto it = this->pages.begin(); it != this->pages.end(); it++)
    {
        if (it->pageName == pageName)
        {
            return &(*it);
        }
    }
}

/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure.
 *
 * @param tableName
 * @param pageIndex
 * @return Page*
 */
Page *BufferManager::insertPtrIntoPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertPtrIntoPool");
    Page page(tableName, pageIndex);
    if (this->pages.size() >= BLOCK_COUNT)
        pages.pop_front();
    pages.push_back(page);
    return &(*(pages.end() - 1));
}

/* Sorting Functions */

/**
 * @brief Function called to read a subfile page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName
 * @param run
 * @param subfile
 * @param pageIndex
 * @return Page
 */
Page BufferManager::getPage(string tableName, int run, int subfile, int pageIndex)
{
    logger.log("BufferManager::getPage Overload for Subfile");
    string pageName = "../data/temp/" + tableName + "_" + to_string(run) + "_" + to_string(subfile) + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
        return this->getFromPool(pageName);
    else
        return this->insertIntoPool(tableName, run, subfile, pageIndex);
}

/**
 * @brief Inserts page indicated by tableName, run, subfile pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure.
 *
 * @param tableName
 * @param run
 * @param subfile
 * @param pageIndex
 * @return Page
 */
Page BufferManager::insertIntoPool(string tableName, int run, int subfile, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool Overload for Subfile");
    Page page(tableName, run, subfile, pageIndex);
    if (this->pages.size() >= BLOCK_COUNT)
        pages.pop_front();
    pages.push_back(page);
    return page;
}

/**
 * @brief Removes page indicated by tableName, and pageIndex from pool.
 *
 * @param tableName
 * @param pageIndex
 */
void BufferManager::removeFromPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::removeFromPool");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    for (auto it = this->pages.begin(); it != this->pages.end(); it++)
    {
        if (it->pageName == pageName)
        {
            pages.erase(it);
            return;
        }
    }
}

/**
 * @brief This is called when new pages for subfiles are created for sorting
 *
 * @param tableName
 * @param run
 * @param subfile
 * @param pageIndex
 * @param rows
 * @param rowCount
 */
void BufferManager::writePage(string tableName, int run, int subfile, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("BufferManager::writePage Overload for Subfile");
    Page page(tableName, run, subfile, pageIndex, rows, rowCount);
    page.writePage();
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the tableName, run, subfile and pageIndex.
 *
 * @param tableName
 * @param run
 * @param subfile
 * @param pageIndex
 */
void BufferManager::deleteFile(string tableName, int run, int subfile, int pageIndex)
{
    logger.log("BufferManager::deleteFile Overload for Subfile");
    string fileName = "../data/temp/" + tableName + "_" + to_string(run) + "_" + to_string(subfile) + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
}