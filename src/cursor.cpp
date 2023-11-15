#include "global.h"

Cursor::Cursor(string tableName, int pageIndex)
{
    logger.log("Cursor::Cursor");
    this->page = bufferManager.getPage(tableName, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
}

/**
 * @brief This function reads the next row from the page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> Cursor::getNext()
{
    logger.log("Cursor::geNext");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    if (result.empty())
    {
        tableCatalogue.getTable(this->tableName)->getNextPage(this);
        if (!this->pagePointer)
        {
            result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
        }
    }
    return result;
}
/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page.
 *
 * @param pageIndex
 */
void Cursor::nextPage(int pageIndex)
{
    logger.log("Cursor::nextPage");
    this->page = bufferManager.getPage(this->tableName, pageIndex);
    this->pageIndex = pageIndex;
    this->pagePointer = 0;
}

/* Matrix cursor functions */

/**
 * @brief This function reads the next segment of the matrix's row from the page.
 * If the row is fully read, it moves to first segment of the next row. The index
 * of the current segment read from the page is indicated by the pagePointer
 * (points to row in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> Cursor::getMatrixNext()
{
    logger.log("Cursor::getMatrixNext");
    vector<int> result = this->page.getRow(this->pagePointer);
    tableCatalogue.getTable(this->tableName)->getNextPointer(this);

    return result;
}

/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page from the row pointed by pagePointer.
 *
 * @param pageIndex
 */
void Cursor::nextMatrixPage(int pageIndex, int pagePointer)
{
    logger.log("Cursor::nextMatrixPage");
    if (this->pageIndex != pageIndex)
        this->page = bufferManager.getPage(this->tableName, pageIndex);

    this->pageIndex = pageIndex;
    this->pagePointer = pagePointer;
}

/* Sorting cursor functions */

/**
 * @brief This is a cursor for subfiles during sorting of the table
 *
 * @param tableName
 * @param run
 * @param subfile
 * @param pageIndex
 */
Cursor::Cursor(string tableName, int run, int subfile, int pageIndex)
{
    logger.log("Cursor::Cursor Overload for Subfile");
    this->page = bufferManager.getPage(tableName, run, subfile, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->run = run;
    this->subfile = subfile;
    this->pageIndex = pageIndex;
}

/**
 * @brief This function reads the next row from the page of a subfile. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> Cursor::getSubfileNext()
{
    logger.log("Cursor::getSubfileNext");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    return result;
}
