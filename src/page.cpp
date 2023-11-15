#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{
    this->pageName = "";
    this->tableName = "";
    this->pageIndex = -1;
    this->rowCount = 0;
    this->columnCount = 0;
    this->rows.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName
 * @param pageIndex
 */
Page::Page(string tableName, int pageIndex)
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
    Table table = *tableCatalogue.getTable(tableName);

    this->columnCount = table.columnCount;
    if (table.tableType == "MATRIX")
    {
        this->columnCount = table.columnsPerBlockCount[pageIndex];
    }

    unsigned int maxRowCount = table.maxRowsPerBlock;
    vector<int> row(columnCount, 0);
    this->rows.assign(maxRowCount, row);

    ifstream fin(pageName, ios::in);
    this->rowCount = table.rowsPerBlockCount[pageIndex];
    int number;
    for (unsigned int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
        {
            fin >> number;
            this->rows[rowCounter][columnCounter] = number;
        }
    }
    fin.close();

    blocksRead++;
}

/**
 * @brief Get row from page indexed by rowIndex
 *
 * @param rowIndex
 * @return vector<int>
 */
vector<int> Page::getRow(int rowIndex)
{
    logger.log("Page::getRow");
    vector<int> result;
    result.clear();
    if (rowIndex >= this->rowCount)
        return result;
    return this->rows[rowIndex];
}

Page::Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
}

/**
 * @brief writes current page contents to file.
 *
 */
void Page::writePage()
{
    logger.log("Page::writePage");
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
    blocksWritten++;
}

/* Matrix Functions */

/**
 * @brief appends a row to the current page and its page file.
 *
 */
void Page::appendRowToPage()
{
    logger.log("Page::appendRowToPage");

    ofstream fout(pageName, ios::out | ios::app);

    for (int i = 0; i < this->rows[0].size(); i++)
    {
        if (i != 0)
            fout << " ";

        fout << this->rows[0][i];
    }

    fout << endl;
    fout.close();
    blocksWritten++;
}

/**
 * @brief rewrites the current page rows and contents to file.
 *
 * @param mat matrix to rewrite with
 */
void Page::reWritePage(vector<vector<int>> &mat)
{
    logger.log("Page::reWritePage");

    int resizedRowSize = mat.size();
    int resizedColSize = mat[0].size();
    this->rowCount = resizedRowSize;
    this->columnCount = resizedColSize;

    this->rows.resize(this->rowCount);
    for (int r = 0; r < this->rowCount; r++)
    {
        this->rows[r].resize(this->columnCount);
        for (int c = 0; c < this->columnCount; c++)
        {
            this->rows[r][c] = mat[r][c];
        }
    }

    this->writePage();
}

/* Sorting Functions */

Page::Page(string tableName, int run, int subfile, int pageIndex)
{
    logger.log("Page::Page Overload for Subfile");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->pageName = "../data/temp/" + this->tableName + "_" + to_string(run) + "_" + to_string(subfile) + "_Page" + to_string(pageIndex);
    Table table = *tableCatalogue.getTable(tableName);

    this->columnCount = table.columnCount;
    if (table.tableType == "MATRIX")
    {
        this->columnCount = table.columnsPerBlockCount[pageIndex];
    }

    unsigned int maxRowCount = table.maxRowsPerBlock;
    vector<int> row(columnCount, 0);
    this->rows.assign(maxRowCount, row);

    ifstream fin(pageName, ios::in);
    this->rowCount = table.rowsPerBlockCount[pageIndex];
    int number;
    for (unsigned int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
        {
            fin >> number;
            this->rows[rowCounter][columnCounter] = number;
        }
    }
    fin.close();

    blocksRead++;
}

Page::Page(string tableName, int run, int subfile, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("Page::Page Overload for Subfile");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/" + this->tableName + "_" + to_string(run) + "_" + to_string(subfile) + "_Page" + to_string(pageIndex);
}