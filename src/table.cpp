#include "global.h"

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName
 */
Table::Table(string tableName, string tableType)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
    this->tableType = tableType;
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName
 * @param columns
 */
Table::Table(string tableName, vector<string> columns)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableType = "RELATION";
    this->tableName = tableName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (unsigned int)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->writeRow<string>(columns);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded
 * @return false if an error occurred
 */
bool Table::load()
{
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNames(line))
            if (this->blockify())
                return true;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file.
 *
 * @param line
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
    logger.log("Table::extractColumnNames");

    if (this->tableType == "MATRIX")
    {
        // Only counts the number of columns in the matrix and calculates the subMatrix size
        unsigned int N = 0;
        string word;
        stringstream s(firstLine);

        while (getline(s, word, ','))
        {
            N++;
        }

        this->columnCount = N;
        this->rowCount = N;
        this->maxRowsPerBlock = (unsigned int)sqrt((unsigned int)((BLOCK_SIZE * 1000) / (sizeof(int))));
        this->subMatrixSize = (unsigned int)(this->columnCount / this->maxRowsPerBlock);
        if (this->columnCount % this->maxRowsPerBlock != 0)
        {
            this->subMatrixSize++;
        }

        return true;
    }

    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (columnNames.count(word))
            return false;
        columnNames.insert(word);
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    this->maxRowsPerBlock = (unsigned int)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size.
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    if (this->tableType == "MATRIX")
    {
        return this->blockifyMatrix();
    }

    logger.log("Table::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->columnCount, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    unordered_set<int> dummy;
    dummy.clear();
    this->distinctValuesInColumns.assign(this->columnCount, dummy);
    this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    getline(fin, line);
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            row[columnCounter] = stoi(word);
            rowsInPage[pageCounter][columnCounter] = row[columnCounter];
        }
        pageCounter++;
        this->updateStatistics(row);
        if (pageCounter == this->maxRowsPerBlock)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }
    }
    if (pageCounter)
    {
        bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(pageCounter);
        pageCounter = 0;
    }

    if (this->rowCount == 0)
        return false;
    this->distinctValuesInColumns.clear();
    return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row
 */
void Table::updateStatistics(vector<int> row)
{
    this->rowCount++;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (!this->distinctValuesInColumns[columnCounter].count(row[columnCounter]))
        {
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName
 * @return true
 * @return false
 */
bool Table::isColumn(string columnName)
{
    logger.log("Table::isColumn");
    for (auto col : this->columns)
    {
        if (col == columnName)
        {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName
 * @param toColumnName
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (columns[columnCounter] == fromColumnName)
        {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print()
{
    logger.log("Table::print");
    unsigned int count = min((long long)PRINT_COUNT, this->rowCount);

    // print headings
    this->writeRow(this->columns, cout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, cout);
    }
    printRowCount(this->rowCount);
}

/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor
 * @return vector<int>
 */
void Table::getNextPage(Cursor *cursor)
{
    logger.log("Table::getNext");

    if (cursor->pageIndex < this->blockCount - 1)
    {
        cursor->nextPage(cursor->pageIndex + 1);
    }
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent");
    if (!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    // print headings
    this->writeRow(this->columns, fout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, fout);
    }
    fout.close();
}

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
        return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload()
{
    logger.log("Table::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 *
 * @return Cursor
 */
Cursor Table::getCursor()
{
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 *
 * @param columnName
 * @return int
 */
int Table::getColumnIndex(string columnName)
{
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
}

/* Matrix Functions */

/**
 * @brief This function splits the matrix into submatrices and stores them in multiple files of
 * one block size.
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockifyMatrix()
{
    logger.log("Table::blockifyMatrix");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;

    vector<int> row;
    int rowCounter = 0;

    while (getline(fin, line))
    {
        stringstream s(line);
        int columnCounter = 0;
        int pageIndex = this->blockCount;

        for (int i = 0; i < this->columnCount; i++)
        {
            if (!getline(s, word, ','))
                return false;

            row.emplace_back(stoi(word));
            columnCounter++;

            if (columnCounter == this->maxRowsPerBlock)
            {
                if (rowCounter == 0)
                {
                    this->columnsPerBlockCount.emplace_back(columnCounter);
                    bufferManager.writePage(this->tableName, pageIndex, {row}, 1);
                }
                else
                {
                    bufferManager.appendRowToPage(this->tableName, pageIndex, row);
                }

                columnCounter = 0;
                row.clear();
                pageIndex++;
            }
        }

        if (columnCounter)
        {
            if (rowCounter == 0)
            {
                this->columnsPerBlockCount.emplace_back(columnCounter);
                bufferManager.writePage(this->tableName, pageIndex, {row}, 1);
            }
            else
            {
                bufferManager.appendRowToPage(this->tableName, pageIndex, row);
            }

            row.clear();
        }

        rowCounter++;

        if (rowCounter == this->maxRowsPerBlock)
        {
            for (int num = 0; num < this->subMatrixSize; num++)
                this->rowsPerBlockCount.emplace_back(rowCounter);

            this->blockCount += this->subMatrixSize;
            rowCounter = 0;
        }
    }
    if (rowCounter)
    {
        for (int num = 0; num < this->subMatrixSize; num++)
            this->rowsPerBlockCount.emplace_back(rowCounter);

        this->blockCount += this->subMatrixSize;
    }

    return true;
}

/**
 * @brief Function prints the first few rows of the matrix. If the size of the matrix is
 * more than PRINT_COUNT, top left submatrix of size PRINT_COUNT is printed
 *
 */
void Table::printMatrix()
{
    logger.log("Table::printMatrix");

    unsigned int count = min((long long)PRINT_COUNT, this->rowCount);
    Cursor cursor(this->tableName, 0);

    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        int remaining = count;
        vector<int> row;
        bool lastSegment = false;

        for (int blockCounter = 0; blockCounter < this->subMatrixSize; blockCounter++)
        {
            row = cursor.getMatrixNext();
            if (remaining <= row.size())
            {
                row.resize(remaining);
                lastSegment = true;
            }

            this->writeMatrixRowSegment(row, cout, blockCounter == 0, lastSegment);
            remaining -= row.size();

            // Checks which page to go to when a row is complete
            if (remaining == 0 && blockCounter != this->subMatrixSize - 1 && rowCounter != count - 1)
            {
                int pageIndex = cursor.pageIndex - 1;
                int pageR = pageIndex / this->subMatrixSize;

                int nextPageIndex = -1;
                int nextPagePointer = -1;

                if (rowCounter % this->maxRowsPerBlock == this->maxRowsPerBlock - 1)
                {
                    nextPageIndex = (pageR + 1) * this->subMatrixSize;
                    nextPagePointer = 0;
                }
                else
                {
                    nextPageIndex = pageR * this->subMatrixSize;
                    nextPagePointer = cursor.pagePointer + 1;
                }

                cursor.nextMatrixPage(nextPageIndex, nextPagePointer);
                break;
            }

            if (remaining == 0 && rowCounter == count - 1)
            {
                break;
            }
        }
    }

    printRowCount(this->rowCount);
}

/**
 * @brief called when table is a matrix and EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */

void Table::makeMatrixPermanent()
{
    logger.log("Table::makeMatrixPermanent");

    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    Cursor cursor(this->tableName, 0);

    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        int remaining = this->columnCount;
        vector<int> row;
        bool lastSegment = false;

        for (int blockCounter = 0; blockCounter < this->subMatrixSize; blockCounter++)
        {
            row = cursor.getMatrixNext();
            if (remaining <= row.size())
            {
                row.resize(remaining);
                lastSegment = true;
            }

            this->writeMatrixRowSegment(row, fout, blockCounter == 0, lastSegment);
            remaining -= row.size();
        }
    }
}

/**
 * @brief Renames the matrix indicated by toMatrixName. All the pages of matrix and
 * the name in tableCatalogue is also updated.
 *
 * @param toMatrixName
 */
void Table::renameMatrix(string toMatrixName)
{
    logger.log("Table::renameMatrix");

    if (this->tableName == toMatrixName)
        return;

    string oldName = this->tableName;
    this->tableName = toMatrixName;
    tableCatalogue.renameTable(oldName, toMatrixName);

    // Renaming each page of the matrix
    for (int blockIndex = 0; blockIndex < this->blockCount; blockIndex++)
    {
        string oldPageName = "../data/temp/" + oldName + "_Page" + to_string(blockIndex);
        string newPageName = "../data/temp/" + toMatrixName + "_Page" + to_string(blockIndex);
        rename(oldPageName.c_str(), newPageName.c_str());
    }
}

/**
 * @brief executes when table is a matrix and CHECKSYMMETRY command is invoked.
 * It checks whether the matrix is symmetric or not
 *
 * @return True if matrix is symmetric, else False
 */
bool Table::checkSymmetry()
{
    logger.log("Table::checkSymmetry");

    bool isSymmetric = true;
    Cursor cursor(this->tableName, 0);

    for (int subMatrixR = 0; subMatrixR < this->subMatrixSize; subMatrixR++)
    {
        for (int subMatrixC = subMatrixR; subMatrixC < this->subMatrixSize; subMatrixC++)
        {
            if (subMatrixC == subMatrixR)
            {
                int pageIndex = subMatrixR * this->subMatrixSize + subMatrixC;
                cursor.nextPage(pageIndex);

                vector<int> row(this->columnsPerBlockCount[pageIndex]);
                vector<vector<int>> mat(this->rowsPerBlockCount[pageIndex], row);

                for (int i = 0; i < this->rowsPerBlockCount[pageIndex]; i++)
                {
                    row = cursor.getNext();
                    mat[i] = row;
                }

                if (!checkMatrixSymmetry(mat))
                {
                    isSymmetric = false;
                    break;
                }
            }
            else
            {
                int pageIndex1 = subMatrixR * this->subMatrixSize + subMatrixC;
                cursor.nextPage(pageIndex1);

                vector<int> row(this->columnsPerBlockCount[pageIndex1]);
                vector<vector<int>> mat1(this->rowsPerBlockCount[pageIndex1], row);

                for (int i = 0; i < this->rowsPerBlockCount[pageIndex1]; i++)
                {
                    row = cursor.getNext();
                    mat1[i] = row;
                }

                int pageIndex2 = subMatrixC * this->subMatrixSize + subMatrixR;
                cursor.nextPage(pageIndex2);

                row.resize(this->columnsPerBlockCount[pageIndex2]);
                vector<vector<int>> mat2(this->rowsPerBlockCount[pageIndex2], row);

                for (int i = 0; i < this->rowsPerBlockCount[pageIndex2]; i++)
                {
                    row = cursor.getNext();
                    mat2[i] = row;
                }

                if (!checkMatrixSymmetry(mat1, mat2))
                {
                    isSymmetric = false;
                    break;
                }
            }
        }

        if (isSymmetric == false)
        {
            break;
        }
    }

    return isSymmetric;
}

/**
 * @brief called when the TRANSPOSE command is invokes. Transposes the matrix in-place and rewrites its pages
 *
 */
void Table::transpose()
{
    logger.log("Table::transpose");

    for (int i = 0; i < this->blockCount; i++)
    {
        int r = i / this->subMatrixSize, c = i % this->subMatrixSize;

        if (r > c)
            continue;

        else if (r == c)
        {
            vector<vector<int>> temp_sub_matrix;
            Cursor cursor(this->tableName, i);

            for (int k = 0; k < this->rowsPerBlockCount[i]; k++)
            {
                vector<int> temp_row = cursor.getNext();
                temp_sub_matrix.push_back(temp_row);
            }

            int x = temp_sub_matrix.size(), y = temp_sub_matrix[0].size();
            for (int p = 0; p < x; p++)
            {
                for (int q = 0; q < y; q++)
                {
                    if (q < p)
                    {
                        swap(temp_sub_matrix[p][q], temp_sub_matrix[q][p]);
                    }
                }
            }

            Page *pageToBeMod = bufferManager.getPagePtr(this->tableName, i);
            pageToBeMod->reWritePage(temp_sub_matrix);
        }
        else
        {
            int pageNum2 = c * (this->subMatrixSize) + r;

            Cursor cursor1(this->tableName, i);
            Cursor cursor2(this->tableName, pageNum2);

            vector<vector<int>> temp_sub_matrix1;
            for (int k = 0; k < this->rowsPerBlockCount[i]; k++)
            {
                vector<int> temp_row = cursor1.getNext();
                temp_sub_matrix1.push_back(temp_row);
            }

            vector<vector<int>> temp_sub_matrix2;
            for (int k = 0; k < this->rowsPerBlockCount[pageNum2]; k++)
            {
                vector<int> temp_row = cursor2.getNext();
                temp_sub_matrix2.push_back(temp_row);
            }

            int x = temp_sub_matrix1.size(), y = temp_sub_matrix1[0].size();
            for (int p = 0; p < x; p++)
            {
                for (int q = 0; q < y; q++)
                {
                    swap(temp_sub_matrix1[p][q], temp_sub_matrix2[q][p]);
                }
            }

            Page *pageToBeMod1 = bufferManager.getPagePtr(this->tableName, i);
            Page *pageToBeMod2 = bufferManager.getPagePtr(this->tableName, pageNum2);
            pageToBeMod1->reWritePage(temp_sub_matrix1);
            pageToBeMod2->reWritePage(temp_sub_matrix2);
        }
    }
}

/**
 * @brief Function that checks if a subMatrix is symmetric or not
 *
 * @param mat1
 *
 * @return True if submatrix is symmetric, else False
 */
bool Table::checkMatrixSymmetry(vector<vector<int>> &mat)
{
    logger.log("Table::checkMatrixSymmetry");
    int n = mat.size();
    for (int r = 0; r < n; r++)
    {
        for (int c = r + 1; c < n; c++)
        {
            if (mat[r][c] != mat[c][r])
                return false;
        }
    }

    return true;
}

/**
 * @brief Overloaded function that checks if transpose of subMatrix_1 is equal to subMatrix_2 or not
 *
 * @param mat1
 * @param mat2
 *
 * @return True if subMatrix_1 is transpose of subMatrix_2, else False
 */
bool Table::checkMatrixSymmetry(vector<vector<int>> &mat1, vector<vector<int>> &mat2)
{
    logger.log("Table::checkMatrixSymmetry");

    int m = mat1.size();
    int n = mat1[0].size();

    for (int r = 0; r < m; r++)
    {
        for (int c = 0; c < n; c++)
        {
            if (mat1[r][c] != mat2[c][r])
                return false;
        }
    }

    return true;
}

/**
 * @brief This function moves cursor to point to the next segment
 * of the matrix's row or to the starting of the next row if exists.
 *
 * @param cursor
 */
void Table::getNextPointer(Cursor *cursor)
{
    logger.log("Table::getNextPointer");

    if ((cursor->pageIndex + 1) % this->subMatrixSize == 0)
    {
        if (cursor->pagePointer == this->rowsPerBlockCount[cursor->pageIndex] - 1)
        {
            if (cursor->pageIndex < this->blockCount - 1)
            {
                cursor->nextPage(cursor->pageIndex + 1);
            }
        }
        else
        {
            cursor->nextMatrixPage(cursor->pageIndex - this->subMatrixSize + 1, cursor->pagePointer + 1);
        }
    }
    else if (cursor->pageIndex < this->blockCount - 1)
    {
        cursor->nextMatrixPage(cursor->pageIndex + 1, cursor->pagePointer);
    }
}

/* Sorting Functions */

/**
 * @brief Gives idex of columns for a given set of column names
 *
 * @param columnsName vector of column names whose index we want to find
 * @returns column index according to column names
 */
vector<int> Table::getColumnIndex(const vector<string> &columnsName)
{
    logger.log("Table::getColumnIndex <vector>");
    unordered_map<string, int> columnIdx;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        columnIdx[this->columns[columnCounter]] = columnCounter;
    }

    vector<int> idx;
    for (auto columnName : columnsName)
    {
        idx.push_back(columnIdx[columnName]);
    }

    return idx;
}

/**
 * @brief sorts the pages and creates the initial runs
 *
 * @param columnIndex indices of columns according to which we need to sort
 * @param sortingColumnsStrategy sorting startegy of each column (ASC | DESC)
 * @return number of subfiles created
 *
 * @todo write buffer manager functions for subfile
 * @todo write cursor functions for subfiles
 */
int Table::sortingPhase(const vector<int> &columnIndex, const vector<int> &sortingColumnsStrategy)
{
    logger.log("Table::sortingPhase");

    int buffer = BLOCK_COUNT - 1;

    vector<vector<int>> resultantPage(this->maxRowsPerBlock, vector<int>(this->columnCount));
    vector<int> row(this->columnCount);

    // Comparator function for priority queue
    auto comp = [&](vector<int> &a, vector<int> &b)
    {
        for (int i = 0; i < columnIndex.size(); i++)
        {
            if (a[columnIndex[i]] == b[columnIndex[i]])
                continue;

            if (sortingColumnsStrategy[i] == 1)
                return a[columnIndex[i]] > b[columnIndex[i]];
            else
                return a[columnIndex[i]] < b[columnIndex[i]];
        }

        return true;
    };

    int subfileCounter = 0;
    int blockCounter = 0;
    while (blockCounter < this->blockCount)
    {
        int rowCounter = 0;
        Cursor cursor(this->tableName, blockCounter);
        priority_queue<vector<int>, vector<vector<int>>, decltype(comp)> pq(comp);

        // Reading in set of <buffer> blocks and sorting
        for (int i = blockCounter; i < min(blockCounter + buffer, (int)this->blockCount); i++)
        {
            rowCounter = 0;
            while (rowCounter < this->maxRowsPerBlock)
            {
                row = cursor.getNext();
                if (row.empty())
                    break;

                pq.push(row);
                rowCounter++;
            }
        }

        rowCounter = 0;
        int pageIndex = blockCounter;

        // Creating pages of subfiles of the first run
        while (pq.size())
        {
            resultantPage[rowCounter] = pq.top();
            pq.pop();

            rowCounter++;
            if (rowCounter == this->maxRowsPerBlock)
            {
                bufferManager.writePage(this->tableName, 0, subfileCounter, pageIndex, resultantPage, rowCounter);
                rowCounter = 0;
                pageIndex++;
            }
        }

        if (rowCounter)
        {
            bufferManager.writePage(this->tableName, 0, subfileCounter, pageIndex, resultantPage, rowCounter);
            rowCounter = 0;
            pageIndex++;
        }

        subfileCounter++;
        blockCounter = min(blockCounter + buffer, (int)this->blockCount);
    }

    return subfileCounter;
}

/**
 * @brief Merges the sorted subfiles created in the first run
 *
 * @param columnIndex indices of columns according to which we need to sort
 * @param sortingColumnsStrategy sorting startegy of each column (ASC | DESC)
 * @param numSubfiles number of subfiles created in the initial run
 */
void Table::mergingPhase(const vector<int> &columnIndex, const vector<int> &sortingColumnsStrategy, int numSubfiles)
{
    logger.log("Table::mergingPhase");

    int buffer = BLOCK_COUNT - 1;

    vector<vector<int>> resultantPage(this->maxRowsPerBlock, vector<int>(this->columnCount));
    vector<int> row(this->columnCount);

    // Comparator function for priority queue
    auto comp = [&](pair<vector<int>, Cursor> &a, pair<vector<int>, Cursor> &b)
    {
        for (int i = 0; i < columnIndex.size(); i++)
        {
            if (a.first[columnIndex[i]] == b.first[columnIndex[i]])
                continue;

            if (sortingColumnsStrategy[i] == 1)
                return a.first[columnIndex[i]] > b.first[columnIndex[i]];
            else
                return a.first[columnIndex[i]] < b.first[columnIndex[i]];
        }

        return true;
    };

    int currRun = 0;
    int pagesPerSubfile = buffer;
    while (numSubfiles > 1)
    {
        int subfileCounter = 0;
        int newSubfileCounter = 0;

        while (subfileCounter < numSubfiles)
        {
            priority_queue<pair<vector<int>, Cursor>, vector<pair<vector<int>, Cursor>>, decltype(comp)> pq(comp);

            for (int i = subfileCounter; i < min(subfileCounter + buffer, numSubfiles); i++)
            {
                Cursor cursor(this->tableName, currRun, i, i * pagesPerSubfile);

                row = cursor.getSubfileNext();
                pq.push({row, cursor});
            }

            int rowCounter = 0;
            int pageIndex = pagesPerSubfile * subfileCounter;
            while (pq.size())
            {
                row = pq.top().first;
                Cursor cursor = pq.top().second;
                pq.pop();

                // Insert into page matrix
                resultantPage[rowCounter] = row;
                rowCounter++;

                // Get next row of the subfile
                row = cursor.getSubfileNext();
                if (!row.empty())
                    pq.push({row, cursor});
                else
                {
                    if (cursor.pageIndex + 1 != this->blockCount && (cursor.pageIndex + 1) % pagesPerSubfile != 0)
                    {
                        Cursor newCursor(this->tableName, cursor.run, cursor.subfile, cursor.pageIndex + 1);
                        row = newCursor.getSubfileNext();
                        pq.push({row, newCursor});
                    }

                    bufferManager.deleteFile(this->tableName, cursor.run, cursor.subfile, cursor.pageIndex);
                }

                if (rowCounter == this->maxRowsPerBlock)
                {
                    if (ceil((double)numSubfiles / buffer) > 1)
                    {
                        bufferManager.writePage(this->tableName, currRun + 1, newSubfileCounter, pageIndex, resultantPage, rowCounter);
                        rowCounter = 0;
                        pageIndex++;
                    }
                    else
                    {
                        bufferManager.removeFromPool(this->tableName, pageIndex);
                        bufferManager.writePage(this->tableName, pageIndex, resultantPage, rowCounter);
                        rowCounter = 0;
                        pageIndex++;
                    }
                }
            }

            if (rowCounter)
            {
                if (ceil((double)numSubfiles / buffer) > 1)
                {
                    bufferManager.writePage(this->tableName, currRun + 1, newSubfileCounter, pageIndex, resultantPage, rowCounter);
                    rowCounter = 0;
                    pageIndex++;
                }
                else
                {
                    bufferManager.removeFromPool(this->tableName, pageIndex);
                    bufferManager.writePage(this->tableName, pageIndex, resultantPage, rowCounter);
                    rowCounter = 0;
                    pageIndex++;
                }
            }

            subfileCounter = min(subfileCounter + buffer, numSubfiles);
            newSubfileCounter++;
        }

        pagesPerSubfile *= buffer;
        numSubfiles = newSubfileCounter;
        currRun++;
    }
}

/**
 * @brief This function uses external sorting to sort a relation based on
 * the given column names and sorting stratergy.
 *
 * @param sortColumnsName columns based on which the table is to be sorted (priority from left to right)
 * @param sortingColumnsStrategy sorting startegy of each column (ASC | DESC)
 */
void Table::sortTable(const vector<string> &sortColumnsName, const vector<int> &sortingColumnsStrategy)
{
    logger.log("Table::sortTable");
    vector<int> sortColumnsIdx = this->getColumnIndex(sortColumnsName);

    int numSubfiles = this->sortingPhase(sortColumnsIdx, sortingColumnsStrategy);
    this->mergingPhase(sortColumnsIdx, sortingColumnsStrategy, numSubfiles);
}