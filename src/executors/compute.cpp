#include "global.h"
/**
 * @brief
 * SYNTAX: COMPUTE matrix_name
 */
bool syntacticParseCOMPUTE()
{
    logger.log("syntacticParseCOMPUTE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = COMPUTE;
    parsedQuery.computeMatrixName = tokenizedQuery[1];

    return true;
}

bool semanticParseCOMPUTE()
{
    logger.log("semanticParseCOMPUTE");
    if (!tableCatalogue.isTable(parsedQuery.computeMatrixName))
    {
        cout << "SEMANTIC ERROR: Matrix does not exists" << endl;
        return false;
    }

    if (tableCatalogue.getTable(parsedQuery.computeMatrixName)->tableType != "MATRIX")
    {
        cout << "SEMANTIC ERROR: Table is not a matrix" << endl;
        return false;
    }

    string computeResult = parsedQuery.computeMatrixName + "_RESULT";
    if (tableCatalogue.isTable(computeResult))
    {
        cout << "SEMANTIC ERROR: Resultant Matrix already exists" << endl;
        return false;
    }

    return true;
}

void getProperties(Table *src, Table *des)
{
    logger.log("getProperties");

    des->rowCount = src->rowCount;
    des->columnCount = src->columnCount;
    des->blockCount = src->blockCount;
    des->subMatrixSize = src->subMatrixSize;
    des->maxRowsPerBlock = src->maxRowsPerBlock;
    des->rowsPerBlockCount = src->rowsPerBlockCount;
    des->columnsPerBlockCount = src->columnsPerBlockCount;
}

/**
 * @brief Computes A-A' for a submatrix A inPlace
 *
 * @param mat
 */
void computeMatrix(vector<vector<int>> &mat)
{
    logger.log("computeMatrix");

    int n = mat.size();
    for (int r = 0; r < n; r++)
    {
        for (int c = r; c < n; c++)
        {
            mat[r][c] -= mat[c][r];
            mat[c][r] = -1 * mat[r][c];
        }
    }
}

/**
 * @brief overloaded function that computes A-B' and B-A' for a submatrix A and B inPlace
 *
 * @param mat1
 * @param mat2
 */
void computeMatrix(vector<vector<int>> &mat1, vector<vector<int>> &mat2)
{
    logger.log("computeMatrix");

    int m = mat1.size();
    int n = mat1[0].size();

    for (int r = 0; r < m; r++)
    {
        for (int c = 0; c < n; c++)
        {
            mat1[r][c] -= mat2[c][r];
            mat2[c][r] = -1 * mat1[r][c];
        }
    }
}

void executeCOMPUTE()
{
    logger.log("executeCOMPUTE");
    Table *table = tableCatalogue.getTable(parsedQuery.computeMatrixName);

    string computeResult = parsedQuery.computeMatrixName + "_RESULT";
    Table *resultantMatrix = new Table(computeResult, "MATRIX");

    getProperties(table, resultantMatrix);

    Cursor cursor(table->tableName, 0);

    for (int subMatrixR = 0; subMatrixR < table->subMatrixSize; subMatrixR++)
    {
        for (int subMatrixC = subMatrixR; subMatrixC < table->subMatrixSize; subMatrixC++)
        {
            if (subMatrixC == subMatrixR)
            {
                int pageIndex = subMatrixR * table->subMatrixSize + subMatrixC;
                cursor.nextPage(pageIndex);

                vector<int> row(table->columnsPerBlockCount[pageIndex]);
                vector<vector<int>> mat(table->rowsPerBlockCount[pageIndex], row);

                for (int i = 0; i < table->rowsPerBlockCount[pageIndex]; i++)
                {
                    row = cursor.getNext();
                    mat[i] = row;
                }

                computeMatrix(mat);
                bufferManager.writePage(resultantMatrix->tableName, pageIndex, mat, table->rowsPerBlockCount[pageIndex]);
            }
            else
            {
                int pageIndex1 = subMatrixR * table->subMatrixSize + subMatrixC;
                cursor.nextPage(pageIndex1);

                vector<int> row(table->columnsPerBlockCount[pageIndex1]);
                vector<vector<int>> mat1(table->rowsPerBlockCount[pageIndex1], row);

                for (int i = 0; i < table->rowsPerBlockCount[pageIndex1]; i++)
                {
                    row = cursor.getNext();
                    mat1[i] = row;
                }

                int pageIndex2 = subMatrixC * table->subMatrixSize + subMatrixR;
                cursor.nextPage(pageIndex2);

                row.resize(table->columnsPerBlockCount[pageIndex2]);
                vector<vector<int>> mat2(table->rowsPerBlockCount[pageIndex2], row);

                for (int i = 0; i < table->rowsPerBlockCount[pageIndex2]; i++)
                {
                    row = cursor.getNext();
                    mat2[i] = row;
                }

                computeMatrix(mat1, mat2);

                bufferManager.writePage(resultantMatrix->tableName, pageIndex1, mat1, table->rowsPerBlockCount[pageIndex1]);
                bufferManager.writePage(resultantMatrix->tableName, pageIndex2, mat2, table->rowsPerBlockCount[pageIndex2]);
            }
        }
    }

    tableCatalogue.insertTable(resultantMatrix);
    cout << "Loaded Table. Column Count: " << resultantMatrix->columnCount << " Row Count: " << resultantMatrix->rowCount << endl;
    return;
}