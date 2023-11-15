#include "global.h"
/**
 * @brief
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[5] != "ON")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];
    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[8];

    string binaryOperator = tokenizedQuery[7];
    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

/**
 * @brief Parse the join predicate
 */
bool joinBinaryOperatorParser(int val1, int val2, BinaryOperator binaryOperator)
{
    logger.log("joinBinaryOperatorParser");

    switch (binaryOperator)
    {
    case EQUAL:
        return val1 == val2;
    case GREATER_THAN:
        return val1 > val2;
    case GEQ:
        return val1 >= val2;
    case LESS_THAN:
        return val1 < val2;
    case LEQ:
        return val1 <= val2;
    case NOT_EQUAL:
        return val1 != val2;
    default:
        return false;
    }
}

/**
 * @brief Execute the Sorted Merge Join algorithm
 */
void executeSortedMergeJoin()
{
    // Getting the tables
    Table *t1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *t2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    // Setting up result table
    vector<string> resultantColumns;
    for (auto column : t1->columns)
        resultantColumns.push_back(column);

    for (auto column : t2->columns)
        resultantColumns.push_back(column);

    Table *resultantTable = new Table(parsedQuery.joinResultRelationName, resultantColumns);

    // Getting column indices
    int joinFirstColumnIndex = t1->getColumnIndex(parsedQuery.joinFirstColumnName);
    int joinSecondColumnIndex = t2->getColumnIndex(parsedQuery.joinSecondColumnName);

    // Sorting the tables on JOIN attribute
    vector<int> sortingStrategy({1});
    vector<string> sortingColumns1({parsedQuery.joinFirstColumnName});
    vector<string> sortingColumns2({parsedQuery.joinSecondColumnName});

    t1->sortTable(sortingColumns1, sortingStrategy);
    t2->sortTable(sortingColumns2, sortingStrategy);

    // Setting up temporary pages
    vector<vector<int>> resultantPage(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount));
    vector<int> row1(t1->columnCount);
    vector<int> row2(t2->columnCount);
    vector<int> row3(t2->columnCount);

    // Setting up cursors
    Cursor cursor1(t1->tableName, 0);
    Cursor cursor2(t2->tableName, 0);
    Cursor cursor3(t2->tableName, 0);

    // Performing the join
    int rowCounter = 0;
    int pageIndex = 0;

    row1 = cursor1.getNext();
    row2 = cursor2.getNext();

    while (!row1.empty() && !row2.empty())
    {
        while (row1[joinFirstColumnIndex] < row2[joinSecondColumnIndex])
        {
            row1 = cursor1.getNext();
            if (row1.empty())
                break;
        }

        if (row1.empty())
            break;

        while (row1[joinFirstColumnIndex] > row2[joinSecondColumnIndex])
        {
            row2 = cursor2.getNext();
            if (row2.empty())
                break;
        }

        if (row2.empty())
            break;

        while (row1[joinFirstColumnIndex] == row2[joinSecondColumnIndex])
        {
            row3 = row2;
            cursor3 = cursor2;
            while (row1[joinFirstColumnIndex] == row3[joinSecondColumnIndex])
            {
                copy(row1.begin(), row1.end(), resultantPage[rowCounter].begin());
                copy(row3.begin(), row3.end(), resultantPage[rowCounter].begin() + row1.size());
                rowCounter++;

                if (rowCounter == resultantTable->maxRowsPerBlock)
                {
                    bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
                    resultantTable->blockCount++;
                    resultantTable->rowsPerBlockCount.push_back(rowCounter);
                    rowCounter = 0;
                    pageIndex++;
                }

                row3 = cursor3.getNext();
                if (row3.empty())
                    break;
            }

            row1 = cursor1.getNext();
            if (row1.empty())
                break;
        }

        row2 = row3;
        cursor2 = cursor3;
    }

    if (rowCounter)
    {
        bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
        resultantTable->blockCount++;
        resultantTable->rowsPerBlockCount.push_back(rowCounter);
    }

    // Adding table to table catalogue
    tableCatalogue.insertTable(resultantTable);
}

/**
 * @brief Execute the Block Nested Loop Join algorithm
 */
void executeBlockNestedLoopJoin()
{
    // Getting the tables
    Table *t1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *t2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

    // Setting up outer and inner loops
    Table *outer = t1;
    Table *inner = t2;

    bool flag = false;
    if (outer->blockCount > inner->blockCount)
    {
        swap(outer, inner);
        flag = true;
    }

    // Setting up result table
    vector<string> resultantColumns;
    for (auto column : t1->columns)
        resultantColumns.push_back(column);

    for (auto column : t2->columns)
        resultantColumns.push_back(column);

    Table *resultantTable = new Table(parsedQuery.joinResultRelationName, resultantColumns);

    // Getting column indices
    int joinFirstColumnIndex = t1->getColumnIndex(parsedQuery.joinFirstColumnName);
    int joinSecondColumnIndex = t2->getColumnIndex(parsedQuery.joinSecondColumnName);

    // Setting up temporary pages
    vector<vector<int>> resultantPage(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount));
    vector<int> row1(t1->columnCount);
    vector<int> row2(t2->columnCount);

    // Setting up cursors
    Cursor cursor1(outer->tableName, 0);

    // Performing the join
    int rowCounter = 0, pageIndex = 0;

    row1 = cursor1.getNext();
    while (!row1.empty())
    {
        Cursor cursor2(inner->tableName, 0);
        row2 = cursor2.getNext();
        while (!row2.empty())
        {
            if (!flag && joinBinaryOperatorParser(row1[joinFirstColumnIndex], row2[joinSecondColumnIndex], parsedQuery.joinBinaryOperator))
            {

                copy(row1.begin(), row1.end(), resultantPage[rowCounter].begin());
                copy(row2.begin(), row2.end(), resultantPage[rowCounter].begin() + row1.size());
                rowCounter++;
            }
            else if (flag && joinBinaryOperatorParser(row2[joinFirstColumnIndex], row1[joinSecondColumnIndex], parsedQuery.joinBinaryOperator))
            {
                copy(row2.begin(), row2.end(), resultantPage[rowCounter].begin());
                copy(row1.begin(), row1.end(), resultantPage[rowCounter].begin() + row2.size());
                rowCounter++;
            }

            if (rowCounter == resultantTable->maxRowsPerBlock)
            {
                bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
                resultantTable->blockCount++;
                resultantTable->rowsPerBlockCount.push_back(rowCounter);
                rowCounter = 0;
                pageIndex++;
            }
            row2 = cursor2.getNext();
        }
        row1 = cursor1.getNext();
    }

    if (rowCounter)
    {
        bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
        resultantTable->blockCount++;
        resultantTable->rowsPerBlockCount.push_back(rowCounter);
    }

    // Adding to table catalogue
    tableCatalogue.insertTable(resultantTable);
}

/**
 * @brief Driver function for join algorithm to decide what type of join to perform
 */
void executeJOIN()
{
    logger.log("executeJOIN");

    if (parsedQuery.joinBinaryOperator == EQUAL)
        executeSortedMergeJoin();
    else
        executeBlockNestedLoopJoin();
}