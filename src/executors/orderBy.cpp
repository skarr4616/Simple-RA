#include "global.h"
/**
 * @brief File contains method to process GROUP BY commands.
 *
 * syntax:
 * <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> HAVING
 * <aggregate(attribute)> <bin_op> <attribute_value> RETURN <aggregate_func(attribute)>

 *
 */
bool syntacticParseORDERBY()
{
    logger.log("syntacticParseORDERBY");
    if (tokenizedQuery.size() != 8 || tokenizedQuery[3] != "BY" || tokenizedQuery[6] != "ON")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = ORDERBY;
    parsedQuery.orderByResultRelationName = tokenizedQuery[0];
    parsedQuery.orderByColumnName = tokenizedQuery[4];
    parsedQuery.orderByRelationName = tokenizedQuery[7];

    if (tokenizedQuery[5] == "ASC")
        parsedQuery.orderBySortingStrategy = ASC;
    else if (tokenizedQuery[5] == "DESC")
        parsedQuery.orderBySortingStrategy = DESC;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    return true;
}

bool semanticParseORDERBY()
{
    logger.log("semanticParseORDERBY");

    if (tableCatalogue.isTable(parsedQuery.orderByResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.orderByRelationName))
    {
        cout << "SEMANTIC ERROR: Group By Relation doesn't exists" << endl;
        return false;
    }

    if (!tableCatalogue.getTable(parsedQuery.orderByRelationName)->isColumn(parsedQuery.orderByColumnName))
    {
        cout << "SEMANTIC ERROR: Order by column doesn't exists" << endl;
        return false;
    }

    return true;
}

void executeORDERBY()
{
    // Getting the table
    Table *t1 = tableCatalogue.getTable(parsedQuery.orderByRelationName);

    // Setting up the resultant table
    Table *resultantTable = new Table(parsedQuery.orderByResultRelationName, t1->columns);

    int rowCounter = 0;
    int pageIndex = 0;

    // Setting up cursors
    Cursor cursor1(t1->tableName, 0);

    // Setting up temporary pages
    vector<int> row1(t1->columnCount);
    vector<vector<int>> resultantPage(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount));

    // Copy table pages
    row1 = cursor1.getNext();
    while (!row1.empty())
    {
        copy(row1.begin(), row1.end(), resultantPage[rowCounter++].begin());

        if (rowCounter == resultantTable->maxRowsPerBlock)
        {
            bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
            resultantTable->blockCount++;
            resultantTable->rowsPerBlockCount.push_back(rowCounter);
            rowCounter = 0;
            pageIndex++;
        }
        row1 = cursor1.getNext();
    }

    if (rowCounter)
    {
        bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
        resultantTable->blockCount++;
        resultantTable->rowsPerBlockCount.push_back(rowCounter);
    }

    // Adding table to table catalogue
    tableCatalogue.insertTable(resultantTable);

    // Sorting resultant table
    vector<string> colName({parsedQuery.orderByColumnName});
    if (parsedQuery.orderBySortingStrategy == ASC)
    {
        vector<int> sortingStrategy({1});
        resultantTable->sortTable(colName, sortingStrategy);
    }
    else
    {
        vector<int> sortingStrategy({-1});
        resultantTable->sortTable(colName, sortingStrategy);
    }
}