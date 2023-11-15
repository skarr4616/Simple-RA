#include "global.h"
/**
 * @brief
 * SYNTAX: LOAD relation_name
 */
bool syntacticParseLOAD()
{
    logger.log("syntacticParseLOAD");
    if (tokenizedQuery.size() != 2 && tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    if (tokenizedQuery.size() == 3 && tokenizedQuery[1] != "MATRIX")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = LOAD;

    if (tokenizedQuery.size() == 3)
    {
        parsedQuery.loadDataType = tokenizedQuery[1];
        parsedQuery.loadRelationName = tokenizedQuery[2];
    }
    else
    {
        parsedQuery.loadDataType = "RELATION";
        parsedQuery.loadRelationName = tokenizedQuery[1];
    }

    return true;
}

bool semanticParseLOAD()
{
    logger.log("semanticParseLOAD");
    if (tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Relation already exists" << endl;
        return false;
    }

    if (!isFileExists(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeLOAD()
{
    logger.log("executeLOAD");

    if (parsedQuery.loadDataType == "RELATION")
    {
        Table *table = new Table(parsedQuery.loadRelationName, "RELATION");
        if (table->load())
        {
            tableCatalogue.insertTable(table);
            cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
        }
    }
    else
    {
        Table *table = new Table(parsedQuery.loadRelationName, "MATRIX");
        if (table->load())
        {
            tableCatalogue.insertTable(table);
            cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;
        }
    }

    return;
}