#include "global.h"
/**
 * @brief
 * SYNTAX: RENAME column_name TO column_name FROM relation_name
 */
bool syntacticParseRENAME()
{
    logger.log("syntacticParseRENAME");

    if (tokenizedQuery.size() != 6 && tokenizedQuery.size() != 4)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    if (tokenizedQuery.size() == 6 && (tokenizedQuery[2] != "TO" || tokenizedQuery[4] != "FROM"))
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    if (tokenizedQuery.size() == 4 && tokenizedQuery[1] != "MATRIX")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = RENAME;

    if (tokenizedQuery.size() == 4)
    {
        parsedQuery.renameDataType = tokenizedQuery[1];
        parsedQuery.renameFromMatrixName = tokenizedQuery[2];
        parsedQuery.renameToMatrixName = tokenizedQuery[3];
    }
    else
    {
        parsedQuery.renameDataType = "RELATION";
        parsedQuery.renameFromColumnName = tokenizedQuery[1];
        parsedQuery.renameToColumnName = tokenizedQuery[3];
        parsedQuery.renameRelationName = tokenizedQuery[5];
    }

    return true;
}

bool semanticParseRENAME()
{
    logger.log("semanticParseRENAME");

    if (parsedQuery.renameDataType != "MATRIX")
    {
        if (!tableCatalogue.isTable(parsedQuery.renameRelationName))
        {
            cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
            return false;
        }

        if (!tableCatalogue.isColumnFromTable(parsedQuery.renameFromColumnName, parsedQuery.renameRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }

        if (tableCatalogue.isColumnFromTable(parsedQuery.renameToColumnName, parsedQuery.renameRelationName))
        {
            cout << "SEMANTIC ERROR: Column with name already exists" << endl;
            return false;
        }
    }
    else
    {
        if (!tableCatalogue.isTable(parsedQuery.renameFromMatrixName))
        {
            cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
            return false;
        }

        if (tableCatalogue.getTable(parsedQuery.renameFromMatrixName)->tableType != "MATRIX")
        {
            cout << "SEMANTIC ERROR: Table data type does not match" << endl;
            return false;
        }

        if (tableCatalogue.isTable(parsedQuery.renameToMatrixName))
        {
            cout << "SEMANTIC ERROR: Table already exists" << endl;
            return false;
        }
    }

    return true;
}

void executeRENAME()
{
    logger.log("executeRENAME");

    if (parsedQuery.renameDataType == "MATRIX")
    {
        Table *table = tableCatalogue.getTable(parsedQuery.renameFromMatrixName);
        table->renameMatrix(parsedQuery.renameToMatrixName);
    }
    else
    {
        Table *table = tableCatalogue.getTable(parsedQuery.renameRelationName);
        table->renameColumn(parsedQuery.renameFromColumnName, parsedQuery.renameToColumnName);
    }
    return;
}