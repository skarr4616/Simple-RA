#include "global.h"

/**
 * @brief
 * SYNTAX: EXPORT <relation_name>
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
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

    parsedQuery.queryType = EXPORT;

    if (tokenizedQuery.size() == 3)
    {
        parsedQuery.exportDataType = tokenizedQuery[1];
        parsedQuery.exportRelationName = tokenizedQuery[2];
    }
    else
    {
        parsedQuery.exportDataType = "RELATION";
        parsedQuery.exportRelationName = tokenizedQuery[1];
    }

    return true;
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    // Table should exist
    if (!tableCatalogue.isTable(parsedQuery.exportRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    // Table type should match
    if (parsedQuery.exportDataType == "MATRIX" && tableCatalogue.getTable(parsedQuery.exportRelationName)->tableType != "MATRIX")
    {
        cout << "SEMANTIC ERROR: Table is not a matrix" << endl;
        return false;
    }

    return true;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    Table *table = tableCatalogue.getTable(parsedQuery.exportRelationName);

    if (table->tableType == "MATRIX")
    {
        table->makeMatrixPermanent();
    }
    else
    {
        table->makePermanent();
    }

    return;
}