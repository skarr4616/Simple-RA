#include "global.h"
/**
 * @brief
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
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

    parsedQuery.queryType = PRINT;

    if (tokenizedQuery.size() == 3)
    {
        parsedQuery.printDataType = tokenizedQuery[1];
        parsedQuery.printRelationName = tokenizedQuery[2];
    }
    else
    {
        parsedQuery.printDataType = "RELATION";
        parsedQuery.printRelationName = tokenizedQuery[1];
    }

    return true;
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (parsedQuery.printDataType == "MATRIX" && tableCatalogue.getTable(parsedQuery.printRelationName)->tableType != "MATRIX")
    {
        cout << "SEMANTIC ERROR: Table is not a matrix" << endl;
        return false;
    }

    return true;
}

void executePRINT()
{
    logger.log("executePRINT");
    Table *table = tableCatalogue.getTable(parsedQuery.printRelationName);

    if (table->tableType == "MATRIX")
    {
        table->printMatrix();
    }
    else
    {
        table->print();
    }

    return;
}
