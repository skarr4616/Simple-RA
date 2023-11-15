#include "global.h"
/**
 * @brief File contains method to process SORT commands.
 *
 * syntax:
 * SORT <table_name> BY <column_name1, column_name2,..., column_namek> IN <ASC|DESC, ASC|DESC,..., ASC|DESC>
 *
 */
bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");
    if (tokenizedQuery.size() < 6 || tokenizedQuery[2] != "BY")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = SORT;
    parsedQuery.sortRelationName = tokenizedQuery[1];

    int i = 3;
    for (; i < tokenizedQuery.size(); i++)
    {
        if (tokenizedQuery[i] == "IN")
        {
            i++;
            break;
        }
        parsedQuery.sortColumnsName.push_back(tokenizedQuery[i]);
    }

    if (i == tokenizedQuery.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    for (; i < tokenizedQuery.size(); i++)
    {
        if (tokenizedQuery[i] == "ASC")
            parsedQuery.sortingColumnsStrategy.push_back(ASC);
        else if (tokenizedQuery[i] == "DESC")
            parsedQuery.sortingColumnsStrategy.push_back(DESC);
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    if (parsedQuery.sortColumnsName.size() != parsedQuery.sortingColumnsStrategy.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    for (auto columnName : parsedQuery.sortColumnsName)
    {
        if (!tableCatalogue.isColumnFromTable(columnName, parsedQuery.sortRelationName))
        {
            cout << "SEMANTIC ERROR: Column " << columnName << " doesn't exist in relation" << endl;
            return false;
        }
    }

    return true;
}

void executeSORT()
{
    logger.log("executeSORT");

    vector<int> sortingColumnsStrategy;
    for (auto it : parsedQuery.sortingColumnsStrategy)
    {
        if (it == ASC)
            sortingColumnsStrategy.push_back(1);
        else
            sortingColumnsStrategy.push_back(-1);
    }

    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    table->sortTable(parsedQuery.sortColumnsName, sortingColumnsStrategy);
    return;
}