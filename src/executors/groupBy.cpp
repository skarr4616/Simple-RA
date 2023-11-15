#include "global.h"
/**
 * @brief File contains method to process GROUP BY commands.
 *
 * syntax:
 * <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> HAVING
 * <aggregate(attribute)> <bin_op> <attribute_value> RETURN <aggregate_func(attribute)>

 *
 */
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");
    if (tokenizedQuery.size() != 13 || tokenizedQuery[3] != "BY" || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "HAVING" || tokenizedQuery[11] != "RETURN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupByResultRelationName = tokenizedQuery[0];
    parsedQuery.groupByColumnName = tokenizedQuery[4];
    parsedQuery.groupByRelationName = tokenizedQuery[6];

    // Parsing Having Clause
    bool isHavingClause = false;
    for (int i = 0; i < tokenizedQuery[8].length(); i++)
    {
        if (tokenizedQuery[8][i] != '(')
        {
            parsedQuery.groupByHavingAggregate += tokenizedQuery[8][i];
            continue;
        }

        i++;
        isHavingClause = true;
        while (i < tokenizedQuery[8].length() && tokenizedQuery[8][i] != ')')
        {
            parsedQuery.groupByHavingColumnName += tokenizedQuery[8][i];
            i++;
        }

        if (i != tokenizedQuery[8].length() - 1)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    if (!isHavingClause)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Parsing Binary Operator
    if (tokenizedQuery[9] == "==")
    {
        parsedQuery.groupByBinaryOperator = EQUAL;
    }
    else if (tokenizedQuery[9] == ">")
    {
        parsedQuery.groupByBinaryOperator = GREATER_THAN;
    }
    else if (tokenizedQuery[9] == ">=")
    {
        parsedQuery.groupByBinaryOperator = GEQ;
    }
    else if (tokenizedQuery[9] == "<")
    {
        parsedQuery.groupByBinaryOperator = LESS_THAN;
    }
    else if (tokenizedQuery[9] == "<=")
    {
        parsedQuery.groupByBinaryOperator = LEQ;
    }
    else if (tokenizedQuery[9] == "!=")
    {
        parsedQuery.groupByBinaryOperator = NOT_EQUAL;
    }
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Parsing aggregate value
    for (int i = 0; i < tokenizedQuery[10].length(); i++)
    {
        if ('0' > tokenizedQuery[10][i] || tokenizedQuery[10][i] > '9')
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    parsedQuery.groupByHavingValue = stoi(tokenizedQuery[10]);

    // Parsing Return Clause
    bool isReturnClause = false;
    for (int i = 0; i < tokenizedQuery[12].length(); i++)
    {
        if (tokenizedQuery[12][i] != '(')
        {
            parsedQuery.groupByReturnAggregate += tokenizedQuery[12][i];
            continue;
        }

        i++;
        isReturnClause = true;
        while (i < tokenizedQuery[12].length() && tokenizedQuery[12][i] != ')')
        {
            parsedQuery.groupByReturnColumnName += tokenizedQuery[12][i];
            i++;
        }

        if (i != tokenizedQuery[12].length() - 1)
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    if (!isReturnClause)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupByResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.groupByRelationName))
    {
        cout << "SEMANTIC ERROR: Group By Relation doesn't exists" << endl;
        return false;
    }

    if (!tableCatalogue.getTable(parsedQuery.groupByRelationName)->isColumn(parsedQuery.groupByColumnName) || !tableCatalogue.getTable(parsedQuery.groupByRelationName)->isColumn(parsedQuery.groupByHavingColumnName) || !tableCatalogue.getTable(parsedQuery.groupByRelationName)->isColumn(parsedQuery.groupByReturnColumnName))
    {
        cout << "SEMANTIC ERROR: One of the given columns doesn't exists" << endl;
        return false;
    }

    unordered_set<string> aggregateFunctions({"MAX", "MIN", "SUM", "COUNT", "AVG"});

    if (aggregateFunctions.find(parsedQuery.groupByHavingAggregate) == aggregateFunctions.end() || aggregateFunctions.find(parsedQuery.groupByReturnAggregate) == aggregateFunctions.end())
    {
        cout << "SEMANTIC ERROR: One of the given aggregate functions doesn't exists" << endl;
        return false;
    }

    return true;
}

/**
 * @brief checks if the given values satisfies the given binary operator
 */
bool groupByBinaryOperatorParser(int val1, int val2, BinaryOperator binaryOperator)
{
    logger.log("groupByBinaryOperatorParser");

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
 * @brief Executes the GROUP BY command
 */
void executeGROUPBY()
{
    logger.log("executeGROUPBY");

    // Getting old table
    Table *table = tableCatalogue.getTable(parsedQuery.groupByRelationName);

    // Creating resultant table
    vector<string> resultantColumns({parsedQuery.groupByColumnName, parsedQuery.groupByReturnColumnName});
    Table *resultantTable = new Table(parsedQuery.groupByResultRelationName, resultantColumns);
    resultantTable->rowCount = 0;
    resultantTable->blockCount = 0;

    // Sorting table based on grouping column
    vector<string> sortingColumn({parsedQuery.groupByColumnName});
    vector<int> sortingOrder({1});
    table->sortTable(sortingColumn, sortingOrder);

    // Setting up temporary matrics
    vector<vector<int>> resultantPage(resultantTable->maxRowsPerBlock, vector<int>(resultantTable->columnCount));
    vector<int> row(table->maxRowsPerBlock);

    // Setting up cursors
    Cursor cursor(table->tableName, 0);

    // Setting up column indices
    int groupByColumnIndex = table->getColumnIndex(parsedQuery.groupByColumnName);
    int havingColumnIndex = table->getColumnIndex(parsedQuery.groupByHavingColumnName);
    int returnColumnIndex = table->getColumnIndex(parsedQuery.groupByReturnColumnName);

    // Setting up aggregate function
    unordered_map<string, int> funcsHaving;
    funcsHaving["MAX"] = INT_MIN;
    funcsHaving["MIN"] = INT_MAX;
    funcsHaving["SUM"] = 0;
    funcsHaving["COUNT"] = 0;
    funcsHaving["AVG"] = 1;

    unordered_map<string, int> funcsReturn;
    funcsReturn["MAX"] = INT_MIN;
    funcsReturn["MIN"] = INT_MAX;
    funcsReturn["SUM"] = 0;
    funcsReturn["COUNT"] = 0;
    funcsReturn["AVG"] = 1;

    // Setting up grouping variables
    int key = 0;
    bool foundKey = false;

    // Setting up tracking variables
    int rowCounter = 0;
    int pageIndex = 0;

    // Grouping and aggregating
    while (true)
    {
        row = cursor.getNext();
        if (row.empty())
            break;

        if (!foundKey)
        {
            key = row[groupByColumnIndex];
            funcsHaving["MAX"] = row[havingColumnIndex];
            funcsHaving["MIN"] = row[havingColumnIndex];
            funcsHaving["SUM"] = row[havingColumnIndex];
            funcsHaving["COUNT"] = 1;
            funcsHaving["AVG"] = row[havingColumnIndex];

            funcsReturn["MAX"] = row[returnColumnIndex];
            funcsReturn["MIN"] = row[returnColumnIndex];
            funcsReturn["SUM"] = row[returnColumnIndex];
            funcsReturn["COUNT"] = 1;
            funcsReturn["AVG"] = row[returnColumnIndex];
            foundKey = true;
        }
        else
        {
            if (row[groupByColumnIndex] != key)
            {
                if (groupByBinaryOperatorParser(funcsHaving[parsedQuery.groupByHavingAggregate], parsedQuery.groupByHavingValue, parsedQuery.groupByBinaryOperator))
                {
                    resultantPage[rowCounter][0] = key;
                    resultantPage[rowCounter][1] = funcsReturn[parsedQuery.groupByReturnAggregate];
                    rowCounter++;

                    if (rowCounter == resultantTable->maxRowsPerBlock)
                    {
                        bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
                        resultantTable->rowCount += rowCounter;
                        resultantTable->rowsPerBlockCount.push_back(rowCounter);
                        resultantTable->blockCount++;
                        rowCounter = 0;
                        pageIndex++;
                    }
                }

                key = row[groupByColumnIndex];
                funcsHaving["MAX"] = row[havingColumnIndex];
                funcsHaving["MIN"] = row[havingColumnIndex];
                funcsHaving["SUM"] = row[havingColumnIndex];
                funcsHaving["COUNT"] = 1;
                funcsHaving["AVG"] = row[havingColumnIndex];

                funcsReturn["MAX"] = row[returnColumnIndex];
                funcsReturn["MIN"] = row[returnColumnIndex];
                funcsReturn["SUM"] = row[returnColumnIndex];
                funcsReturn["COUNT"] = 1;
                funcsReturn["AVG"] = row[returnColumnIndex];
            }
            else
            {
                funcsHaving["MAX"] = max(funcsHaving["MAX"], row[havingColumnIndex]);
                funcsHaving["MIN"] = min(funcsHaving["MIN"], row[havingColumnIndex]);
                funcsHaving["SUM"] += row[havingColumnIndex];
                funcsHaving["COUNT"]++;
                funcsHaving["AVG"] = funcsHaving["SUM"] / funcsHaving["COUNT"];

                funcsReturn["MAX"] = max(funcsReturn["MAX"], row[returnColumnIndex]);
                funcsReturn["MIN"] = min(funcsReturn["MIN"], row[returnColumnIndex]);
                funcsReturn["SUM"] += row[returnColumnIndex];
                funcsReturn["COUNT"]++;
                funcsReturn["AVG"] = funcsReturn["SUM"] / funcsReturn["COUNT"];
            }
        }
    }

    if (foundKey)
    {
        if (groupByBinaryOperatorParser(funcsHaving[parsedQuery.groupByHavingAggregate], parsedQuery.groupByHavingValue, parsedQuery.groupByBinaryOperator))
        {
            resultantPage[rowCounter][0] = key;
            resultantPage[rowCounter][1] = funcsReturn[parsedQuery.groupByReturnAggregate];
            rowCounter++;
        }

        bufferManager.writePage(resultantTable->tableName, pageIndex, resultantPage, rowCounter);
        resultantTable->rowCount += rowCounter;
        resultantTable->rowsPerBlockCount.push_back(rowCounter);
        resultantTable->blockCount++;
        rowCounter = 0;
        pageIndex++;
    }

    // Adding resultant table to table catalogue
    tableCatalogue.insertTable(resultantTable);
}