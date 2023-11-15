#include "global.h"
/**
 * @brief
 * SYNTAX: TRANSPOSE MATRIX <matrix_name>
 */
bool syntacticParseTRANSPOSE()
{
    logger.log("syntacticParseTRANSPOSE");
    if (tokenizedQuery.size() != 3 && tokenizedQuery[1] != "MATRIX")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = TRANSPOSE;

    parsedQuery.transposeMatrixName = tokenizedQuery[2];
    return true;
}

bool semanticParseTRANSPOSE()
{
    logger.log("semanticParseTRANSPOSE");
    if (!tableCatalogue.isTable(parsedQuery.transposeMatrixName))
    {
        cout << "SEMANTIC ERROR: Matrix does not exist" << endl;
        return false;
    }

    if (tableCatalogue.getTable(parsedQuery.transposeMatrixName)->tableType != "MATRIX")
    {
        cout << "SEMANTIC ERROR: Table is not a matrix" << endl;
        return false;
    }

    return true;
}

void executeTRANSPOSE()
{
    logger.log("executeTRANSPOSE");
    Table *table = tableCatalogue.getTable(parsedQuery.transposeMatrixName);
    table->transpose();
    return;
}