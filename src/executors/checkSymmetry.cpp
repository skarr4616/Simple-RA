#include "global.h"
/**
 * @brief
 * SYNTAX: CHECKSYMMETRY matrix_name
 */
bool syntacticParseCHECKSYMMETRY()
{
    logger.log("syntacticParseCHECKSYMMETRY");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = CHECKSYMMETRY;
    parsedQuery.checkSymmetryMatrixName = tokenizedQuery[1];

    return true;
}

bool semanticParseCHECKSYMMETRY()
{
    logger.log("semanticParseCHECKSYMMETRY");
    if (!tableCatalogue.isTable(parsedQuery.checkSymmetryMatrixName))
    {
        cout << "SEMANTIC ERROR: Matrix does not exists" << endl;
        return false;
    }

    if (tableCatalogue.getTable(parsedQuery.checkSymmetryMatrixName)->tableType != "MATRIX")
    {
        cout << "SEMANTIC ERROR: Table is not a matrix" << endl;
        return false;
    }

    return true;
}

void executeCHECKSYMMETRY()
{
    logger.log("executeCHECKSYMMETRY");
    Table *table = tableCatalogue.getTable(parsedQuery.checkSymmetryMatrixName);

    if (table->checkSymmetry())
    {
        cout << "TRUE" << endl;
    }
    else
    {
        cout << "FALSE" << endl;
    }

    return;
}