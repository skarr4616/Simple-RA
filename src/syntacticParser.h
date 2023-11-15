#include "tableCatalogue.h"

using namespace std;

enum QueryType
{
    CLEAR,
    CROSS,
    DISTINCT,
    EXPORT,
    INDEX,
    JOIN,
    LIST,
    LOAD,
    PRINT,
    PROJECTION,
    RENAME,
    SELECTION,
    SORT,
    SOURCE,
    CHECKSYMMETRY,
    COMPUTE,
    TRANSPOSE,
    GROUPBY,
    ORDERBY,
    UNDETERMINED
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;

    string clearRelationName = "";

    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string exportDataType = "";
    string exportRelationName = "";

    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";

    // Join
    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";

    string loadDataType = "";
    string loadRelationName = "";

    string printDataType = "";
    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string>
        projectionColumnList;
    string projectionRelationName = "";

    string renameDataType = "";
    string renameFromMatrixName = "";
    string renameToMatrixName = "";
    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;

    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    string sortRelationName = "";
    vector<string> sortColumnsName;
    vector<SortingStrategy> sortingColumnsStrategy;

    string sourceFileName = "";

    string checkSymmetryMatrixName = "";

    string computeMatrixName = "";

    string transposeMatrixName = "";

    // Group By
    string groupByResultRelationName = "";
    string groupByRelationName = "";
    string groupByColumnName = "";
    string groupByHavingColumnName = "";
    string groupByHavingAggregate = "";
    BinaryOperator groupByBinaryOperator = NO_BINOP_CLAUSE;
    int groupByHavingValue;
    string groupByReturnColumnName = "";
    string groupByReturnAggregate = "";

    // Order By
    string orderByResultRelationName = "";
    string orderByRelationName = "";
    string orderByColumnName = "";
    SortingStrategy orderBySortingStrategy = NO_SORT_CLAUSE;

    ParsedQuery();
    void clear();
};

bool syntacticParse();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSOURCE();
bool syntacticParseCHECKSYMMETRY();
bool syntacticParseCOMPUTE();
bool syntacticParseTRANSPOSE();
bool syntacticParseSORT();
bool syntacticParseGROUPBY();
bool syntacticParseORDERBY();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);
