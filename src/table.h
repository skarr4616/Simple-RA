#include "cursor.h"

enum IndexingStrategy
{
    BTREE,
    HASH,
    NOTHING
};

/**
 * @brief The Table class holds all information related to a loaded table. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a table object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT).
 *
 */
class Table
{
    vector<unordered_set<int>> distinctValuesInColumns;

public:
    string sourceFileName = "";
    string tableName = "";
    string tableType = "";

    // Number of submatrices in each direction after dividing into submatrices
    unsigned int subMatrixSize = 0;

    vector<string> columns;
    vector<unsigned int> distinctValuesPerColumnCount;
    unsigned int columnCount = 0;
    long long int rowCount = 0;
    unsigned int blockCount = 0;
    unsigned int maxRowsPerBlock = 0;

    vector<unsigned int> rowsPerBlockCount;
    vector<unsigned int> columnsPerBlockCount;

    bool indexed = false;
    string indexedColumn = "";
    IndexingStrategy indexingStrategy = NOTHING;

    bool extractColumnNames(string firstLine);
    bool blockify();
    void updateStatistics(vector<int> row);
    Table();
    Table(string tableName, string tableType = "RELATION");
    Table(string tableName, vector<string> columns);

    bool load();
    bool isColumn(string columnName);
    void renameColumn(string fromColumnName, string toColumnName);

    void print();
    void makePermanent();
    bool isPermanent();
    void getNextPage(Cursor *cursor);
    Cursor getCursor();
    int getColumnIndex(string columnName);
    vector<int> getColumnIndex(const vector<string> &columnsName);
    void unload();

    /* Matrix Functions */
    bool blockifyMatrix();
    void printMatrix();
    void makeMatrixPermanent();
    void getNextPointer(Cursor *cursor);
    void renameMatrix(string toMatrixName);
    bool checkSymmetry();
    bool checkMatrixSymmetry(vector<vector<int>> &mat);
    bool checkMatrixSymmetry(vector<vector<int>> &mat1, vector<vector<int>> &mat2);
    void transpose();

    /* Sorting Functions */
    int sortingPhase(const vector<int> &columnIndex, const vector<int> &sortingColumnsStrategy);
    void mergingPhase(const vector<int> &columnIndex, const vector<int> &sortingColumnsStrategy, int numSubfiles);
    void sortTable(const vector<string> &sortColumnsName, const vector<int> &sortingColumnsStrategy);

    /**
     * @brief Static function that takes a vector of valued and prints them out in a
     * comma seperated format.
     *
     * @tparam T current usaages include int and string
     * @param row
     */
    template <typename T>
    void writeRow(vector<T> row, ostream &fout)
    {
        logger.log("Table::printRow");
        for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
        {
            if (columnCounter != 0)
                fout << ", ";
            fout << row[columnCounter];
        }
        fout << endl;
    }

    /**
     * @brief Static function that takes a vector of valued and prints them out in a
     * comma seperated format.
     *
     * @tparam T current usaages include int and string
     * @param row
     */
    template <typename T>
    void writeRow(vector<T> row)
    {
        logger.log("Table::printRow");
        ofstream fout(this->sourceFileName, ios::app);
        this->writeRow(row, fout);
        fout.close();
    }

    /**
     * @brief Static function that takes a segment of the matrix row and prints it out in a
     * comma seperated format.
     *
     * @tparam T current usaages includes int
     * @param row
     * @param startSegment indicates whether the segement is the start of the row or not
     * @param endSegment indicates whether the segment is the end of the row or not
     */
    template <typename T>
    void writeMatrixRowSegment(vector<T> row, ostream &fout, bool startSegment, bool endSegment)
    {
        logger.log("Table::printRow");
        for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
        {
            if (startSegment == false || columnCounter != 0)
                fout << ", ";
            fout << row[columnCounter];
        }

        if (endSegment)
        {
            fout << endl;
        }
    }
};