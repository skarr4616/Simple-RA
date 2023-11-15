#include "bufferManager.h"
/**
 * @brief The cursor is an important component of the system. To read from a
 * table, you need to initialize a cursor. The cursor reads rows from a page one
 * at a time.
 *
 */
class Cursor
{
public:
    Page page;
    int pageIndex;
    int run;
    int subfile;
    string tableName;
    int pagePointer;

public:
    Cursor(string tableName, int pageIndex);
    vector<int> getNext();
    void nextPage(int pageIndex);

    // Cursor functions for matrices
    vector<int> getMatrixNext();
    void nextMatrixPage(int pageIndex, int pagePointer = 0);

    // Cursor functions for subfile
    Cursor(string tableName, int run, int subfile, int pageIndex);
    vector<int> getSubfileNext();
};