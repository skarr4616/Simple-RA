#include "executor.h"

extern float BLOCK_SIZE;
extern unsigned int BLOCK_COUNT;
extern unsigned int PRINT_COUNT;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern TableCatalogue tableCatalogue;
extern BufferManager bufferManager;

extern unsigned int blocksRead;
extern unsigned int blocksWritten;