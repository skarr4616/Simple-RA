// Server Code
#include "global.h"

using namespace std;

float BLOCK_SIZE = 1;
unsigned int BLOCK_COUNT = 10;
unsigned int PRINT_COUNT = 20;
Logger logger;
vector<string> tokenizedQuery;
ParsedQuery parsedQuery;
TableCatalogue tableCatalogue;
BufferManager bufferManager;
unsigned int blocksRead;
unsigned int blocksWritten;

void doCommand()
{
    logger.log("doCommand");
    if (syntacticParse() && semanticParse())
        executeCommand();
    return;
}

int main(void)
{

    regex delim("[^\\s,]+");
    string command;
    system("rm -rf ../data/temp");
    system("mkdir ../data/temp");

    while (!cin.eof())
    {
        cout << "\n> ";
        tokenizedQuery.clear();
        parsedQuery.clear();
        blocksRead = 0;
        blocksWritten = 0;
        logger.log("\nReading New Command: ");
        getline(cin, command);
        logger.log(command);

        auto words_begin = std::sregex_iterator(command.begin(), command.end(), delim);
        auto words_end = std::sregex_iterator();
        for (std::sregex_iterator i = words_begin; i != words_end; ++i)
            tokenizedQuery.emplace_back((*i).str());

        if (tokenizedQuery.size() == 1 && tokenizedQuery.front() == "QUIT")
        {
            break;
        }

        if (tokenizedQuery.empty())
        {
            continue;
        }

        if (tokenizedQuery.size() == 1)
        {
            cout << "SYNTAX ERROR" << endl;
            continue;
        }

        doCommand();

        cout << "Number of blocks read: " << blocksRead << endl;
        cout << "Number of blocks written: " << blocksWritten << endl;
        cout << "Number of blocks accessed: " << blocksRead + blocksWritten << endl;
    }
}