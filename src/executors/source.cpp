#include "global.h"
/**
 * @brief
 * SYNTAX: SOURCE filename
 */
bool syntacticParseSOURCE()
{
    logger.log("syntacticParseSOURCE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SOURCE;
    parsedQuery.sourceFileName = tokenizedQuery[1];
    return true;
}

bool semanticParseSOURCE()
{
    logger.log("semanticParseSOURCE");
    if (!isQueryFile(parsedQuery.sourceFileName))
    {
        cout << "SEMANTIC ERROR: File doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeSOURCE()
{
    logger.log("executeSOURCE");
    string fileName = "../data/" + parsedQuery.sourceFileName + ".ra";

    ifstream file(fileName);
    string command;

    regex delim("[^\\s,]+");

    while (getline(file, command))
    {
        tokenizedQuery.clear();
        parsedQuery.clear();

        blocksRead = 0;
        blocksWritten = 0;

        logger.log("\nReading New Command from Source File: ");
        logger.log(command);

        auto words_begin = std::sregex_iterator(command.begin(), command.end(), delim);
        auto words_end = std::sregex_iterator();
        for (std::sregex_iterator i = words_begin; i != words_end; ++i)
            tokenizedQuery.emplace_back((*i).str());

        if (tokenizedQuery.size() == 1 && tokenizedQuery.front() == "QUIT")
        {
            exit(0);
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

        logger.log("doSourceCommand");
        if (syntacticParse() && semanticParse())
            executeCommand();

        cout << "Number of blocks read: " << blocksRead << endl;
        cout << "Number of blocks written: " << blocksWritten << endl;
        cout << "Number of blocks accessed: " << blocksRead + blocksWritten << endl;
    }

    return;
}
