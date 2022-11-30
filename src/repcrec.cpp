#include <fstream>
#include <iostream>
#include <list>
#include <string>

#include "operation.hpp"
#include "transactionManager.hpp"
using namespace std;

class IOUtil {
   public:
    IOUtil() {}

    IOUtil(const char* filename) {
        ifstream infile(filename);
        string line;
        int time = 0;

        while (getline(infile, line)) {
            if (!isalpha(line[0])) {
                continue;
            }
            Operation operation;
            ++time;
            switch (line[0]) {
                case 'R': {
                    operation = getReadOperation(line);
                } break;

                case 'W': {
                    operation = getWriteOperation(line);
                } break;

                case 'b': {
                    auto idx = line.find("(");
                    if (idx == 5) {
                        operation = getBeginOperation(line);
                    } else if (idx == 7) {
                        operation = getBeginROOperation(line);
                    } else {
                        cout << "Error: wrong operation." << endl;
                        exit(1);
                    }
                } break;

                case 'e': {
                    operation = getEndOperation(line);
                } break;

                case 'd': {
                    operation = getDumpOperation(line);
                } break;

                case 'r': {
                    operation = getRecoverOperation(line);
                } break;

                case 'f': {
                    operation = getFailOperation(line);
                } break;

                default:
                    cout << "Error: wrong operation." << endl;
                    exit(1);
                    break;
            }
            operation.timeStamp = time;
            operations.push_back(operation);
        }
    }

    list<Operation> operations;
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cout << "Usage: ./repcrec <input_file>" << endl;
        return 1;
    }

    IOUtil ioUtil(argv[argc - 1]);

    TransactionManager tm(ioUtil.operations);
    tm.simulate();
    return 0;
}
