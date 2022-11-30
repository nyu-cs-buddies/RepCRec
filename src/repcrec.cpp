
#include <iostream>
#include <string>

#include "ioUtil.h"
#include "transactionManager.hpp"

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cout << "Usage: ./repcrec <input_file>"
              << std::endl;
    return 1;
  }
  IOUtil ioUtil(argv[argc - 1]);

  TransactionManager tm(ioUtil.operations);

  tm.simulate();
  return 0;
}
