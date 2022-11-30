#pragma once

#include <list>

#include "operation.hpp"

class IOUtil {
 public:
  IOUtil() {}
  IOUtil(const char* filename);

  std::list<Operation> operations;
};