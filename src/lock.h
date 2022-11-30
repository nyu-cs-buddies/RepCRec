#pragma once

#include <unordered_set>

class Lock {
 public:
  bool isShared;
};

class ReadLock : public Lock {
 public:
  ReadLock() { isShared = true; };
  std::unordered_set<int> transactionIds;
};

class WriteLock : public Lock {
 public:
  WriteLock() { isShared = false; };
  int transactionId;
};