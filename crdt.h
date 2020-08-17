// Copyright (C) 2020 Felipe O. Carvalho
#pragma once

#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "lib.h"

class GCounter {
 public:
  using ValueType = int;

  class Payload {
   public:
    unsigned int localValueForReplica(const std::string &replica_name) const {
      if (auto *value = lookup(_payload, replica_name)) {
        return *value;
      }
      return 0;
    }

    unsigned int query() const {
      unsigned int sum = 0;
      for (auto & [ _, v ] : _payload) {
        sum += v;
      }
      return sum;
    }

    bool operator<=(const Payload &other) const {
      std::vector<const std::string *> replica_names;
      for (auto & [ replica_name, _ ] : _payload) {
        replica_names.push_back(&replica_name);
      }
      for (auto & [ replica_name, _ ] : other._payload) {
        replica_names.push_back(&replica_name);
      }

      for (auto *replica_name : replica_names) {
        const bool leq =
            localValueForReplica(*replica_name) <= other.localValueForReplica(*replica_name);
        if (!leq) {
          return false;
        }
      }
      return false;
    }

    void increment(const std::string &replica_name, unsigned int delta) {
      _payload[replica_name] += delta;
    }

    void merge(const Payload &other) {
      std::vector<const std::string *> replica_names;
      for (auto & [ replica_name, _ ] : _payload) {
        replica_names.push_back(&replica_name);
      }
      for (auto & [ replica_name, _ ] : other._payload) {
        replica_names.push_back(&replica_name);
      }

      for (auto *replica_name : replica_names) {
        auto &val = _payload[*replica_name];
        val = std::max(val, other.localValueForReplica(*replica_name));
      }
    }

    void dump() const {
      for (auto & [ replica_name, replica ] : _payload) {
        printf("%s=%d\n", replica_name.c_str(), localValueForReplica(replica_name));
      }
    }

   private:
    std::unordered_map<std::string, unsigned int> _payload;
  };

  // GCounter definition {{{
  explicit GCounter(std::string name) : _name(std::move(name)) {}

  unsigned int query() const { return _payload.query(); }

  void increment(unsigned int delta = 1) {
    printf("Incrementing by %u at replica '%s'.\n", delta, _name.c_str());
    _payload.increment(_name, delta);
  }

  void merge(const Payload &other) { _payload.merge(other); }
  // }}}

  const std::string &name() const { return _name; }
  const Payload &payload() const { return _payload; }
  void dump() { printf("GCounter('%s', %d)\n", _name.c_str(), query()); }

 private:
  const std::string _name;
  Payload _payload;
};

class PNCounter {
 public:
  using ValueType = int;

  struct Payload {
    GCounter::Payload positive;
    GCounter::Payload negative;
  };

  // PNCounter definition {{{
  explicit PNCounter(std::string name) : _name(std::move(name)) {}

  int query() const { return (int)_payload.positive.query() - (int)_payload.negative.query(); }

  void increment(int delta) {
    if (delta >= 0) {
      printf("Incrementing by %u at replica '%s'.\n", delta, _name.c_str());
      _payload.positive.increment(_name, (unsigned int)delta);
    } else {
      printf("Decrementing by %u at replica '%s'.\n", -delta, _name.c_str());
      _payload.negative.increment(_name, (unsigned int)-delta);
    }
  }

  void merge(const Payload &other) {
    _payload.positive.merge(other.positive);
    _payload.negative.merge(other.negative);
  }
  // }}}

  const std::string &name() const { return _name; }
  const Payload &payload() const { return _payload; }
  void dump() { printf("PNCounter('%s', %d)\n", _name.c_str(), query()); }

 private:
  const std::string _name;
  Payload _payload;
};
