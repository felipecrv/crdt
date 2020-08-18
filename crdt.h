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

template <typename T>
struct ValuePrinter {
  void print(const T &) { assert(false && "ValuePrinter not implemented"); }
};

template <>
struct ValuePrinter<std::string> {
  void print(const std::string &value) { printf("'%s'", value.c_str()); }
};

template <typename T>
struct ValuePrinter<std::optional<T>> {
  void print(const std::optional<T> &value) {
    if (value.has_value()) {
      ValuePrinter<T> printer;
      printf("Some(");
      printer.print(*value);
      putchar(')');
    } else {
      printf("None");
    }
  }
};

template <typename T>
class LWWRegister {
 public:
  using ValueType = std::optional<T>;

  class Payload {
   public:
    Payload() : _value{}, _timestamp(0, 0), _empty(true) {}

    void assign(const T *value, uint64_t now, const std::string &replica_name) {
      if (value) {
        _value = *value;
      }
      _timestamp = std::make_pair(now, hashedReplicaName(replica_name));
      _empty = !value;
    }

    const T *query() const { return _empty ? nullptr : &_value; }

    bool operator<=(const Payload &other) const { return _timestamp <= other._timestamp; }

    void merge(const Payload &other) {
      if (*this <= other) {
        *this = other;
      }
    }

   private:
    static size_t hashedReplicaName(const std::string &name) {
      // In the real and distributed world this would be a well-defined and
      // stable hash function like SipHash-2-4, BLAKE, SHA1 etc.
      return std::hash<std::string>{}(name);
    }

    T _value;
    std::pair<uint64_t, size_t> _timestamp;
    bool _empty;
  };

  // LWWRegister definition {{{
  explicit LWWRegister(std::string name) : _name(std::move(name)) {}

  void assign(const T &value) {
    _now += 1;
    _payload.assign(&value, _now, _name);
  }

  void clear() {
    _now += 1;
    _payload.assign(nullptr, _now, _name);
  }

  const ValueType query() const {
    const auto *value = _payload.query();
    return value ? std::optional(*value) : std::nullopt;
  }

  void merge(const Payload &other) { _payload.merge(other); }
  // }}}

  const std::string &name() const { return _name; }
  const Payload &payload() const { return _payload; }

  void dump() {
    printf("LWWRegister('%s', ", _name.c_str());
    ValuePrinter<ValueType> printer;
    printer.print(query());
    puts(")");
  }

 private:
  const std::string _name;
  uint64_t _now = 0;
  Payload _payload;
};
