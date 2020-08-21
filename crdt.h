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

struct VersionVec {
  using Repr = std::unordered_map<std::string, uint64_t>;

  uint64_t max() const {
    uint64_t sum = 0;
    for (auto & [ _, v ] : data) {
      sum += v;
    }
    return sum;
  }

  void increment(const std::string &replica_name, uint64_t delta) { data[replica_name] += delta; }

  uint64_t localVersionForReplica(const std::string &replica_name) const {
    if (auto *value = lookup(data, replica_name)) {
      return *value;
    }
    return 0;
  }

  // Two version vectors v, w can be:
  //
  // (1) v < w : For all i, v[i] <= w[i] and there is at least one i such that v[i] < w[i];
  // (2) v > w : For all i, v[i] >= w[i] and there is at least one i such that v[i] > w[i];
  // (3) v = w : For all i, v[i] = w[i]; and
  // (4) v || w : otherwise -- concurrent version vectors -- not(v < w) and not(v >= w).

  bool operator<=(const VersionVec &other) const {
    std::vector<const std::string *> replica_names;
    for (auto & [ replica_name, _ ] : data) {
      replica_names.push_back(&replica_name);
    }
    for (auto & [ replica_name, _ ] : other.data) {
      replica_names.push_back(&replica_name);
    }

    for (auto *replica_name : replica_names) {
      const bool leq =
          localVersionForReplica(*replica_name) <= other.localVersionForReplica(*replica_name);
      if (!leq) {
        return false;
      }
    }
    return true;
  }

  // When v < w, we say that v is dominated by w. [Marc Shapiro et al 2011]
  // expresses the converse -- v is not dominated by w -- as (v || w) or (v >= w).
  //
  // My proof of equivalence:
  //
  // not(v < w) <=> (v || w) or (v >= w)
  //  <=> (not(v < w) and not(v >= w)) or (v >= w)                (4)
  //  <=> (not(v < w) or (v >= w)) and (not(v >= w)) or (v >= w)) (Distribut.)
  //  <=> (not(v < w) or (v >= w)) and true                       (Complement)
  //  <=> not(v < w) or (v >= w)                                  (Identity)
  //  <=> not(v < w)                                              (v >= w implies not(v < w)
  //                                                               so it can be dropped).
  bool operator<(const VersionVec &other) const {
    std::vector<const std::string *> replica_names;
    for (auto & [ replica_name, _ ] : data) {
      replica_names.push_back(&replica_name);
    }
    for (auto & [ replica_name, _ ] : other.data) {
      replica_names.push_back(&replica_name);
    }

    bool at_least_one_strict_lt = false;
    for (auto *replica_name : replica_names) {
      const auto v = localVersionForReplica(*replica_name);
      const auto w = other.localVersionForReplica(*replica_name);
      if (v < w) {
        at_least_one_strict_lt = true;
      } else if (v > w) {
        return false;
      }
    }
    return at_least_one_strict_lt;
  }

  bool dominatedBy(const VersionVec &other) const { return *this < other; }

  bool operator==(const VersionVec &other) const { return data == other.data; }

  uint64_t mergeVersionForReplica(const std::string &replica_name, uint64_t other_version) {
    if (other_version == 0) {
      auto *version = lookup(data, replica_name);
      return version ? *version : 0;
    }
    auto &max_version = data[replica_name];
    max_version = std::max(max_version, other_version);
    return max_version;
  }

  void merge(const VersionVec &other) {
    std::vector<const std::string *> replica_names;
    for (auto & [ replica_name, _ ] : data) {
      replica_names.push_back(&replica_name);
    }
    for (auto & [ replica_name, _ ] : other.data) {
      replica_names.push_back(&replica_name);
    }

    for (auto *replica_name : replica_names) {
      mergeVersionForReplica(*replica_name, other.localVersionForReplica(*replica_name));
    }
  }

  Repr::const_iterator begin() const { return data.begin(); }
  Repr::const_iterator end() const { return data.end(); }

 private:
  Repr data;
};

namespace std {

template <>
struct hash<VersionVec> {
  size_t operator()(const VersionVec &v) const {
    size_t ret = 0;
    for (const auto & [ key, value ] : v) {
      if (value != 0) {
        size_t h = 0;
        hash_combine(h, key);
        hash_combine(h, value);
        ret ^= h;
      }
    }
    return ret;
  }
};

}  // namespace std

class GCounter {
 public:
  using ValueType = uint64_t;
  using Payload = VersionVec;

  // GCounter definition {{{
  explicit GCounter(std::string name) : _name(std::move(name)) {}

  unsigned int query() const { return _payload.max(); }

  void increment(uint64_t delta = 1) {
    printf("Incrementing by %llu at replica '%s'.\n", delta, _name.c_str());
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
  using ValueType = int64_t;

  struct Payload {
    VersionVec positive;
    VersionVec negative;
  };

  // PNCounter definition {{{
  explicit PNCounter(std::string name) : _name(std::move(name)) {}

  int64_t query() const {
    return (int64_t)_payload.positive.max() - (int64_t)_payload.negative.max();
  }

  void increment(int64_t delta) {
    if (delta >= 0) {
      printf("Incrementing by %lld at replica '%s'.\n", delta, _name.c_str());
      _payload.positive.increment(_name, (uint64_t)delta);
    } else {
      printf("Decrementing by %lld at replica '%s'.\n", -delta, _name.c_str());
      _payload.negative.increment(_name, (uint64_t)-delta);
    }
  }

  void merge(const Payload &other) {
    _payload.positive.merge(other.positive);
    _payload.negative.merge(other.negative);
  }
  // }}}

  const std::string &name() const { return _name; }
  const Payload &payload() const { return _payload; }
  void dump() { printf("PNCounter('%s', %lld)\n", _name.c_str(), query()); }

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
struct ValuePrinter<std::unordered_set<T>> {
  void print(const std::unordered_set<T> &value) {
    ValuePrinter<T> printer;
    putchar('{');
    bool first = true;
    for (auto &elem : value) {
      if (!first) {
        printf(", ");
      } else {
        first = false;
      }
      printer.print(elem);
    }
    putchar('}');
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

template <typename T>
struct MVRegisterSetNode {
  MVRegisterSetNode() : _empty(true) {}

  MVRegisterSetNode(VersionVec version_vec)
      : _empty(true), _version_vector(std::move(version_vec)) {}

  MVRegisterSetNode(const T &value, VersionVec version_vec)
      : _value(value), _empty(false), _version_vector(std::move(version_vec)) {}

  bool operator==(const MVRegisterSetNode &other) const {
    if (_empty != other._empty) {
      return false;
    }
    if (_empty) {
      return _version_vector == other._version_vector;
    }
    return _value == other._value && _version_vector == other._version_vector;
  }

  const T *value() const { return _empty ? nullptr : &_value; }
  const VersionVec &versionVector() const { return _version_vector; }

 private:
  T _value;
  bool _empty = true;
  VersionVec _version_vector;
};

namespace std {

template <typename T>
struct hash<MVRegisterSetNode<T>> {
  size_t operator()(const MVRegisterSetNode<T> &key) const {
    size_t h = 0;
    auto *value = key.value();
    hash_combine(h, !!value);
    if (value) {
      hash_combine(h, *value);
    }
    hash_combine(h, key.versionVector());
    return h;
  }
};

}  // namespace std

// MV-Register does not behave like a set, contrary to what one might expect
// since its payload is a set.
template <typename T>
class MVRegister {
 public:
  using ValueType = std::unordered_set<T>;

  class Payload {
   public:
    void assign(ValueType value, const std::string &replica_name) {
      _set.clear();
      const auto version_vec = bumpVersionVector(replica_name);
      if (value.empty()) {
        _set.emplace(version_vec);
      } else {
        for (const auto &i : value) {
          _set.emplace(i, version_vec);
        }
      }
    }

    ValueType query() const {
      ValueType ret;
      for (auto &node : _set) {
        auto *value = node.value();
        if (value) {
          ret.insert(*value);
        }
      }
      return ret;
    }

    void merge(const Payload &other) {
      std::unordered_set<MVRegisterSetNode<T>> merged;
      for (const MVRegisterSetNode<T> &i : _set) {
        for (const MVRegisterSetNode<T> &j : other._set) {
          if (!i.versionVector().dominatedBy(j.versionVector())) {
            merged.insert(i);
          }
          if (!j.versionVector().dominatedBy(i.versionVector())) {
            merged.insert(j);
          }
        }
      }
      _set = merged;
    }

   private:
    VersionVec bumpVersionVector(const std::string &replica_name) {
      VersionVec inc_version_vec;
      for (auto &i : _set) {
        inc_version_vec.merge(i.versionVector());
      }
      inc_version_vec.increment(replica_name, 1);
      return inc_version_vec;
    }

    std::unordered_set<MVRegisterSetNode<T>> _set;
  };

  // MVRegister definition {{{
  explicit MVRegister(std::string name) : _name(std::move(name)) {}

  void assign(ValueType value) { _payload.assign(std::move(value), _name); }

  const ValueType query() const { return _payload.query(); }

  void merge(const Payload &other) { _payload.merge(other); }
  // }}}

  void clear() { assign({}); }
  const std::string &name() const { return _name; }
  const Payload &payload() const { return _payload; }

  void dump() {
    printf("MVRegister('%s', ", _name.c_str());
    ValuePrinter<ValueType> printer;
    printer.print(query());
    puts(")");
  }

 private:
  std::string _name;
  Payload _payload;
};
