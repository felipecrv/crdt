// Copyright (C) 2020 Felipe O. Carvalho

#include <stdio.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// STL helpers

template <typename MapType>
typename MapType::mapped_type *lookup(MapType &m, const typename MapType::key_type &k) {
  auto it = m.find(k);
  if (it != m.end()) {
    return &it->second;
  }
  return nullptr;
}

template <typename MapType>
const typename MapType::mapped_type *lookup(const MapType &m, const typename MapType::key_type &k) {
  auto it = m.find(k);
  if (it != m.end()) {
    return &it->second;
  }
  return nullptr;
}

template <typename Container>
bool contains(const Container &s, const typename Container::value_type &v) {
  return s.find(v) != s.end();
}

template <typename Container>
bool linearContains(const Container &s, const typename Container::value_type &v) {
  return std::find(s.begin(), s.end(), v) != s.end();
}

template <typename T>
void hash_combine(std::size_t &seed, const T &val) {
  seed ^= std::hash<T>()(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

// Hashers

namespace std {

template <typename A, typename B>
struct hash<std::pair<A, B>> {
  size_t operator()(const std::pair<A, B> &k) const {
    size_t h = 0;
    hash_combine(h, k.first);
    hash_combine(h, k.second);
    return h;
  }
};

template <>
struct hash<vector<string>> {
  size_t operator()(const vector<string> &v) const {
    size_t h = 0;
    for (const auto &s : v) {
      hash_combine(h, s);
    }
    return h;
  }
};

}  // namespace std

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

  void merge(const GCounter &other) { _payload.merge(other._payload); }
  // }}}

  const std::string &name() const { return _name; }
  void dump() { printf("GCounter('%s', %d)\n", _name.c_str(), query()); }

 private:
  const std::string _name;
  Payload _payload;
};

template <typename CRDT>
class P2PNetwork {
 public:
  size_t add(CRDT *crdt) {
    _replicas.push_back(crdt);
    return _replicas.size() - 1;
  }

  void broadcast(size_t i) {
    const CRDT *replica = _replicas[i];
    if (!replica) {
      return;
    }
    printf("Broadcasting from '%s' to all connected replicas...\n", replica->name().c_str());
    for (size_t j = 0; j < _replicas.size(); j++) {
      if (j != i) {
        auto *other = _replicas[j];
        if (other) {
          other->merge(*replica);
        }
      }
    }
  }

  void broadcastAll() {
    for (size_t i = 0; i < _replicas.size(); i++) {
      broadcast(i);
    }
  }

  void disconnect(size_t i) {
    CRDT *replica = _replicas[i];
    if (replica) {
      printf("Disconnect '%s' from the network.\n", replica->name().c_str());
      _replicas[i] = nullptr;
      _offline_set.emplace(i, replica);
    }
  }

  void reconnect(size_t i) {
    for (auto it = _offline_set.begin(); it != _offline_set.end(); ++it) {
      if (it->first == i) {
        assert(_replicas[i] == nullptr);
        printf("Reconnecting '%s' to the network.\n", it->second->name().c_str());
        _replicas[i] = it->second;
        _offline_set.erase(it);
        break;
      }
    }
  }

  int countPartitions() const {
    std::unordered_set<unsigned int> distinct_values;
    for (auto *replica : _replicas) {
      if (replica) {
        const auto value = replica->query();
        distinct_values.insert(value);
      }
    }
    for (auto & [ _, replica ] : _offline_set) {
      const auto value = replica->query();
      distinct_values.insert(value);
    }
    return (int)distinct_values.size();
  }

  void dump() const {
    printf("Network state:\n");
    if (!_offline_set.empty()) {
      printf("- online:\n");
    }
    for (auto *replica : _replicas) {
      if (replica) {
        replica->dump();
      }
    }
    if (!_offline_set.empty()) {
      printf("- offline\n");
      for (auto[_, replica] : _offline_set) {
        replica->dump();
      }
    }
    if (countPartitions() == 1) {
      puts("ALL CONVERGED!");
    }
    printf("\n");
  }

 private:
  std::vector<CRDT *> _replicas;
  std::unordered_set<std::pair<size_t, CRDT *>> _offline_set;
};

void simulteGCounters() {
  P2PNetwork<GCounter> network;

  GCounter a_counter("A");
  GCounter b_counter("B");
  GCounter c_counter("C");

  const size_t a = network.add(&a_counter);  // a=0
  const size_t b = network.add(&b_counter);  // b=0
  const size_t c = network.add(&c_counter);  // c=0
  (void)c;
  network.dump();
  assert(a_counter.query() == 0);
  assert(b_counter.query() == 0);
  assert(c_counter.query() == 0);

  a_counter.increment(1);  // a=1
  b_counter.increment(2);  // b=2
  c_counter.increment(3);  // c=3
  network.dump();
  assert(a_counter.query() == 1);
  assert(b_counter.query() == 2);
  assert(c_counter.query() == 3);
  assert(network.countPartitions() == 3);

  network.broadcast(a);  // a=1, b=3, c=4
  network.dump();
  assert(network.countPartitions() == 3);

  network.broadcastAll();  // a=6
  network.dump();
  assert(network.countPartitions() == 1);

  network.disconnect(b);
  a_counter.increment(10);  // a=16
  network.dump();

  network.broadcastAll();
  network.dump();
  assert(a_counter.query() == 16);
  assert(b_counter.query() == 6);
  assert(c_counter.query() == 16);
  assert(network.countPartitions() == 2);

  b_counter.increment(3);
  network.dump();
  assert(network.countPartitions() == 2);

  network.reconnect(b);
  network.broadcastAll();
  network.dump();
  assert(network.countPartitions() == 1);
}

int main(int argc, char *argv[]) {
  simulteGCounters();
  return 0;
}
