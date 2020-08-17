// Copyright (C) 2020 Felipe O. Carvalho
#pragma once

#include <string>
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
