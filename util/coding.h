#ifndef MERODIS_CODING_H
#define MERODIS_CODING_H

#include <cstdint>

inline void EncodeFixed32(char* dst, uint32_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  buffer[0] = static_cast<uint8_t>(value >> 24);
  buffer[1] = static_cast<uint8_t>(value >> 16);
  buffer[2] = static_cast<uint8_t>(value >> 8);
  buffer[3] = static_cast<uint8_t>(value);
}

inline void EncodeFixed64(char* dst, uint64_t value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  buffer[0] = static_cast<uint8_t>(value >> 56);
  buffer[1] = static_cast<uint8_t>(value >> 48);
  buffer[2] = static_cast<uint8_t>(value >> 40);
  buffer[3] = static_cast<uint8_t>(value >> 32);
  buffer[4] = static_cast<uint8_t>(value >> 24);
  buffer[5] = static_cast<uint8_t>(value >> 16);
  buffer[6] = static_cast<uint8_t>(value >> 8);
  buffer[7] = static_cast<uint8_t>(value);
}

template<typename T>
inline void EncodeFixed64(char* dst, T value) {
  uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

  buffer[0] = static_cast<uint8_t>(value >> 56);
  buffer[1] = static_cast<uint8_t>(value >> 48);
  buffer[2] = static_cast<uint8_t>(value >> 40);
  buffer[3] = static_cast<uint8_t>(value >> 32);
  buffer[4] = static_cast<uint8_t>(value >> 24);
  buffer[5] = static_cast<uint8_t>(value >> 16);
  buffer[6] = static_cast<uint8_t>(value >> 8);
  buffer[7] = static_cast<uint8_t>(value);
}

inline uint32_t DecodeFixed32(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  return (static_cast<uint32_t>(buffer[0]) << 24) |
         (static_cast<uint32_t>(buffer[1]) << 16) |
         (static_cast<uint32_t>(buffer[2]) << 8) |
         (static_cast<uint32_t>(buffer[3]));
}

inline uint64_t DecodeFixed64(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  return (static_cast<uint64_t>(buffer[0]) << 56) |
         (static_cast<uint64_t>(buffer[1]) << 48) |
         (static_cast<uint64_t>(buffer[2]) << 40) |
         (static_cast<uint64_t>(buffer[3]) << 32) |
         (static_cast<uint64_t>(buffer[4]) << 24) |
         (static_cast<uint64_t>(buffer[5]) << 16) |
         (static_cast<uint64_t>(buffer[6]) << 8) |
         (static_cast<uint64_t>(buffer[7]));
}

template<typename T>
inline T DecodeFixed64(const char* ptr) {
  const uint8_t* const buffer = reinterpret_cast<const uint8_t*>(ptr);

  return (static_cast<T>(buffer[0]) << 56) |
         (static_cast<T>(buffer[1]) << 48) |
         (static_cast<T>(buffer[2]) << 40) |
         (static_cast<T>(buffer[3]) << 32) |
         (static_cast<T>(buffer[4]) << 24) |
         (static_cast<T>(buffer[5]) << 16) |
         (static_cast<T>(buffer[6]) << 8) |
         (static_cast<T>(buffer[7]));
}

#endif //MERODIS_CODING_H
