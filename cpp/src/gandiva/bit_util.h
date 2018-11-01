// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Adapted from Apache Impala

#ifndef GANDIVA_BIT_UTIL_H
#define GANDIVA_BIT_UTIL_H

#include <limits>
#if defined(__APPLE__)
#include <machine/endian.h>
#else
#include <endian.h>
#endif

#include "arrow/util/macros.h"
#include "gandiva/decimal_large_ints.h"
#include "gandiva/logging.h"

namespace gandiva {

/// Utility class to do standard bit tricks
/// TODO: is this in boost or something else like that?
class BitUtil {
 public:
  /// Returns the width of the integer portion of the type, not counting the sign bit.
  /// Not safe for use with unknown or non-native types, so make it undefined
  template <typename T, typename CVR_REMOVED = typename std::decay<T>::type,
            typename std::enable_if<std::is_integral<CVR_REMOVED>{} ||
                                        std::is_same<CVR_REMOVED, unsigned __int128>{} ||
                                        std::is_same<CVR_REMOVED, __int128>{},
                                    int>::type = 0>
  constexpr static inline int32_t UnsignedWidth() {
    return std::is_integral<CVR_REMOVED>::value
               ? std::numeric_limits<CVR_REMOVED>::digits
               : std::is_same<CVR_REMOVED, unsigned __int128>::value
                     ? 128
                     : std::is_same<CVR_REMOVED, __int128>::value ? 127 : -1;
  }

  /// Return an integer signifying the sign of the value, returning +1 for
  /// positive integers (and zero), -1 for negative integers.
  /// The extra shift is to silence GCC warnings about full width shift on
  /// unsigned types. It compiles out in optimized builds into the expected increment.
  template <typename T>
  constexpr static inline T Sign(T value) {
    return 1 | ((value >> (UnsignedWidth<T>() - 1)) >> 1);
  }

  template <typename T>
  static inline int32_t CountLeadingZeros(T v) {
    DCHECK_GE(v, 0);
    if (sizeof(T) == 4) {
      uint32_t orig = static_cast<uint32_t>(v);
      return __builtin_clz(orig);
    } else if (sizeof(T) == 8) {
      uint64_t orig = static_cast<uint64_t>(v);
      return __builtin_clzll(orig);
    } else {
      DCHECK_EQ(sizeof(T), 16);
      if (ARROW_PREDICT_FALSE(v == 0)) return 128;
      unsigned __int128 orig = static_cast<unsigned __int128>(v);
      unsigned __int128 shifted = orig >> 64;
      if (shifted != 0) {
        return __builtin_clzll(shifted);
      } else {
        return __builtin_clzll(orig) + 64;
      }
    }
  }
};

template <>
inline int256_t BitUtil::Sign(int256_t value) {
  return value < 0 ? -1 : 1;
}

}  // namespace gandiva

#endif  // GANDIVA_BIT_UTIL_H
