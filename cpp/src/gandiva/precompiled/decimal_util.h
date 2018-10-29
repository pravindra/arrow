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

#ifndef GANDIVA_DECIMAL_UTIL_H
#define GANDIVA_DECIMAL_UTIL_H

#include <cstdint>
#include <ostream>
#include <string>

#include "arrow/util/macros.h"
#include "gandiva/precompiled/bit_util.h"
#include "gandiva/precompiled/multi_precision.h"

namespace gandiva {

class DecimalUtil {
 public:
  /// The maximum precision representable by a 4-byte decimal (Decimal4Value)
  static const int32_t MAX_DECIMAL4_PRECISION = 9;

  /// The maximum precision representable by a 8-byte decimal (Decimal8Value)
  static const int32_t MAX_DECIMAL8_PRECISION = 18;

  /// The maximum precision representable by a 16-byte decimal (Decimal16Value)
  static const int32_t MAX_PRECISION = 38;

  static const int32_t MAX_SCALE = MAX_PRECISION;
  static const int32_t MIN_ADJUSTED_SCALE = 6;

  /// Maximum absolute value of a valid Decimal4Value. This is 9 digits of 9's.
  static const int32_t MAX_UNSCALED_DECIMAL4 = 999999999;

  /// Maximum absolute value of a valid Decimal8Value. This is 18 digits of 9's.
  static const int64_t MAX_UNSCALED_DECIMAL8 = 999999999999999999;

  /// Maximum absolute value a valid Decimal16Value. This is 38 digits of 9's.
  static const int128_t MAX_UNSCALED_DECIMAL16 =
      99 +
      100 * (MAX_UNSCALED_DECIMAL8 +
             (1 + MAX_UNSCALED_DECIMAL8) * static_cast<int128_t>(MAX_UNSCALED_DECIMAL8));

  // Helper function that checks for multiplication overflow. We only check for overflow
  // if may_overflow is false.
  template <typename T>
  static T SafeMultiply(T a, T b, bool may_overflow) {
    T result = a * b;
    DCHECK(may_overflow || a == 0 || result / a == b);
    return result;
  }

  template <typename T>
  static T MultiplyByScale(const T& v, int32_t scale, bool may_overflow) {
    T multiplier = GetScaleMultiplier<T>(scale);
    DCHECK_GT(multiplier, 0);
    return SafeMultiply(v, multiplier, may_overflow);
  }

  template <typename T>
  static T GetScaleMultiplier(int32_t scale) {
    DCHECK_GE(scale, 0);
    T result = 1;
    for (int32_t i = 0; i < scale; ++i) {
      // Verify that the result of multiplication does not overflow.
      // TODO: This is not an ideal way to check for overflow because if T is signed, the
      // behavior is undefined in case of overflow. Replace this with a better overflow
      // check.
      DCHECK_GE(result * 10, result);
      result *= 10;
    }
    return result;
  }

  /// Helper function to scale down values by 10^delta_scale, truncating if
  /// round is false or rounding otherwise.
  template <typename T>
  static inline T ScaleDownAndRound(T value, int32_t delta_scale, bool round) {
    DCHECK_GT(delta_scale, 0);
    T divisor = DecimalUtil::GetScaleMultiplier<T>(delta_scale);
    if (divisor > 0) {
      DCHECK_EQ(divisor % 2, 0);
      T result = value / divisor;
      if (round) {
        T remainder = value % divisor;
        // In general, shifting down the multiplier is not safe, but we know
        // here that it is a multiple of two.
        if (abs(remainder) >= (divisor >> 1)) {
          // Bias at zero must be corrected by sign of dividend.
          result += BitUtil::Sign(value);
        }
      }
      return result;
    } else {
      DCHECK_EQ(divisor, -1);
      return 0;
    }
  }
};

template <>
inline int32_t DecimalUtil::GetScaleMultiplier<int32_t>(int32_t scale) {
  DCHECK_GE(scale, 0);
  static const int32_t values[] = {1,      10,      100,      1000,      10000,
                                   100000, 1000000, 10000000, 100000000, 1000000000};
  DCHECK_GE(sizeof(values) / sizeof(int32_t), MAX_DECIMAL4_PRECISION);
  if (ARROW_PREDICT_TRUE(scale < 10)) return values[scale];
  return -1;  // Overflow
}

template <>
inline int64_t DecimalUtil::GetScaleMultiplier<int64_t>(int32_t scale) {
  DCHECK_GE(scale, 0);
  static const int64_t values[] = {1ll,
                                   10ll,
                                   100ll,
                                   1000ll,
                                   10000ll,
                                   100000ll,
                                   1000000ll,
                                   10000000ll,
                                   100000000ll,
                                   1000000000ll,
                                   10000000000ll,
                                   100000000000ll,
                                   1000000000000ll,
                                   10000000000000ll,
                                   100000000000000ll,
                                   1000000000000000ll,
                                   10000000000000000ll,
                                   100000000000000000ll,
                                   1000000000000000000ll};
  DCHECK_GE(sizeof(values) / sizeof(int64_t), MAX_DECIMAL8_PRECISION);
  if (ARROW_PREDICT_TRUE(scale < 19)) return values[scale];
  return -1;  // Overflow
}

template <>
inline int128_t DecimalUtil::GetScaleMultiplier<int128_t>(int32_t scale) {
  DCHECK_GE(scale, 0);
  static const int128_t values[] = {
      static_cast<int128_t>(1ll),
      static_cast<int128_t>(10ll),
      static_cast<int128_t>(100ll),
      static_cast<int128_t>(1000ll),
      static_cast<int128_t>(10000ll),
      static_cast<int128_t>(100000ll),
      static_cast<int128_t>(1000000ll),
      static_cast<int128_t>(10000000ll),
      static_cast<int128_t>(100000000ll),
      static_cast<int128_t>(1000000000ll),
      static_cast<int128_t>(10000000000ll),
      static_cast<int128_t>(100000000000ll),
      static_cast<int128_t>(1000000000000ll),
      static_cast<int128_t>(10000000000000ll),
      static_cast<int128_t>(100000000000000ll),
      static_cast<int128_t>(1000000000000000ll),
      static_cast<int128_t>(10000000000000000ll),
      static_cast<int128_t>(100000000000000000ll),
      static_cast<int128_t>(1000000000000000000ll),
      static_cast<int128_t>(1000000000000000000ll) * 10ll,
      static_cast<int128_t>(1000000000000000000ll) * 100ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 1000000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 10000000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll * 10ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll * 100ll,
      static_cast<int128_t>(1000000000000000000ll) * 100000000000000000ll * 1000ll};
  DCHECK_GE(sizeof(values) / sizeof(int128_t), MAX_PRECISION);
  if (ARROW_PREDICT_TRUE(scale < 39)) return values[scale];
  return -1;  // Overflow
}
}  // namespace gandiva

#endif
