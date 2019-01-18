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

#ifndef ARROW_DECIMAL_H
#define ARROW_DECIMAL_H

#include <array>
#include <cstdint>
#include <limits>
#include <sstream>
#include <string>
#include <type_traits>

#include "arrow/status.h"
#include "arrow/util/decimal_basic.h"
#include "arrow/util/string_view.h"

namespace arrow {

/// Represents a signed 128-bit integer in two's complement.
/// Calculations wrap around and overflow is ignored.
///
/// For a discussion of the algorithms, look at Knuth's volume 2,
/// Semi-numerical Algorithms section 4.3.1.
///
/// Adapted from the Apache ORC C++ implementation
class ARROW_EXPORT Decimal128 : public DecimalBasic128 {
 public:
  /// \brief Create an Decimal128 from the two's complement representation.
  constexpr Decimal128(int64_t high, uint64_t low) noexcept
      : DecimalBasic128(high, low) {}

  /// \brief Empty constructor creates an Decimal128 with a value of 0.
  constexpr Decimal128() noexcept : DecimalBasic128() {}

  /// \brief Convert any integer value into an Decimal128.
  template <typename T,
            typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
  constexpr Decimal128(T value) noexcept : DecimalBasic128(value) {}

  /// \brief Empty constructor creates an Decimal128 with a value of 0.
  constexpr Decimal128(const DecimalBasic128& value) noexcept : DecimalBasic128(value) {}

  /// \brief Parse the number from a base 10 string representation.
  explicit Decimal128(const std::string& value);

  /// \brief Create an Decimal128 from an array of bytes. Bytes are assumed to be in
  /// little endian byte order.
  explicit Decimal128(const uint8_t* bytes) : DecimalBasic128(bytes) {}

  Status Divide(const Decimal128& divisor, Decimal128* result,
                Decimal128* remainder) const {
    auto dstatus = DecimalBasic128::Divide(divisor, result, remainder);
    return ToArrowStatus(dstatus);
  }

  /// \brief Convert Decimal128 from one scale to another
  Status Rescale(int32_t original_scale, int32_t new_scale, Decimal128* out) const {
    auto dstatus = DecimalBasic128::Rescale(original_scale, new_scale, out);
    return ToArrowStatus(dstatus);
  }

  /// \brief Convert from a big endian byte representation. The length must be
  ///        between 1 and 16
  /// \return error status if the length is an invalid value
  static Status FromBigEndian(const uint8_t* data, int32_t length, Decimal128* out);

  /// \brief Convert the Decimal128 value to a base 10 decimal string with the given
  /// scale.
  std::string ToString(int32_t scale) const;

  /// \brief Convert the value to an integer string
  std::string ToIntegerString() const;

  static Status FromString(const util::string_view& s, Decimal128* out,
                           int32_t* precision = NULLPTR, int32_t* scale = NULLPTR);
  /// \brief Convert a decimal string to an Decimal128 value, optionally including
  /// precision and scale if they're passed in and not null.
  static Status FromString(const std::string& s, Decimal128* out,
                           int32_t* precision = NULLPTR, int32_t* scale = NULLPTR);
  static Status FromString(const char* s, Decimal128* out, int32_t* precision = NULLPTR,
                           int32_t* scale = NULLPTR);

  /// \brief Convert to a signed integer
  template <typename T, typename = internal::EnableIfIsOneOf<T, int32_t, int64_t>>
  Status ToInteger(T* out) const {
    constexpr auto min_value = std::numeric_limits<T>::min();
    constexpr auto max_value = std::numeric_limits<T>::max();
    const auto& self = *this;
    if (self < min_value || self > max_value) {
      return Status::Invalid("Invalid cast from DecimalBasic128 to ", sizeof(T),
                             " byte integer");
    }
    *out = static_cast<T>(low_bits());
    return Status::OK();
  }

 private:
  Status ToArrowStatus(DecimalStatus dstatus) const;
};

}  // namespace arrow

#endif  //  ARROW_DECIMAL_H
