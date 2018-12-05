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

#ifndef DECIMAL_FULL_H
#define DECIMAL_FULL_H

#include <cstdint>
#include <string>
#include "arrow/util/decimal.h"

namespace gandiva {

using Decimal128 = arrow::Decimal128;

class Decimal128Full {
 public:
  Decimal128Full(int64_t high_bits, uint64_t low_bits, int32_t precision, int32_t scale)
      : value_(high_bits, low_bits), precision_(precision), scale_(scale) {}

  Decimal128Full(std::string value, int32_t precision, int32_t scale)
      : value_(value), precision_(precision), scale_(scale) {}

  Decimal128Full(const Decimal128& value, int32_t precision, int32_t scale)
      : value_(value), precision_(precision), scale_(scale) {}

  Decimal128Full(int32_t precision, int32_t scale)
      : value_(0), precision_(precision), scale_(scale) {}

  inline bool Equals(const Decimal128Full& o) const {
    return value_ == o.value_ && precision_ == o.precision_ && scale_ == o.scale_;
  }

  inline std::string ToString() const {
    return value_.ToString(0) + "," + std::to_string(precision_) + "," +
           std::to_string(scale_);
  }

  uint32_t scale() const { return scale_; }

  uint32_t precision() const { return precision_; }

  const arrow::Decimal128& value() const { return value_; }

 private:
  Decimal128 value_;

  int32_t precision_;
  int32_t scale_;
};

}  // namespace gandiva

#endif  // DECIMAL_FULL_H
