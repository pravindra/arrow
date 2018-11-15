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

#ifndef GANDIVA_DECIMAL_128FULL_H
#define GANDIVA_DECIMAL_128FULL_H

#include <string>

namespace gandiva {
class Decimal128Full {
 public:
  Decimal128Full(std::string& value, int precision, int scale)
      : value_(value), precision_(precision), scale_(scale) {}

  bool Equals(const Decimal128Full& other) const {
    return precision_ == other.precision() && scale_ == other.scale() &&
           value_ == other.value();
  }

  std::string ToString() const {
    return "(" + value_ + "," + std::to_string(precision_) + "," +
           std::to_string(scale_) + ")";
  }

  friend std::ostream& operator<<(std::ostream& os, const Decimal128Full& dec) {
    os << dec.value() << "," << std::to_string(dec.precision()) << ","
       << std::to_string(dec.scale());
    return os;
  }

  const std::string& value() const { return value_; }

  int32_t precision() const { return precision_; }

  int32_t scale() const { return scale_; }

 private:
  std::string value_;
  int32_t precision_;
  int32_t scale_;
};
}  // namespace gandiva

#endif  // GANDIVA_DECIMAL_128FULL_H
