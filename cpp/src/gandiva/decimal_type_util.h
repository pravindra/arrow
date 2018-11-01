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

#ifndef GANDIVA_DECIMAL_TYPE_UTIL_H
#define GANDIVA_DECIMAL_TYPE_UTIL_H

#include <algorithm>
#include <memory>

#include "arrow/type.h"
#include "gandiva/arrow.h"

namespace gandiva {

/// @brief Handles conversion of scale/precision for operations on decimal types.
class DecimalTypeUtil {
 public:
  enum Op {
    kOpAdd,
    kOpSubtract,
    kOpMultiply,
    kOpDivide,
    kOpMod,
  };

  /// The maximum precision representable by a 4-byte decimal
  static constexpr int32_t kMaxDecimal4Precision = 9;

  /// The maximum precision representable by a 8-byte decimal
  static constexpr int32_t kMaxDecimal8Precision = 18;

  /// The maximum precision representable by a 16-byte decimal
  static constexpr int32_t kMaxPrecision = 38;

  static constexpr int32_t kMaxScale = kMaxPrecision;
  static constexpr int32_t kMinAdjustedScale = 6;

  // For specified operation and input scale/precision, determine the output
  // scale/precision.
  static Status GetResultType(Op op, const Decimal128TypeVector& in_types,
                              Decimal128TypePtr* out_type);

 private:
  static Decimal128TypePtr MakeType(int32_t precision, int32_t scale);
  static Decimal128TypePtr MakeAdjustedType(int32_t precision, int32_t scale);
};

inline Decimal128TypePtr DecimalTypeUtil::MakeType(int32_t precision, int32_t scale) {
  return std::make_shared<arrow::Decimal128Type>(precision, scale);
}

inline Decimal128TypePtr DecimalTypeUtil::MakeAdjustedType(int32_t precision,
                                                           int32_t scale) {
  if (precision > kMaxPrecision) {
    int32_t min_scale = std::min(scale, kMinAdjustedScale);
    int32_t delta = precision - kMaxPrecision;
    precision = kMaxPrecision;
    scale = std::max(scale - delta, min_scale);
  }
  return MakeType(precision, scale);
}

// Implementation of decimal rules.
inline Status DecimalTypeUtil::GetResultType(Op op, const Decimal128TypeVector& in_types,
                                             Decimal128TypePtr* out_type) {
  // TODO : validations
  *out_type = NULL;
  auto t1 = in_types[0];
  auto t2 = in_types[1];

  switch (op) {
    case kOpAdd:
    case kOpSubtract:
      *out_type = MakeType(
          std::max(t1->scale(), t2->scale()) +
              std::max(t1->precision() - t1->scale(), t2->precision() - t2->scale()) + 1,
          std::max(t1->scale(), t2->scale()));
      break;

    case kOpMultiply:
      *out_type = MakeType(t1->precision() + t2->precision(), t1->scale() + t2->scale());
      break;

    case kOpDivide: {
      int32_t result_scale =
          std::max(kMinAdjustedScale, t1->scale() + t2->precision() + 1);
      int32_t result_precision =
          t1->precision() - t1->scale() + t2->scale() + result_scale;
      *out_type = MakeAdjustedType(result_precision, result_scale);
      break;
    }

    case kOpMod:
      *out_type = MakeType(
          std::min(t1->precision() - t1->scale(), t2->precision() - t2->scale()) +
              std::max(t1->scale(), t2->scale()),
          std::max(t1->scale(), t2->scale()));
      break;
  }

  return Status::OK();
}

}  // namespace gandiva

#endif  // GANDIVA_DECIMAL_TYPE_UTIL_H
