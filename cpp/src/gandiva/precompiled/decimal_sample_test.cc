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

#include <gtest/gtest.h>

#include "gandiva/decimal_type_util.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {

Decimal64Full MakeDecimal64(int64_t value, int precision, int scale) {
  Decimal64Full a{value, precision, scale};
  return a;
}

static int kMaxPrecision = 18;
static int kMinAdjustedScale = 6;

Decimal64Full GetResultType(DecimalTypeUtil::Op op, const Decimal64Full& t1,
                            const Decimal64Full& t2) {
  int32_t s1 = t1.scale;
  int32_t s2 = t2.scale;
  int32_t p1 = t1.precision;
  int32_t p2 = t2.precision;
  int32_t result_scale;
  int32_t result_precision;

  switch (op) {
    case DecimalTypeUtil::kOpAdd:
    case DecimalTypeUtil::kOpSubtract:
      result_scale = std::max(s1, s2);
      result_precision = std::max(p1 - s1, p2 - s2) + result_scale + 1;
      break;

    case DecimalTypeUtil::kOpMultiply:
      result_scale = s1 + s2;
      result_precision = p1 + p2 + 1;
      break;

    case DecimalTypeUtil::kOpDivide:
      result_scale = std::max(kMinAdjustedScale, s1 + p2 + 1);
      result_precision = p1 - s1 + s2 + result_scale;
      break;

    case DecimalTypeUtil::kOpMod:
      result_scale = std::max(s1, s2);
      result_precision = std::min(p1 - s1, p2 - s2) + result_scale;
      break;
  }
  if (result_precision > kMaxPrecision) {
    int32_t min_scale = std::min(result_scale, kMinAdjustedScale);
    int32_t delta = result_precision - kMaxPrecision;
    result_precision = kMaxPrecision;
    result_scale = std::max(result_scale - delta, min_scale);
  }
  return MakeDecimal64(0, result_precision, result_scale);
}

Decimal64Full DoAdd(const Decimal64Full x, const Decimal64Full y) {
  auto out = GetResultType(DecimalTypeUtil::kOpAdd, x, y);
  add_decimal64_decimal64(&x, &y, &out);
  return out;
}

#define EXPECT_DECIMAL_EQ(a, b)                                                      \
  EXPECT_TRUE(a.Equals(b)) << "expected : "                                          \
                           << "(" << a.value << "," << a.precision << "," << a.scale \
                           << ")"                                                    \
                           << " found "                                              \
                           << "(" << b.value << "," << b.precision << "," << b.scale \
                           << ")"

//
// Add :
// case 1 : result precision < 18
// case 2 : result precision == 18
//
TEST(TestDecimalSample, AddCase1) {
  EXPECT_DECIMAL_EQ(MakeDecimal64(502, 11, 3),
                    DoAdd(MakeDecimal64(201, 10, 3), MakeDecimal64(301, 10, 3)));

  EXPECT_DECIMAL_EQ(MakeDecimal64(3211, 12, 3),
                    DoAdd(MakeDecimal64(201, 10, 3), MakeDecimal64(301, 10, 2)));

  EXPECT_DECIMAL_EQ(MakeDecimal64(2311, 12, 4),
                    DoAdd(MakeDecimal64(201, 10, 3), MakeDecimal64(301, 10, 4)));
}

TEST(TestDecimalSample, AddCase2) {
  EXPECT_DECIMAL_EQ(MakeDecimal64(502, 18, 3),
                    DoAdd(MakeDecimal64(201, 18, 3), MakeDecimal64(301, 18, 3)));

  EXPECT_DECIMAL_EQ(MakeDecimal64(3211, 18, 3),
                    DoAdd(MakeDecimal64(201, 18, 3), MakeDecimal64(301, 18, 2)));

  EXPECT_DECIMAL_EQ(MakeDecimal64(2311, 18, 4),
                    DoAdd(MakeDecimal64(201, 18, 3), MakeDecimal64(301, 18, 4)));
}

}  // namespace gandiva
