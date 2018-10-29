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

#include "gandiva/precompiled/types.h"

namespace gandiva {

Decimal64Full
MakeDecimal64(int64_t value, int precision, int scale) {
  Decimal64Full a{value, precision, scale};
  return a;
}

Decimal64Full
DoAdd(const Decimal64Full x, const Decimal64Full y) {
  Decimal64Full out;
  add_decimal64_decimal64(&x, &y, &out);
  return out;
}

#define EXPECT_DECIMAL_EQ(a, b) \
  EXPECT_TRUE(a.Equals(b)) << "expected : " \
    << "(" << a.value << "," << a.precision << "," << a.scale << ")" \
    << " found " \
    << "(" << b.value << "," << b.precision << "," << b.scale << ")"

TEST(TestDecimalSample, Simple) {
  EXPECT_DECIMAL_EQ(MakeDecimal64(5, 38, 0),
                    DoAdd(MakeDecimal64(2, 38, 0), MakeDecimal64(3, 38, 0)));
}

}  // namespace gandiva
