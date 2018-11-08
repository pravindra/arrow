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

// Decimal functions

#include <algorithm>

using int128_t = __int128;

extern "C" {

#include "gandiva/precompiled/types.h"

static int64_t scale_multipliers_[] = {
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
    10000000000000,
    100000000000000,
    1000000000000000,
    10000000000000000,
    100000000000000000,
    1000000000000000000,
};

int32_t GetHigherScale(int32_t x_scale, int32_t y_scale) {
  return (x_scale <= y_scale ? y_scale : x_scale);
}

int64_t AdjustToHigherScale64(int64_t in, int32_t delta_scale) {
  if (delta_scale <= 0) {
    return in;
  } else {
    return in * scale_multipliers_[delta_scale];
  }
}

int128_t AdjustToHigherScale128(int128_t in, int32_t delta_scale) {
  if (delta_scale <= 0) {
    return in;
  } else {
    return in * scale_multipliers_[delta_scale];
  }
}

int64_t AddFastPath(int64_t x_value, int32_t x_scale, int64_t y_value, int32_t y_scale) {
  auto higher_scale = std::max(x_scale, y_scale);
  auto x_scaled = AdjustToHigherScale64(x_value, higher_scale - x_scale);
  auto y_scaled = AdjustToHigherScale64(y_value, higher_scale - y_scale);
  return x_scaled + y_scaled;
}

void add_decimal64_decimal64(const Decimal64Full* x, const Decimal64Full* y,
                             Decimal64Full* out) {
  if (out->precision < 19) {
    out->value = AddFastPath(x->value, x->scale, y->value, y->scale);
  } else {
    int128_t x_up = x->value;
    int128_t y_up = y->value;

    auto higher_scale = std::max(x->scale, y->scale);
    auto x_scaled = AdjustToHigherScale128(x_up, higher_scale - x->scale);
    auto y_scaled = AdjustToHigherScale128(y_up, higher_scale - y->scale);
    auto res = x_scaled + y_scaled;

    int32_t delta = higher_scale - out->scale;
    res /= scale_multipliers_[delta];

    out->value = static_cast<int64_t>(res);
  }
}

}  // extern "C"
