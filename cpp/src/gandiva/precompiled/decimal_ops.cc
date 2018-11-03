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

// BitMap functions

#define NDEBUG  // work-around DCHECK

#include "arrow/util/bit-util.h"
#include "gandiva/decimal_value_inline.h"
#include "gandiva/ir_struct_types.h"

using gandiva::int128_t;
using gandiva::IRDecimal128;

extern "C" {

#include "./types.h"

FORCE_INLINE
void add_decimal128_decimal128(void* xv, void* yv, void* retv) {
  auto x = static_cast<const IRDecimal128*>(xv);
  auto y = static_cast<const IRDecimal128*>(yv);
  auto ret = static_cast<IRDecimal128*>(retv);

  gandiva::Decimal16Value x_value(x->value);
  gandiva::Decimal16Value y_value(y->value);
  bool overflow = false;

  auto result = x_value.Add<int128_t>(x->scale, y_value, y->scale, ret->precision,
                                      ret->scale, true /*round*/, &overflow);
  ret->value = result.value();
}

}  // extern "C"
