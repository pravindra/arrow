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

#include <gtest/gtest.h>

#include "gandiva/decimal_type_util.h"

namespace gandiva {

TEST(DecimalResultTypes, Basic) {
  auto t1 = std::make_shared<arrow::Decimal128Type>(38, 10);
  auto t2 = std::make_shared<arrow::Decimal128Type>(38, 38);
  auto t3 = std::make_shared<arrow::Decimal128Type>(38, 0);

  Decimal128TypePtr ret_type;

  auto status =
      DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpDivide, {t1, t2}, &ret_type);
  EXPECT_EQ(ret_type->precision(), 38);
  EXPECT_EQ(ret_type->scale(), 6);

  status =
      DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpDivide, {t1, t3}, &ret_type);
  EXPECT_EQ(ret_type->precision(), 38);
  EXPECT_EQ(ret_type->scale(), 10);
}

}  // namespace gandiva
