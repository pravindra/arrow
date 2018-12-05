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

#include <sstream>

#include <gtest/gtest.h>
#include "arrow/memory_pool.h"
#include "arrow/status.h"

#include "gandiva/decimal_full.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

using arrow::Decimal128;

namespace gandiva {

#define EXPECT_DECIMAL_SUM_EQUALS(x, y, expected, actual) \
  EXPECT_TRUE((expected).Equals(actual))                  \
      << (x).ToString() << " + " << (y).ToString()        \
      << " expected : " << (expected).ToString() << " actual : " << (actual).ToString();

class TestDecimalOps : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

  ArrayPtr MakeDecimalVector(const Decimal128Full& in);
  void AddAndVerify(const Decimal128Full& x, const Decimal128Full& y,
                    const Decimal128Full& expected);

 protected:
  arrow::MemoryPool* pool_;
};

ArrayPtr TestDecimalOps::MakeDecimalVector(const Decimal128Full& in) {
  std::vector<arrow::Decimal128> ret;

  auto decimal_type = std::make_shared<arrow::Decimal128Type>(in.precision(), in.scale());
  return MakeArrowArrayDecimal(decimal_type, {in.value()}, {true});
}

void TestDecimalOps::AddAndVerify(const Decimal128Full& x, const Decimal128Full& y,
                                  const Decimal128Full& expected) {
  auto x_type = std::make_shared<arrow::Decimal128Type>(x.precision(), x.scale());
  auto y_type = std::make_shared<arrow::Decimal128Type>(y.precision(), y.scale());
  auto field_x = field("x", x_type);
  auto field_y = field("y", y_type);
  auto schema = arrow::schema({field_x, field_y});

  Decimal128TypePtr output_type;
  auto status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd, {x_type, y_type},
                                               &output_type);
  // output fields
  auto res = field("res", output_type);

  // build expression : x + y
  auto expr = TreeExprBuilder::MakeExpression("add", {field_x, field_y}, res);

  // Build a projector for the expression.
  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  auto array_a = MakeDecimalVector(x);
  auto array_b = MakeDecimalVector(y);

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, 1 /*num_records*/, {array_a, array_b});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  auto out_array = dynamic_cast<arrow::Decimal128Array*>(outputs[0].get());
  const Decimal128 out_value(out_array->GetValue(0));

  auto dtype = dynamic_cast<arrow::Decimal128Type*>(out_array->type().get());
  Decimal128Full actual{out_value.ToString(0), dtype->precision(), dtype->scale()};

  EXPECT_DECIMAL_SUM_EQUALS(x, y, expected, actual);
}

TEST_F(TestDecimalOps, TestAdd) {
#if 0
  // fast-path
  AddAndVerify(Decimal128Full{"201", 30, 3},   // x
               Decimal128Full{"301", 30, 3},   // y
               Decimal128Full{"502", 31, 3});  // expected

  AddAndVerify(Decimal128Full{"201", 30, 3},    // x
               Decimal128Full{"301", 30, 2},    // y
               Decimal128Full{"3211", 32, 3});  // expected

  AddAndVerify(Decimal128Full{"201", 30, 3},    // x
               Decimal128Full{"301", 30, 4},    // y
               Decimal128Full{"2311", 32, 4});  // expected

  // max precision, but no overflow
  AddAndVerify(Decimal128Full{"201", 38, 3},   // x
               Decimal128Full{"301", 38, 3},   // y
               Decimal128Full{"502", 38, 3});  // expected

  AddAndVerify(Decimal128Full{"201", 38, 3},    // x
               Decimal128Full{"301", 38, 2},    // y
               Decimal128Full{"3211", 38, 3});  // expected

  AddAndVerify(Decimal128Full{"201", 38, 3},    // x
               Decimal128Full{"301", 38, 4},    // y
               Decimal128Full{"2311", 38, 4});  // expected
#endif

  AddAndVerify(Decimal128Full{"201", 38, 3},      // x
               Decimal128Full{"301", 38, 7},      // y
               Decimal128Full{"201030", 38, 6});  // expected

#if 0
  AddAndVerify(Decimal128Full{"1201", 38, 3},   // x
               Decimal128Full{"1801", 38, 3},   // y
               Decimal128Full{"3002", 38, 3});  // expected (carry-over from fractional)

  // max precision
  AddAndVerify(Decimal128Full{"09999999999999999999999999999999000000", 38, 5},  // x
               Decimal128Full{"100", 38, 7},                                     // y
               Decimal128Full{"99999999999999999999999999999990000010", 38, 6});

  AddAndVerify(Decimal128Full{"-09999999999999999999999999999999000000", 38, 5},  // x
               Decimal128Full{"100", 38, 7},                                      // y
               Decimal128Full{"-99999999999999999999999999999989999990", 38, 6});

  AddAndVerify(Decimal128Full{"09999999999999999999999999999999000000", 38, 5},  // x
               Decimal128Full{"-100", 38, 7},                                    // y
               Decimal128Full{"99999999999999999999999999999989999990", 38, 6});

  AddAndVerify(Decimal128Full{"-09999999999999999999999999999999000000", 38, 5},  // x
               Decimal128Full{"-100", 38, 7},                                     // y
               Decimal128Full{"-99999999999999999999999999999990000010", 38, 6});

  AddAndVerify(Decimal128Full{"09999999999999999999999999999999999999", 38, 6},  // x
               Decimal128Full{"89999999999999999999999999999999999999", 38, 7},  // y
               Decimal128Full{"18999999999999999999999999999999999999", 38, 6});

  // Both -ve
  AddAndVerify(Decimal128Full{"-201", 30, 3},    // x
               Decimal128Full{"-301", 30, 2},    // y
               Decimal128Full{"-3211", 32, 3});  // expected

  AddAndVerify(Decimal128Full{"-201", 38, 3},    // x
               Decimal128Full{"-301", 38, 4},    // y
               Decimal128Full{"-2311", 38, 4});  // expected

  // Mix of +ve and -ve
  AddAndVerify(Decimal128Full{"-201", 30, 3},   // x
               Decimal128Full{"301", 30, 2},    // y
               Decimal128Full{"2809", 32, 3});  // expected

  AddAndVerify(Decimal128Full{"-201", 38, 3},    // x
               Decimal128Full{"301", 38, 4},     // y
               Decimal128Full{"-1709", 38, 4});  // expected

  AddAndVerify(Decimal128Full{"201", 38, 3},      // x
               Decimal128Full{"-301", 38, 7},     // y
               Decimal128Full{"200970", 38, 6});  // expected

  AddAndVerify(Decimal128Full{"-1901", 38, 4},  // x
               Decimal128Full{"1801", 38, 4},   // y
               Decimal128Full{"-100", 38, 4});  // expected

  AddAndVerify(Decimal128Full{"1801", 38, 4},   // x
               Decimal128Full{"-1901", 38, 4},  // y
               Decimal128Full{"-100", 38, 4});  // expected

  // rounding +ve
  AddAndVerify(Decimal128Full{"1000999", 38, 6},   // x
               Decimal128Full{"10000999", 38, 7},  // y
               Decimal128Full{"2001099", 38, 6});

  AddAndVerify(Decimal128Full{"1000999", 38, 6},   // x
               Decimal128Full{"10000992", 38, 7},  // y
               Decimal128Full{"2001098", 38, 6});

  // rounding -ve
  AddAndVerify(Decimal128Full{"-1000999", 38, 6},   // x
               Decimal128Full{"-10000999", 38, 7},  // y
               Decimal128Full{"-2001099", 38, 6});

  AddAndVerify(Decimal128Full{"-1000999", 38, 6},   // x
               Decimal128Full{"-10000992", 38, 7},  // y
               Decimal128Full{"-2001098", 38, 6});
#endif
}

}  // namespace gandiva
