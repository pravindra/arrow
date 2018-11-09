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

#ifndef GANDIVA_DECIMAL_ADD_IR_BUILDER_H
#define GANDIVA_DECIMAL_ADD_IR_BUILDER_H

#include "gandiva/function_ir_builder.h"

namespace gandiva {

// Decimal IR functions
class DecimalIR : public FunctionIRBuilder {
 public:
  DecimalIR(Engine* engine) : FunctionIRBuilder(engine) {}

  static Status AddFunctions(Engine* engine);

 private:
  llvm::Value* ExtractMemberFromPtr(llvm::Value* ptr, LLVMTypes::DecimalMember member);

  llvm::Value* GetScaleMultiplier(llvm::Value* scale);

  llvm::Value* GetHigherScale(llvm::Value* x_scale, llvm::Value* y_scale);

  llvm::Value* IncreaseScale(llvm::Value* in_value, llvm::Value* increase_scale_by);

  llvm::Value* ReduceScale(llvm::Value* in_value, llvm::Value* reduce_scale_by);

  llvm::Value* AddFastPath(llvm::Value* x_value, llvm::Value* x_scale,
                           llvm::Value* y_value, llvm::Value* y_scale);

  llvm::Value* AddAndReduceScale(llvm::Value* x_value, llvm::Value* x_scale,
                                 llvm::Value* y_value, llvm::Value* y_scale,
                                 llvm::Value* out_scale);

  Status BuildAdd();
  static Status MakeAdd(Engine* engine, std::shared_ptr<FunctionIRBuilder>* out);
};

}  // namespace gandiva

#endif  // GANDIVA_FUNCTION_IR_BUILDER_H
