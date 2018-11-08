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

#include "arrow/status.h"
#include "gandiva/decimal_ir.h"

static __int128 scale_multipliers_[]{1, 10, 100};

namespace gandiva {

//
// Equivalent to :
//   x <= y ? y : x
llvm::Value* DecimalIR::GetHigherScale(llvm::Value* x_scale, llvm::Value* y_scale) {
  llvm::Value* le = ir_builder()->CreateICmpSLE(x_scale, y_scale);
  return BuildIfElse(le, types()->i32_type(), [&] { return y_scale; },
                     [&] { return x_scale; });
}

//
// Equivalent to :
//   return (delta_scale <= 0)  ? in_value : in_value * scale_multipliers_[delta_scale]
//
llvm::Value* DecimalIR::AdjustToHigherScale128(llvm::Value* in_value,
                                               llvm::Value* delta_scale) {
  llvm::Value* le_zero =
      ir_builder()->CreateICmpSLE(delta_scale, types()->i32_constant(0));
  // then block
  auto then_lambda = [&] { return in_value; };

  // else block
  auto else_lambda = [&] {
    // Equivalent to :
    //   multiplier = scale_multipliers_[delta_scale]
    auto ptr_int_cast =
        types()->i64_constant(reinterpret_cast<int64_t>(scale_multipliers_));
    auto scale_array_ptr =
        llvm::ConstantExpr::getIntToPtr(ptr_int_cast, types()->i128_ptr_type());
    auto ptr = ir_builder()->CreateGEP(scale_array_ptr, delta_scale);
    auto multiplier = ir_builder()->CreateLoad(ptr);

    // Equivalent to :
    //   return in * multiplier
    return ir_builder()->CreateMul(in_value, multiplier);
  };

  return BuildIfElse(le_zero, types()->i128_type(), then_lambda, else_lambda);
}

llvm::Value* DecimalIR::AddFastPath(llvm::Value* x_value, llvm::Value* x_scale,
                                    llvm::Value* y_value, llvm::Value* y_scale) {
  auto higher_scale = GetHigherScale(x_scale, y_scale);

  // Equivalent to :
  //   x_scaled = AdjustToHigherScale(x_value, higher_scale - x_scale)
  auto x_delta = ir_builder()->CreateSub(higher_scale, x_scale);
  auto x_scaled = AdjustToHigherScale128(x_value, x_delta);

  // Equivalent to :
  //   y_scaled = AdjustToHigherScale(y_value, higher_scale - y_scale)
  auto y_delta = ir_builder()->CreateSub(higher_scale, y_scale);
  auto y_scaled = AdjustToHigherScale128(y_value, y_delta);

  return ir_builder()->CreateAdd(x_scaled, y_scaled);
}

llvm::Value* DecimalIR::ExtractMemberFromPtr(llvm::Value* ptr,
                                             LLVMTypes::DecimalMember member) {
  auto member_ptr =
      ir_builder()->CreateGEP(types()->decimal128_struct_type(), ptr,
                              {types()->i32_constant(0), types()->i32_constant(member)});
  return ir_builder()->CreateLoad(member_ptr, "member");
}

Status DecimalIR::BuildAdd() {
  // Create fn prototype :
  //   void add_decimal128_decimal128(decimal128 *in1, decimal128 *in2, decimal128 *out)
  std::vector<llvm::Type*> arguments;
  arguments.push_back(types()->decimal128_ptr_type());
  arguments.push_back(types()->decimal128_ptr_type());
  arguments.push_back(types()->decimal128_ptr_type());
  llvm::FunctionType* prototype =
      llvm::FunctionType::get(types()->void_type(), arguments, false /*isVarArg*/);

  // Create fn
  std::string function_name = "add_decimal128_decimal128";
  engine_->AddFunctionToCompile(function_name);
  function_ = llvm::Function::Create(prototype, llvm::GlobalValue::ExternalLinkage,
                                     function_name, module());
  ARROW_RETURN_FAILURE_IF_FALSE((function_ != nullptr),
                                Status::CodeGenError("Error creating function."));
  // Name the arguments
  llvm::Function::arg_iterator args = function_->arg_begin();
  llvm::Value* arg_x = &*args;
  arg_x->setName("x");
  ++args;
  llvm::Value* arg_y = &*args;
  arg_y->setName("y");
  ++args;
  llvm::Value* arg_out = &*args;
  arg_out->setName("out");
  ++args;

  llvm::BasicBlock* entry = llvm::BasicBlock::Create(*context(), "entry", function_);
  llvm::BasicBlock* exit = llvm::BasicBlock::Create(*context(), "exit", function_);

  ir_builder()->SetInsertPoint(entry);

  // extract members
  auto x_value = ExtractMemberFromPtr(arg_x, LLVMTypes::kDecimalValueIdx);
  auto x_scale = ExtractMemberFromPtr(arg_x, LLVMTypes::kDecimalScaleIdx);
  auto y_value = ExtractMemberFromPtr(arg_y, LLVMTypes::kDecimalValueIdx);
  auto y_scale = ExtractMemberFromPtr(arg_y, LLVMTypes::kDecimalScaleIdx);

  // fast-path add
  auto value = AddFastPath(x_value, x_scale, y_value, y_scale);

  // store result to out
  auto out_value_ptr = ir_builder()->CreateGEP(
      types()->decimal128_struct_type(), arg_out,
      {types()->i32_constant(0), types()->i32_constant(LLVMTypes::kDecimalValueIdx)});
  ir_builder()->CreateStore(value, out_value_ptr);
  ir_builder()->CreateBr(exit);

  ir_builder()->SetInsertPoint(exit);
  ir_builder()->CreateRetVoid();
  return Status::OK();
}

Status DecimalIR::MakeAdd(Engine* engine, std::shared_ptr<FunctionIRBuilder>* out) {
  auto add_builder = std::make_shared<DecimalIR>(engine);
  auto status = add_builder->BuildAdd();
  ARROW_RETURN_NOT_OK(status);

  *out = add_builder;
  return Status::OK();
}

}  // namespace gandiva
