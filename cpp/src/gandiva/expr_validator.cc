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
#include <string>
#include <vector>

#include "gandiva/decimal_type_util.h"
#include "gandiva/expr_validator.h"

namespace gandiva {

Status ExprValidator::Validate(const ExpressionPtr& expr) {
  ARROW_RETURN_IF(expr == nullptr,
                  Status::ExpressionValidationError("Expression cannot be null"));

  Node& root = *expr->root();
  ARROW_RETURN_NOT_OK(root.Accept(*this));

  // Ensure root's return type match the expression return type. Type
  // support validation is not required because root type is already supported.
  ARROW_RETURN_IF(!root.return_type()->Equals(*expr->result()->type()),
                  Status::ExpressionValidationError("Return type of root node ",
                                                    root.return_type()->name(),
                                                    " does not match that of expression ",
                                                    expr->result()->type()->name()));

  return Status::OK();
}

#define TYPE_VISITOR_DEFAULT(TYPE_CLASS) \
  Status Visit(const TYPE_CLASS& type) { return VisitDefault(type); }

class ValidateTypeVisitor : public arrow::TypeVisitor {
 public:
  ValidateTypeVisitor(LLVMTypes* types) : types_(types) {}

  Status VisitDefault(const arrow::DataType& type) {
    auto llvm_type = types_->IRType(type.id());
    ARROW_RETURN_IF(llvm_type == nullptr, Status::ExpressionValidationError(
                                              "unsupported data type ", type.ToString()));
    return Status::OK();
  }

  TYPE_VISITOR_DEFAULT(arrow::NullType)
  TYPE_VISITOR_DEFAULT(arrow::BooleanType)
  TYPE_VISITOR_DEFAULT(arrow::Int8Type)
  TYPE_VISITOR_DEFAULT(arrow::Int16Type)
  TYPE_VISITOR_DEFAULT(arrow::Int32Type)
  TYPE_VISITOR_DEFAULT(arrow::Int64Type)
  TYPE_VISITOR_DEFAULT(arrow::UInt8Type)
  TYPE_VISITOR_DEFAULT(arrow::UInt16Type)
  TYPE_VISITOR_DEFAULT(arrow::UInt32Type)
  TYPE_VISITOR_DEFAULT(arrow::UInt64Type)
  TYPE_VISITOR_DEFAULT(arrow::HalfFloatType)
  TYPE_VISITOR_DEFAULT(arrow::FloatType)
  TYPE_VISITOR_DEFAULT(arrow::DoubleType)
  TYPE_VISITOR_DEFAULT(arrow::StringType)
  TYPE_VISITOR_DEFAULT(arrow::BinaryType)
  TYPE_VISITOR_DEFAULT(arrow::FixedSizeBinaryType)
  TYPE_VISITOR_DEFAULT(arrow::Date64Type)
  TYPE_VISITOR_DEFAULT(arrow::Date32Type)
  TYPE_VISITOR_DEFAULT(arrow::Time32Type)
  TYPE_VISITOR_DEFAULT(arrow::Time64Type)
  TYPE_VISITOR_DEFAULT(arrow::TimestampType)
  TYPE_VISITOR_DEFAULT(arrow::IntervalType)
  TYPE_VISITOR_DEFAULT(arrow::ListType)
  TYPE_VISITOR_DEFAULT(arrow::StructType)
  TYPE_VISITOR_DEFAULT(arrow::UnionType)
  TYPE_VISITOR_DEFAULT(arrow::DictionaryType)

  Status Visit(const arrow::Decimal128Type& type) {
    ARROW_RETURN_NOT_OK(VisitDefault(type));

    auto status = DecimalTypeUtil::IsValid(type);
    return status.ok() ? status : Status::ExpressionValidationError(status.message());
  }

 private:
  LLVMTypes* types_;
};

#undef TYPE_VISITOR_DEFAULT

Status ExprValidator::Visit(const FieldNode& node) {
  // Validate the field type.
  ValidateTypeVisitor type_validator(types_);
  ARROW_RETURN_NOT_OK(node.field()->type()->Accept(&type_validator));

  // Ensure that field is found in schema
  auto field_in_schema_entry = field_map_.find(node.field()->name());
  ARROW_RETURN_IF(field_in_schema_entry == field_map_.end(),
                  Status::ExpressionValidationError("Field ", node.field()->name(),
                                                    " not in schema."));

  // Ensure that that the found field match.
  FieldPtr field_in_schema = field_in_schema_entry->second;
  ARROW_RETURN_IF(!field_in_schema->Equals(node.field()),
                  Status::ExpressionValidationError(
                      "Field definition in schema ", field_in_schema->ToString(),
                      " different from field in expression ", node.field()->ToString()));

  return Status::OK();
}

Status ExprValidator::Visit(const FunctionNode& node) {
  auto desc = node.descriptor();
  FunctionSignature signature(desc->name(), desc->params(), desc->return_type());

  const NativeFunction* native_function = registry_.LookupSignature(signature);
  ARROW_RETURN_IF(native_function == nullptr,
                  Status::ExpressionValidationError("Function ", signature.ToString(),
                                                    " not supported yet. "));

  for (auto& child : node.children()) {
    ARROW_RETURN_NOT_OK(child->Accept(*this));
  }

  return Status::OK();
}

Status ExprValidator::Visit(const IfNode& node) {
  ARROW_RETURN_NOT_OK(node.condition()->Accept(*this));
  ARROW_RETURN_NOT_OK(node.then_node()->Accept(*this));
  ARROW_RETURN_NOT_OK(node.else_node()->Accept(*this));

  auto if_node_ret_type = node.return_type();
  auto then_node_ret_type = node.then_node()->return_type();
  auto else_node_ret_type = node.else_node()->return_type();

  // Then-branch return type must match.
  ARROW_RETURN_IF(!if_node_ret_type->Equals(*then_node_ret_type),
                  Status::ExpressionValidationError(
                      "Return type of if ", if_node_ret_type->ToString(), " and then ",
                      then_node_ret_type->ToString(), " not matching."));

  // Else-branch return type must match.
  ARROW_RETURN_IF(!if_node_ret_type->Equals(*else_node_ret_type),
                  Status::ExpressionValidationError(
                      "Return type of if ", if_node_ret_type->ToString(), " and else ",
                      else_node_ret_type->ToString(), " not matching."));

  return Status::OK();
}

Status ExprValidator::Visit(const LiteralNode& node) {
  auto llvm_type = types_->IRType(node.return_type()->id());
  ARROW_RETURN_IF(llvm_type == nullptr,
                  Status::ExpressionValidationError("Value ", node.holder(),
                                                    " has unsupported data type ",
                                                    node.return_type()->name()));

  return Status::OK();
}

Status ExprValidator::Visit(const BooleanNode& node) {
  ARROW_RETURN_IF(
      node.children().size() < 2,
      Status::ExpressionValidationError("Boolean expression has ", node.children().size(),
                                        " children, expected atleast two"));

  for (auto& child : node.children()) {
    const auto bool_type = arrow::boolean();
    const auto ret_type = child->return_type();

    ARROW_RETURN_IF(!ret_type->Equals(bool_type),
                    Status::ExpressionValidationError(
                        "Boolean expression has a child with return type ",
                        ret_type->ToString(), ", expected return type boolean"));

    ARROW_RETURN_NOT_OK(child->Accept(*this));
  }

  return Status::OK();
}

/*
 * Validate the following
 *
 * 1. Non empty list of constants to search in.
 * 2. Expression returns of the same type as the constants.
 */
Status ExprValidator::Visit(const InExpressionNode<int32_t>& node) {
  return ValidateInExpression(node.values().size(), node.eval_expr()->return_type(),
                              arrow::int32());
}

Status ExprValidator::Visit(const InExpressionNode<int64_t>& node) {
  return ValidateInExpression(node.values().size(), node.eval_expr()->return_type(),
                              arrow::int64());
}

Status ExprValidator::Visit(const InExpressionNode<std::string>& node) {
  return ValidateInExpression(node.values().size(), node.eval_expr()->return_type(),
                              arrow::utf8());
}

Status ExprValidator::ValidateInExpression(size_t number_of_values,
                                           DataTypePtr in_expr_return_type,
                                           DataTypePtr type_of_values) {
  ARROW_RETURN_IF(number_of_values == 0,
                  Status::ExpressionValidationError(
                      "IN Expression needs a non-empty constant list to match."));
  ARROW_RETURN_IF(!in_expr_return_type->Equals(type_of_values),
                  Status::ExpressionValidationError(
                      "Evaluation expression for IN clause returns ", in_expr_return_type,
                      " values are of type", type_of_values));

  return Status::OK();
}

}  // namespace gandiva
