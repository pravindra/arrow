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

#pragma once

#include <memory>
#include <string>

#include "arrow/status.h"

#include "gandiva/visibility.h"

namespace gandiva {

class ConfigurationBuilder;
/// \brief runtime config for gandiva
///
/// It contains elements to customize gandiva execution
/// at run time.
class GANDIVA_EXPORT Configuration {
 public:
  enum OptimiseMode {
    // optimise for minimal build time (uses interpreter).
    kMinimalBuildTime,

    kMinimalEvalTime,

    // optimise for minimal eval time (uses JIT with all optimiser passes).
    kMinimalEvalTime
  };

  OptimiseMode optimise_mode() { return optimise_mode_; }

  explicit Configuration(OptimiseMode mode);
  friend class ConfigurationBuilder;

  std::size_t Hash() const;
  bool operator==(const Configuration& other) const;
  bool operator!=(const Configuration& other) const;

 private:
  OptimiseMode optimise_mode_;
};

/// \brief configuration builder for gandiva
///
/// Provides a default configuration and convenience methods
/// to override specific values and build a custom instance
class GANDIVA_EXPORT ConfigurationBuilder {
 public:
  std::shared_ptr<Configuration> build() {
    std::shared_ptr<Configuration> configuration(new Configuration(optimise_mode_));
    return configuration;
  }

  static std::shared_ptr<Configuration> DefaultConfiguration() {
    return default_configuration_;
  }

  void set_optimise_mode(Configuration::OptimiseMode mode) {
    optimise_mode_ = mode;
  }

 private:
  static std::shared_ptr<Configuration> InitDefaultConfig() {
    std::shared_ptr<Configuration> configuration(new Configuration(Configuration::kMinimalEvalTime));
    return configuration;
  }

  static const std::shared_ptr<Configuration> default_configuration_;
  Configuration::OptimiseMode optimise_mode_;
};

}  // namespace gandiva
