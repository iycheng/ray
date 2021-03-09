// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {
namespace gcs {

class GcsPackageManager {
 public:
  explicit GcsPackageManager(gcs::GcsPubSub *gcs_pub_sub);

  void IncrPackageReference(const std::string &hex_id,
                            const rpc::RuntimeEnv &runtime_env);
  void DecrPackageReference(const std::string &hex_id);
  void Initialize(const GcsInitData &gcs_init_data);

 private:
  /// A publisher for publishing gcs messages.
  gcs::GcsPubSub *gcs_pub_sub_;
  std::unordered_map<std::string, int64_t> package_reference_;
  std::unordered_map<std::string, std::vector<std::string>> id_to_packages_;
};

}  // namespace gcs
}  // namespace ray
