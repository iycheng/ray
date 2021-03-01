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

#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

class GcsPackageManager : public rpc::PackageInfoHandler {
 public:
  GcsPackageManager(gcs::GcsPackageTable *gcs_package_table,
                    gcs::GcsCodeStorageTable *gcs_code_storage_table,
                    gcs::GcsPubSub *gcs_pub_sub);

  void HandleGetPackageInfo(const rpc::GetPackageInfoRequest &request,
                            rpc::GetPackageInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;
  void HandleFetchPackage(const rpc::FetchPackageRequest &request,
                          rpc::FetchPackageReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;
  void HandlePushPackage(const rpc::PushPackageRequest &request,
                         rpc::PushPackageReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  void LockPackage(const PackageID &package_id);
  void UnlockPackage(const PackageID &package_id);

  ~GcsPackageManager() override {}

 private:
  gcs::GcsPackageTable *gcs_package_table_;
  gcs::GcsCodeStorageTable *gcs_code_storage_table_;
  /// A publisher for publishing gcs messages.
  gcs::GcsPubSub *gcs_pub_sub_;
  std::unordered_map<PackageID, uint32_t> reference_count_;
};

}  // namespace gcs
}  // namespace ray
