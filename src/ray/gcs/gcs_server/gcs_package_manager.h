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
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

class GcsPackageManager : public rpc::PackageInfoHandler {
 public:
  GcsPackageManager(gcs::RedisGcsTableStorage &gcs_table_storage,
                    gcs::GcsPubSub *gcs_pub_sub);
  void HandleReportWorkerFailure(const ReportWorkerFailureRequest &request,
                                 ReportWorkerFailureReply *reply,
                                 SendReplyCallback send_reply_callback) override;

  void HandleGetWorkerInfo(const GetWorkerInfoRequest &request, GetWorkerInfoReply *reply,
                           SendReplyCallback send_reply_callback) override;

  void HandleGetAllWorkerInfo(const GetAllWorkerInfoRequest &request,
                              GetAllWorkerInfoReply *reply,
                              SendReplyCallback send_reply_callback) override;

  void HandleAddWorkerInfo(const AddWorkerInfoRequest &request, AddWorkerInfoReply *reply,
                           SendReplyCallback send_reply_callback) override;

  void LockPackage(const PackageID &package_id);
  void UnlockPackage(const PackageID &package_id);

  ~GcsPackageInfoHandler() override {}

 private:
  gcs::GcsTableStorage *gcs_table_storage_;
  /// A publisher for publishing gcs messages.
  gcs::GcsPubSub *gcs_pub_sub_;
};

}  // namespace gcs
}  // namespace ray
