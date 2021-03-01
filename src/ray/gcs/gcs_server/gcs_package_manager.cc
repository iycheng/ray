#include "ray/gcs/gcs_server/gcs_package_manager.h"

namespace ray {
namespace gcs {

GcsPackageManager::GcsPackageManager(gcs::GcsPackageTable *gcs_package_table,
                                     gcs::GcsCodeStorageTable *gcs_code_storage_table,
                                     gcs::GcsPubSub *gcs_pub_sub)
    : gcs_package_table_(gcs_package_table),
      gcs_code_storage_table_(gcs_code_storage_table),
      gcs_pub_sub_(gcs_pub_sub) {}

void GcsPackageManager::HandleGetPackageInfo(const rpc::GetPackageInfoRequest &request,
                                             rpc::GetPackageInfoReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  auto id = PackageID::FromBinary(request.package_id());
  RAY_CHECK_OK(gcs_package_table_->Get(
      id, [reply, send_reply_callback](Status status,
                                       const boost::optional<PackageTableData> &data) {
        if (data) {
          reply->mutable_package_info()->CopyFrom(*data);
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      }));
}
void GcsPackageManager::HandleFetchPackage(const rpc::FetchPackageRequest &request,
                                           rpc::FetchPackageReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  auto id = PackageID::FromBinary(request.package_id());
  RAY_CHECK_OK(gcs_code_storage_table_->Get(
      id, [reply, send_reply_callback](
              Status status, const boost::optional<CodeStorageTableData> &data) {
        if (data) {
          reply->set_package_data(data->data());
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      }));
}

void GcsPackageManager::HandlePushPackage(const rpc::PushPackageRequest &request,
                                          rpc::PushPackageReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  auto package_id = PackageID::FromBinary(request.package_id());
  PackageTableData meta_data;

  meta_data.set_skip_gc(request.skip_gc());
  meta_data.set_reference_count(0);
  meta_data.set_uri(request.uri());

  RAY_CHECK_OK(gcs_code_storage_table_->Put(
      package_id, request.code(),
      [this, package_id, reply, send_reply_callback, meta_data](Status status) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Put package into repo failed: " << package_id;
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        } else {
          RAY_CHECK_OK(gcs_package_table_->Put(
              package_id, meta_data, [reply, send_reply_callback](Status status) {
                GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
              }));
        }
      }));
}

void GcsPackageManager::LockPackage(const PackageID &package_id){};
void GcsPackageManager::UnlockPackage(const PackageID &package_id){};

}  // namespace gcs
}  // namespace ray
