#include "ray/rpc/gcs_server/gcs_package_manager.h"

namespace ray {
namespace gcs {

GcsPackageManager::GcsPackageManager(gcs::RedisGcsTableStorage &gcs_table_storage,
                                     gcs::GcsPubSub *gcs_pub_sub) {}
void GcsPackageManager::HandleReportWorkerFailure(
    const ReportWorkerFailureRequest &request, ReportWorkerFailureReply *reply,
    SendReplyCallback send_reply_callback) {}

void GcsPackageManager::HandleGetWorkerInfo(const GetWorkerInfoRequest &request,
                                            GetWorkerInfoReply *reply,
                                            SendReplyCallback send_reply_callback) {}

void GcsPackageManager::HandleGetAllWorkerInfo(const GetAllWorkerInfoRequest &request,
                                               GetAllWorkerInfoReply *reply,
                                               SendReplyCallback send_reply_callback) {}

void GcsPackageManager::HandleAddWorkerInfo(const AddWorkerInfoRequest &request,
                                            AddWorkerInfoReply *reply,
                                            SendReplyCallback send_reply_callback) {}

void GcsPackageManager::LockPackage(const PackageID &package_id){};
void GcsPackageManager::UnlockPackage(const PackageID &package_id){};

}  // namespace gcs
}  // namespace ray
