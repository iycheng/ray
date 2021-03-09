#include "ray/gcs/gcs_server/gcs_package_manager.h"

namespace ray {
namespace gcs {

GcsPackageManager::GcsPackageManager(gcs::GcsPubSub *gcs_pub_sub)
    : gcs_pub_sub_(gcs_pub_sub) {}

void GcsPackageManager::IncrPackageReference(const std::string &hex_id,
                                             const rpc::RuntimeEnv &runtime_env) {
  if (!runtime_env.working_dir_uri().empty()) {
    const auto &uri = runtime_env.working_dir_uri();
    package_reference_[uri]++;
    id_to_packages_[hex_id].push_back(uri);
  }
}

void GcsPackageManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &jobs = gcs_init_data.Jobs();
  for (const auto &entry : jobs) {
    if (!entry.second.is_dead()) {
      IncrPackageReference(entry.first.Hex(), entry.second.config().runtime_env());
    }
  }

  for (const auto &entry : gcs_init_data.Actors()) {
    if (entry.second.state() != ray::rpc::ActorTableData::DEAD) {
      IncrPackageReference(entry.first.Hex(), entry.second.runtime_env());
    }
  }
}

void GcsPackageManager::DecrPackageReference(const std::string &hex_id) {
  for (const auto &package_uri : id_to_packages_[hex_id]) {
    --package_reference_[package_uri];
    auto ref_cnt = package_reference_[package_uri];
    RAY_CHECK(ref_cnt >= 0);
    if (ref_cnt == 0) {
      package_reference_.erase(package_uri);
      RAY_CHECK_OK(gcs_pub_sub_->Publish(PACKAGE_CHANNEL, "", package_uri, nullptr));
    }
  }
  id_to_packages_.erase(hex_id);
}

}  // namespace gcs
}  // namespace ray
