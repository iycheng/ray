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

#include <cstdio>
#include "boost/interprocess/sync/file_lock.hpp"

namespace ray {
namespace sync {

class FileLock : public boost::interprocess::file_lock {
 public:
  FileLock(const std::string& file_name) {
    // File need to be created first
    auto lock_file = file_name + ".lock";
    auto f = std::fopen(lock_file.c_str(), "a+");
    std::fclose(f);
    boost::interprocess::file_lock other(lock_file.c_str());
    swap(other);
  }
};

}
}
