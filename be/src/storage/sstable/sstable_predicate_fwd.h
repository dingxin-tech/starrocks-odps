// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

namespace starrocks {

namespace sstable {
class SstablePredicate;
using SstablePredicateUPtr = std::unique_ptr<SstablePredicate>;
using SstablePredicateSPtr = std::shared_ptr<SstablePredicate>;

class KeyToChunkConverter;
using KeyToChunkConverterUPtr = std::unique_ptr<KeyToChunkConverter>;

} // namespace sstable

namespace sstable {
using SstablePredicateSPtr = typename starrocks::sstable::SstablePredicateSPtr;
} // namespace sstable

} // namespace starrocks
