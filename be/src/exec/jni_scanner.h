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

#include "column/chunk.h"
#include "common/logging.h"
#include "common/status.h"
#include "hdfs_scanner.h"
#include "jni.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class JniScanner : public HdfsScanner {
public:
    struct CreateOptions {
        const FSOptions* fs_options = nullptr;
        const HiveTableDescriptor* hive_table = nullptr;
        const THdfsScanRange* scan_range = nullptr;
        const THdfsScanNode* scan_node = nullptr;
    };

    JniScanner(std::string factory_class, std::map<std::string, std::string> params)
            : _jni_scanner_params(std::move(params)), _jni_scanner_factory_class(std::move(factory_class)) {}

    ~JniScanner() override { close(); }

    Status do_open(RuntimeState* runtime_state) override;
    void do_update_counter(HdfsScanProfile* profile) override {}
    void do_close(RuntimeState* runtime_state) noexcept override;
    Status do_get_next(RuntimeState* runtime_state, ChunkPtr* chunk) override;
    Status do_init(RuntimeState* runtime_state, const HdfsScannerParams& scanner_params) override;
    virtual Status update_jni_scanner_params();
    Status reinterpret_status(const Status& st) override { return st; }

protected:
    StatusOr<size_t> fill_empty_chunk(ChunkPtr* chunk);

    Filter _chunk_filter;

private:
    struct FillColumnArgs {
        long num_rows;
        const std::string& slot_name;
        const TypeDescriptor& slot_type;

        uint8_t* nulls;
        Column* column;
        bool must_nullable;
    };

    static Status _check_jni_exception(JNIEnv* env, const std::string& message);

    Status _init_jni_table_scanner(JNIEnv* env, RuntimeState* runtime_state);

    Status _init_jni_method(JNIEnv* env);

    Status _get_next_chunk(JNIEnv* env, long* chunk_meta);

    template <LogicalType type>
    Status _append_primitive_data(const FillColumnArgs& args);

    template <LogicalType type>
    Status _append_string_data(const FillColumnArgs& args);

    Status _append_array_data(const FillColumnArgs& args);
    Status _append_map_data(const FillColumnArgs& args);
    Status _append_struct_data(const FillColumnArgs& args);

    Status _fill_column(FillColumnArgs* args);

    // fill chunk according to slot_desc_list(with or without partition columns)
    StatusOr<size_t> _fill_chunk(JNIEnv* env, ChunkPtr* chunk);

    Status _release_off_heap_table(JNIEnv* env);

    std::string _scanner_type() {
        return _jni_scanner_params.contains("scanner_type") ? _jni_scanner_params["scanner_type"] : "default";
    }

    jclass _jni_scanner_cls = nullptr;
    jobject _jni_scanner_obj = nullptr;
    jmethodID _jni_scanner_open = nullptr;
    jmethodID _jni_scanner_get_next_chunk = nullptr;
    jmethodID _jni_scanner_close = nullptr;
    jmethodID _jni_scanner_release_column = nullptr;
    jmethodID _jni_scanner_release_table = nullptr;

    std::map<std::string, std::string> _jni_scanner_params;
    std::string _jni_scanner_factory_class;

    const std::set<std::string> _skipped_log_jni_scanner_params = {"native_table", "split_info", "predicate_info",
                                                                   "access_id",    "access_key"};

private:
    long* _chunk_meta_ptr;
    int _chunk_meta_index;

    void reset_chunk_meta(long chunk_meta) {
        _chunk_meta_ptr = static_cast<long*>(reinterpret_cast<void*>(chunk_meta));
        _chunk_meta_index = 0;
    }
    void* next_chunk_meta_as_ptr() { return reinterpret_cast<void*>(_chunk_meta_ptr[_chunk_meta_index++]); }
    long next_chunk_meta_as_long() { return _chunk_meta_ptr[_chunk_meta_index++]; }
};

std::unique_ptr<JniScanner> create_paimon_jni_scanner(const JniScanner::CreateOptions& options);
std::unique_ptr<JniScanner> create_hudi_jni_scanner(const JniScanner::CreateOptions& options);
std::unique_ptr<JniScanner> create_odps_jni_scanner(const JniScanner::CreateOptions& options);
std::unique_ptr<JniScanner> create_kudu_jni_scanner(const JniScanner::CreateOptions& options);
std::unique_ptr<JniScanner> create_hive_jni_scanner(const JniScanner::CreateOptions& options);
std::unique_ptr<JniScanner> create_iceberg_metadata_jni_scanner(const JniScanner::CreateOptions& options);

} // namespace starrocks
