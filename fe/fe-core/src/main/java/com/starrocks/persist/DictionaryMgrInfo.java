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

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Dictionary;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

public class DictionaryMgrInfo implements Writable {
    @SerializedName(value = "nextTxnId")
    private long nextTxnId = 1L;
    @SerializedName(value = "nextDictionaryId")
    private long nextDictionaryId = 1L;
    @SerializedName(value = "dictionaries")
    private List<Dictionary> dictionaries = Lists.newArrayList();

    public DictionaryMgrInfo(long nextTxnId, long nextDictionaryId) {
        this.nextTxnId = nextTxnId;
        this.nextDictionaryId = nextDictionaryId;
    }

    public DictionaryMgrInfo(long nextTxnId, long nextDictionaryId, List<Dictionary> dictionaries) {
        this.nextTxnId = nextTxnId;
        this.nextDictionaryId = nextDictionaryId;
        if (dictionaries != null) {
            this.dictionaries = dictionaries;
        }
    }

    public DictionaryMgrInfo() {}

    public long getNextTxnId() {
        return nextTxnId;
    }

    public long getNextDictionaryId() {
        return nextDictionaryId;
    }

    public List<Dictionary> getDictionaries() {
        return this.dictionaries;
    }

    public static DictionaryMgrInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DictionaryMgrInfo.class);
    }


}
