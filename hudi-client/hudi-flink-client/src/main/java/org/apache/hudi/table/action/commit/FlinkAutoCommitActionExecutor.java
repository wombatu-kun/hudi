/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.List;

/**
 * Simpler Wrapper to assist with auto commiting with action executor layer.
 * Only PartitionTTL and DeletePartition action executors are leveraging this at the moment.
 */
public class FlinkAutoCommitActionExecutor {
  private final BaseFlinkCommitActionExecutor baseFlinkCommitActionExecutor;

  public FlinkAutoCommitActionExecutor(BaseFlinkCommitActionExecutor baseFlinkCommitActionExecutor) {
    this.baseFlinkCommitActionExecutor = baseFlinkCommitActionExecutor;
  }

  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    HoodieWriteMetadata<List<WriteStatus>> result = (HoodieWriteMetadata<List<WriteStatus>>) baseFlinkCommitActionExecutor.execute();
    baseFlinkCommitActionExecutor.completeCommit(result);
    return result;
  }
}
