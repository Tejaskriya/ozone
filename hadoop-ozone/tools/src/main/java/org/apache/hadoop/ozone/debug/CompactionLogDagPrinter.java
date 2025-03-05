/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import static org.apache.hadoop.ozone.OzoneConsts.COMPACTION_LOG_TABLE;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_LOG_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.DB_COMPACTION_SST_BACKUP_DIR;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIFF_DIR;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.ozone.graph.PrintableGraph;
import org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Handler to generate image for current compaction DAG in the OM leader node.
 * ozone sh snapshot print-log-dag.
 */
@CommandLine.Command(
    name = "print-log-dag",
    aliases = "pld",
    description = "Create an image of the current compaction log DAG.")
@MetaInfServices(DebugSubcommand.class)
public class CompactionLogDagPrinter extends Handler
    implements DebugSubcommand {

  @CommandLine.Option(names = {"-f", "--file-location"},
      required = true,
      description = "Path to location at which image will be downloaded. " +
          "Should include the image file name with \".png\" extension.")
  private String imageLocation;

  @CommandLine.Option(names = {"--db"},
      required = true,
      scope = CommandLine.ScopeType.INHERIT,
      description = "Path to OM RocksDB")
  private String dbPath;

  // TODO: Change graphType to enum.
  @CommandLine.Option(names = {"-t", "--graph-type"},
      description = "Type of node name to use in the graph image. (optional)\n Accepted values are: \n" +
          "  FILE_NAME (default) : to use file name as node name in DAG,\n" +
          "  KEY_SIZE: to show the no. of keys in the file along with file name in the DAG node name,\n" +
          "  CUMULATIVE_SIZE: to show the cumulative size along with file name in the DAG node name.",
      defaultValue = "FILE_NAME")
  private String graphType;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {

    try {
      RocksDBCheckpointDiffer rocksDBCheckpointDiffer;
      final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
      List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(dbPath);
      try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(dbPath, cfDescList, cfHandleList)) {
        ColumnFamilyHandle compactionLogTableCFHandle = RocksDBUtils
            .getColumnFamilyHandle(COMPACTION_LOG_TABLE, cfHandleList);
        rocksDBCheckpointDiffer = RocksDBCheckpointDiffer.RocksDBCheckpointDifferHolder
            .getInstance((Paths.get(dbPath).toFile().getParent() + OM_KEY_PREFIX + OM_SNAPSHOT_DIFF_DIR),
            DB_COMPACTION_SST_BACKUP_DIR, DB_COMPACTION_LOG_DIR, dbPath, new OzoneConfiguration());
        rocksDBCheckpointDiffer.setCompactionLogTableCFHandle(compactionLogTableCFHandle);
        rocksDBCheckpointDiffer.setActiveRocksDB(ManagedRocksDB.openReadOnly(dbPath, cfDescList, cfHandleList));
        rocksDBCheckpointDiffer.loadAllCompactionLogs();
        rocksDBCheckpointDiffer.pngPrintMutableGraph(imageLocation, PrintableGraph.GraphType.valueOf(graphType));
      }
      System.out.println("Graph was generated at '" + imageLocation + "'.");
    } catch (RocksDBException ex) {
      System.err.println("Failed to open given RocksDB: " + ex);
    }
  }
}
