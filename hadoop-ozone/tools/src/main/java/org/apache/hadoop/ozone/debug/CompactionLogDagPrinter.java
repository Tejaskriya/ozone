/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import com.google.common.base.Preconditions;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hdds.cli.DebugSubcommand;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.graph.PrintableGraph;
import org.apache.ozone.rocksdiff.CompactionNode;
import org.kohsuke.MetaInfServices;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableProperties;
import picocli.CommandLine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Handler to generate image for current compaction DAG in the OM leader node.
 * ozone sh snapshot print-log-dag.
 */
@CommandLine.Command(
    name = "print-log-dag",
    aliases = "pld",
    description = "Create an image of the current compaction log DAG in OM.")
@MetaInfServices(DebugSubcommand.class)
public class CompactionLogDagPrinter extends Handler
    implements DebugSubcommand {

  @CommandLine.Option(names = {"-f", "--file-name-prefix"},
      description = "Prefix to be use in image file name. (optional)")
  private String fileNamePrefix;

  @CommandLine.Option(names = {"--db"},
      required = true,
      scope = CommandLine.ScopeType.INHERIT,
      description = "Database File Path")
  private String dbPath;

  // TODO: Change graphType to enum.
  @CommandLine.Option(names = {"-t", "--graph-type"},
      description = "Type of node name to use in the graph image. " +
          "(optional)\n Accepted values are: \n" +
          "  file_name (default) : to use file name as node name in DAG,\n" +
          "  key_size: to show the no. of keys in the file along with file " +
          "name in the DAG node name,\n" +
          "  cumulative_size: to show the cumulative size along with file " +
          "name in the DAG node name.",
      defaultValue = "file_name")
  private String graphType;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException {
    try {
      pngPrintMutableGraph(fileNamePrefix, PrintableGraph.GraphType.valueOf(graphType));
      System.out.println("Created graph png");
    } catch (RocksDBException ex) {
      throw new IOException(ex);
    }

  }

  private final MutableGraph<CompactionNode> backwardCompactionDAG =
      GraphBuilder.directed().build();

  // Hash table to track CompactionNode for a given SST File.
  private final ConcurrentHashMap<String, CompactionNode> compactionNodeMap =
      new ConcurrentHashMap<>();

  private long reconstructionSnapshotCreationTime;
  private String reconstructionCompactionReason;
  private static final String COMPACTION_LOG_COMMENT_LINE_PREFIX = "# ";
  private static final String COMPACTION_LOG_SEQ_NUM_LINE_PREFIX = "S ";
  private static final String COMPACTION_LOG_ENTRY_LINE_PREFIX = "C ";
  private static final String SPACE_DELIMITER = " ";
  private static final String COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER = ":";
  private static final String COMPACTION_LOG_ENTRY_FILE_DELIMITER = ",";
  public static final String COMPACTION_LOG_FILE_NAME_SUFFIX = ".log";
  private static final int LONG_MAX_STR_LEN = String.valueOf(Long.MAX_VALUE).length();

  private ColumnFamilyHandle compactionLogTableCFHandle;
  private ManagedRocksDB activeRocksDB;

  public void pngPrintMutableGraph(String filePath, PrintableGraph.GraphType gType)
      throws IOException, RocksDBException {
    Objects.requireNonNull(filePath, "Image file path is required.");
    Objects.requireNonNull(gType, "Graph type is required.");

    loadAllCompactionLogs();

    PrintableGraph graph;
    synchronized (this) {
      graph = new PrintableGraph(backwardCompactionDAG, gType);
    }

    graph.generateImage(filePath);
  }

  public void loadAllCompactionLogs() throws RocksDBException {
    synchronized (this) {
      // preconditionChecksForLoadAllCompactionLogs();
      addEntriesFromLogFilesToDagAndCompactionLogTable();
      final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
      compactionLogTableCFHandle = cfHandleList.get(4);
      List<ColumnFamilyDescriptor> cfDescList =  RocksDBUtils.getColumnFamilyDescriptors(dbPath);
      activeRocksDB = ManagedRocksDB.openReadOnly(dbPath, cfDescList, cfHandleList);
      try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
          activeRocksDB.get().newIterator(compactionLogTableCFHandle))) {
        managedRocksIterator.get().seekToFirst();
        while (managedRocksIterator.get().isValid()) {
          byte[] value = managedRocksIterator.get().value();
          CompactionLogEntry compactionLogEntry =
              CompactionLogEntry.getFromProtobuf(
                  HddsProtos.CompactionLogEntryProto.parseFrom(value));
          populateCompactionDAG(compactionLogEntry.getInputFileInfoList(),
              compactionLogEntry.getOutputFileInfoList(),
              compactionLogEntry.getDbSequenceNumber());
          managedRocksIterator.get().next();
        }
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
  public void addEntriesFromLogFilesToDagAndCompactionLogTable() {
    synchronized (this) {
      reconstructionSnapshotCreationTime = 0L;
      reconstructionCompactionReason = null;
      try {
        try (Stream<Path> pathStream = Files.list(Paths.get("compaction-log"))
            .filter(e -> e.toString().toLowerCase()
                .endsWith(COMPACTION_LOG_FILE_NAME_SUFFIX))
            .sorted()) {
          for (Path logPath : pathStream.collect(Collectors.toList())) {
            readCompactionLogFile(logPath.toString());
            // Delete the file once entries are added to compaction table
            // so that on next restart, only compaction log table is used.
            Files.delete(logPath);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Error listing compaction log dir " +
            "compaction-log", e);
      }
    }
  }

  private void readCompactionLogFile(String currCompactionLogPath) {
    LOG.debug("Loading compaction log: {}", currCompactionLogPath);
    try (Stream<String> logLineStream =
             Files.lines(Paths.get(currCompactionLogPath), UTF_8)) {
      logLineStream.forEach(this::processCompactionLogLine);
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }
  }

  void processCompactionLogLine(String line) {

    LOG.debug("Processing line: {}", line);

    synchronized (this) {
      if (line.startsWith(COMPACTION_LOG_COMMENT_LINE_PREFIX)) {
        reconstructionCompactionReason =
            line.substring(COMPACTION_LOG_COMMENT_LINE_PREFIX.length());
      } else if (line.startsWith(COMPACTION_LOG_SEQ_NUM_LINE_PREFIX)) {
        reconstructionSnapshotCreationTime =
            getSnapshotCreationTimeFromLogLine(line);
      } else if (line.startsWith(COMPACTION_LOG_ENTRY_LINE_PREFIX)) {
        // Compaction log entry is like following:
        // C sequence_number input_files:output_files
        // where input_files and output_files are joined by ','.
        String[] lineSpilt = line.split(SPACE_DELIMITER);
        if (lineSpilt.length != 3) {
          LOG.error("Invalid line in compaction log: {}", line);
          return;
        }

        String dbSequenceNumber = lineSpilt[1];
        String[] io = lineSpilt[2]
            .split(COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER);

        if (io.length != 2) {
          if (line.endsWith(":")) {
            LOG.debug("Ignoring compaction log line for SST deletion");
          } else {
            LOG.error("Invalid line in compaction log: {}", line);
          }
          return;
        }

        String[] inputFiles = io[0].split(COMPACTION_LOG_ENTRY_FILE_DELIMITER);
        String[] outputFiles = io[1].split(COMPACTION_LOG_ENTRY_FILE_DELIMITER);
        addFileInfoToCompactionLogTable(Long.parseLong(dbSequenceNumber),
            reconstructionSnapshotCreationTime, inputFiles, outputFiles,
            reconstructionCompactionReason);
      } else {
        LOG.error("Invalid line in compaction log: {}", line);
      }
    }
  }

  private void addFileInfoToCompactionLogTable(
      long dbSequenceNumber,
      long creationTime,
      String[] inputFiles,
      String[] outputFiles,
      String compactionReason
  ) {
    List<CompactionFileInfo> inputFileInfoList = Arrays.stream(inputFiles)
        .map(inputFile -> new CompactionFileInfo.Builder(inputFile).build())
        .collect(Collectors.toList());
    List<CompactionFileInfo> outputFileInfoList = Arrays.stream(outputFiles)
        .map(outputFile -> new CompactionFileInfo.Builder(outputFile).build())
        .collect(Collectors.toList());

    CompactionLogEntry.Builder builder =
        new CompactionLogEntry.Builder(dbSequenceNumber, creationTime,
            inputFileInfoList, outputFileInfoList);
    if (compactionReason != null) {
      builder.setCompactionReason(compactionReason);
    }

    addToCompactionLogTable(builder.build());
  }

  void addToCompactionLogTable(CompactionLogEntry compactionLogEntry) {
    String dbSequenceIdStr =
        String.valueOf(compactionLogEntry.getDbSequenceNumber());

    if (dbSequenceIdStr.length() < LONG_MAX_STR_LEN) {
      // Pad zeroes to the left to make sure it is lexicographic ordering.
      dbSequenceIdStr = org.apache.commons.lang3.StringUtils.leftPad(
          dbSequenceIdStr, LONG_MAX_STR_LEN, "0");
    }

    // Key in the transactionId-currentTime
    // Just trxId can't be used because multiple compaction might be
    // running, and it is possible no new entry was added to DB.
    // Adding current time to transactionId eliminates key collision.
    String keyString = dbSequenceIdStr + "-" +
        compactionLogEntry.getCompactionTime();

    byte[] key = keyString.getBytes(UTF_8);
    byte[] value = compactionLogEntry.getProtobuf().toByteArray();
    try {
      activeRocksDB.get().put(compactionLogTableCFHandle, key, value);
    } catch (RocksDBException exception) {
      // TODO: Revisit exception handling before merging the PR.
      throw new RuntimeException(exception);
    }
  }

  /**
   * Populate the compaction DAG with input and output SST files lists.
   * @param inputFiles List of compaction input files.
   * @param outputFiles List of compaction output files.
   * @param seqNum DB transaction sequence number.
   */
  private void populateCompactionDAG(List<CompactionFileInfo> inputFiles,
                                     List<CompactionFileInfo> outputFiles,
                                     long seqNum) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input files: {} -> Output files: {}", inputFiles, outputFiles);
    }

    for (CompactionFileInfo outfile : outputFiles) {
      final CompactionNode outfileNode = compactionNodeMap.computeIfAbsent(
          outfile.getFileName(),

          file -> addNodeToDAG(file, seqNum, outfile.getStartKey(),
              outfile.getEndKey(), outfile.getColumnFamily()));


      for (CompactionFileInfo infile : inputFiles) {
        final CompactionNode infileNode = compactionNodeMap.computeIfAbsent(
            infile.getFileName(),

            file -> addNodeToDAG(file, seqNum, infile.getStartKey(),
                infile.getEndKey(), infile.getColumnFamily()));

        // Draw the edges
        if (!outfileNode.getFileName().equals(infileNode.getFileName())) {
          backwardCompactionDAG.putEdge(infileNode, outfileNode);
        }
      }
    }
  }

  private CompactionNode addNodeToDAG(String file, long seqNum, String startKey,
                                      String endKey, String columnFamily) {
    long numKeys = 0L;
    try {
      numKeys = getSSTFileSummary(file);
    } catch (RocksDBException e) {
      LOG.warn("Can't get num of keys in SST '{}': {}", file, e.getMessage());
    } catch (FileNotFoundException e) {
      LOG.info("Can't find SST '{}'", file);
    }

    CompactionNode fileNode = new CompactionNode(file, numKeys,
        seqNum, startKey, endKey, columnFamily);

    backwardCompactionDAG.addNode(fileNode);

    return fileNode;
  }

  private long getSSTFileSummary(String filename)
      throws RocksDBException, FileNotFoundException {

    if (!filename.endsWith(".sst")) {
      filename += ".sst";
    }

    try (ManagedOptions option = new ManagedOptions();
         ManagedSstFileReader reader = new ManagedSstFileReader(option)) {

      reader.open(dbPath + "/" + filename);

      TableProperties properties = reader.getTableProperties();
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} has {} keys", filename, properties.getNumEntries());
      }
      return properties.getNumEntries();
    }
  }

  private long getSnapshotCreationTimeFromLogLine(String logLine) {
    // Remove `S ` from the line.
    String line = logLine.substring(COMPACTION_LOG_SEQ_NUM_LINE_PREFIX.length());

    String[] splits = line.split(SPACE_DELIMITER);
    Preconditions.checkArgument(splits.length == 3,
        "Snapshot info log statement has more than expected parameters.");

    return Long.parseLong(splits[2]);
  }

}
