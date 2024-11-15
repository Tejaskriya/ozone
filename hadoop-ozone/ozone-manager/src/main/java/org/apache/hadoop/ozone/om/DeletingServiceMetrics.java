/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics related to the OM Deletion services.
 */
@Metrics(about = "Deletion Service Metrics", context = OzoneConsts.OZONE)
public final class DeletingServiceMetrics {

  public static final String METRICS_SOURCE_NAME =
      DeletingServiceMetrics.class.getSimpleName();
  private MetricsRegistry registry;

  private DeletingServiceMetrics() {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
  }

  /**
   * Creates and returns DeletingServiceMetrics instance.
   *
   * @return DeletingServiceMetrics
   */
  public static DeletingServiceMetrics create() {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "Metrics tracking the progress of deletion of directories and keys in the OM",
        new DeletingServiceMetrics());
  }
  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }


  /*
   * Total directory deletion metrics across all iterations of DirectoryDeletingService since last restart.
   */
  @Metric("Total no. of deleted directories sent for purge")
  private MutableGaugeLong numDirsSentForPurge;
  @Metric("Total no. of sub-directories sent for purge")
  private MutableGaugeLong numSubDirsSentForPurge;
  @Metric("Total no. of sub-files sent for purge")
  private MutableGaugeLong numSubFilesSentForPurge;

  public void incrNumDirDeleted(long dirDel) {
    numDirsSentForPurge.incr(dirDel);
  }

  public void incrNumDirsMoved(long dirMove) {
    numSubDirsSentForPurge.incr(dirMove);
  }

  public void incrNumFilesMoved(long filesMove) {
    numSubFilesSentForPurge.incr(filesMove);
  }

  public void incrementDirectoryDeletionTotalMetrics(long dirDel, long dirMove, long filesMove) {
    incrNumDirDeleted(dirDel);
    incrNumDirsMoved(dirMove);
    incrNumFilesMoved(filesMove);
  }

  /*
   * Directory deletion metrics in the latest iteration of DirectoryDeletingService.
   */
  @Metric("Iteration run count of DirectoryDeletingService")
  private MutableGaugeLong iterationDirRunCount;
  @Metric("Iteration start time of DirectoryDeletingService")
  private MutableGaugeLong iterationDirStartTime;
  @Metric("Total time taken by the last iteration of DirectoryDeletingService")
  private MutableGaugeLong iterationDirDuration;
  @Metric("No. of directories deleted in last iteration")
  private MutableGaugeLong iterationDirsDeleted;
  @Metric("No. of sub-directories deleted in last iteration")
  private MutableGaugeLong iterationSubDirsDeleted;
  @Metric("No. of sub-directories sent for purge in last iteration")
  private MutableGaugeLong iterationSubDirsSentForPurge;
  @Metric("No. of files sent for purge in last iteration")
  private MutableGaugeLong iterationFilesSentForPurge;

  public void setIterationDirRunCount(long runcount) {
    iterationDirRunCount.set(runcount);
  }

  public void setIterationDirStartTime(long startTime) {
    iterationDirStartTime.set(startTime);
  }

  public void setIterationDirDuration(long duration) {
    iterationDirDuration.set(duration);
  }

  public void setIterationDirDeleted(long dirDel) {
    iterationDirsDeleted.set(dirDel);
  }

  public void setIterationSubDirDeleted(long subdirDel) {
    iterationSubDirsDeleted.set(subdirDel);
  }

  public void setIterationFilesSentForPurge(long filesMove) {
    iterationFilesSentForPurge.set(filesMove);
  }

  public void setIterationSubDirsSentForPurge(long subdirMove) {
    iterationSubDirsSentForPurge.set(subdirMove);
  }

  public void setDirectoryDeletionIterationMetrics(long runcount, long startTime, long duration,
                                                   long dirDel, long subdirDel,
                                                   long filesMove, long subdirMove) {
    setIterationDirRunCount(runcount);
    setIterationDirStartTime(startTime);
    setIterationDirDuration(duration);
    setIterationDirDeleted(dirDel);
    setIterationSubDirDeleted(subdirDel);
    setIterationFilesSentForPurge(filesMove);
    setIterationSubDirsSentForPurge(subdirMove);
  }

  /*
   * Total key deletion metrics across all iterations of KeyDeletingService since last restart.
   */
  @Metric("Total no. of keys processed")
  private MutableGaugeLong numKeysProcessed;
  @Metric("Total no. of keys sent to scm for deletion")
  private MutableGaugeLong numKeysDeletionRequest;
  @Metric("Total no. of keys deleted successfully")
  private MutableGaugeLong numKeysDeleteSuccess;
  @Metric("Total no. of deleted keys sent for purge")
  private MutableGaugeLong numKeysSentForPurge;

  public void incrNumKeysProcessed(long keysProcessed) {
    this.numKeysProcessed.incr(keysProcessed);
  }

  public void incrNumKeysDeletionRequest(long keysDeletionRequest) {
    this.numKeysDeletionRequest.incr(keysDeletionRequest);
  }

  public void incrNumKeysDeleteSuccess(long keysDeleteSuccess) {
    this.numKeysDeleteSuccess.incr(keysDeleteSuccess);
  }

  public void incrNumKeysSentForPurge(long keysPurge) {
    this.numKeysSentForPurge.incr(keysPurge);
  }

  /*
   * Key deletion metrics in the latest iteration of KeyDeletingService.
   */
  @Metric("Iteration run count of KeyDeletingService")
  private MutableGaugeLong iterationKeyRunCount;
  @Metric("Iteration start time of KeyDeletingService")
  private MutableGaugeLong iterationKeyStartTime;
  @Metric("Total time taken by the last iteration of KeyDeletingService")
  private MutableGaugeLong iterationKeyDuration;
  @Metric("Total no. of keys processed in last iteration")
  private MutableGaugeLong iterationKeysProcessed;
  @Metric("Total no. of keys sent to scm for deletion")
  private MutableGaugeLong iterationKeysDeletionRequest;
  @Metric("Total no. of keys deleted successfully")
  private MutableGaugeLong iterationKeysDeleteSuccess;
  @Metric("Total no. of deleted keys sent for purge")
  private MutableGaugeLong iterationKeysSentForPurge;

  public void setIterationKeyRunCount(long iterationKeyRunCount) {
    this.iterationKeyRunCount.set(iterationKeyRunCount);
  }

  public void setIterationKeyStartTime(long iterationKeyStartTime) {
    this.iterationKeyStartTime.set(iterationKeyStartTime);
  }

  public void setIterationKeyDuration(long iterationKeyDuration) {
    this.iterationKeyDuration.set(iterationKeyDuration);
  }

  public void setIterationKeysProcessed(long iterationKeysProcessed) {
    this.iterationKeysProcessed.set(iterationKeysProcessed);
  }

  public void setIterationKeysDeletionRequest(long iterationKeysDeletionRequest) {
    this.iterationKeysDeletionRequest.set(iterationKeysDeletionRequest);
  }

  public void setIterationKeysDeleteSuccess(long iterationKeysDeleteSuccess) {
    this.iterationKeysDeleteSuccess.set(iterationKeysDeleteSuccess);
  }

  public void setIterationKeysSentForPurge(long keysPurge) {
    this.iterationKeysSentForPurge.set(keysPurge);
  }

  /*
   * Directory purge request metrics.
   */
  @Metric("Total no. of directories purged")
  private MutableGaugeLong numDirsPurged;
  @Metric("Total no. of subFiles purged")
  private MutableGaugeLong numSubKeysPurged;
  @Metric("Total no. of subDirectories purged")
  private MutableGaugeLong numSubDirsPurged;
  @Metric("No. of directories purged in latest request")
  private MutableGaugeLong numDirsPurgedInLatestRequest;
  @Metric("No. of subFiles purged in latest request")
  private MutableGaugeLong numSubKeysPurgedInLatestRequest;
  @Metric("No. of subDirectories purged in latest request")
  private MutableGaugeLong numSubDirsPurgedInLatestRequest;

  public void incrNumDirPurged(long dirPurged) {
    this.numDirsPurged.incr(dirPurged);
  }
  public void incrNumSubKeysPurged(long subKeysPurged) {
    this.numSubKeysPurged.incr(subKeysPurged);
  }
  public void incrNumSubDirectoriesPurged(long subDirectoriesPurged) {
    this.numSubDirsPurged.incr(subDirectoriesPurged);
  }
  public void setNumDirPurgedInLatestRequest(long dirPurged) {
    this.numDirsPurgedInLatestRequest.incr(dirPurged);
  }

  public void setNumSubKeysPurgedInLatestRequest(long subKeysPurged) {
    this.numSubKeysPurgedInLatestRequest.incr(subKeysPurged);
  }
  public void setNumSubDirectoriesPurgedInLatestRequest(long subDirectoriesPurged) {
    this.numSubDirsPurgedInLatestRequest.incr(subDirectoriesPurged);
  }

  /*
   * Key purge request metrics.
   */

  @Metric("Total no. of keys purged")
  private MutableGaugeLong numKeysPurged;
  @Metric("No. of keys purged in latest request")
  private MutableGaugeLong numKeysPurgedInLatestRequest;

  public void incrNumKeysPurged(long keysPurged) {
    this.numKeysPurged.incr(keysPurged);
  }

  public void setNumKeysPurgedInLatestRequest(long numSubKeysPurgedInLastRequest) {
    this.numKeysPurgedInLatestRequest.set(numSubKeysPurgedInLastRequest);
  }

  /*
   * OpenKeyCleanupService related metrics.
   */

  @Metric("Last iteration run count of OpenKeyCleanupService")
  private MutableGaugeLong iterationRunCountOpenKeyCleanup;
  @Metric("Last iteration start time of OpenKeyCleanupService")
  private MutableGaugeLong iterationStartTimeOpenKeyCleanup;
  @Metric("Time taken by the last iteration of OpenKeyCleanupService")
  private MutableGaugeLong iterationDurationOpenKeyCleanup;
  @Metric("No. of keys deleted by OpenKeyCleanupService in last iteration")
  private MutableGaugeLong iterationOpenKeysDeleted;
  @Metric("No. of hsync keys committed by OpenKeyCleanupService in last iteration")
  private MutableGaugeLong iterationOpenKeysCommitted;

  public void setIterationRunCountOpenKeyCleanup(
      long iterationRunCountOpenKeyCleanup) {
    this.iterationRunCountOpenKeyCleanup.set(iterationRunCountOpenKeyCleanup);
  }

  public void setIterationStartTimeOpenKeyCleanup(
      long iterationStartTimeOpenKeyCleanup) {
    this.iterationStartTimeOpenKeyCleanup.set(iterationStartTimeOpenKeyCleanup);
  }

  public void setIterationDurationOpenKeyCleanup(
      long iterationDurationOpenKeyCleanup) {
    this.iterationDurationOpenKeyCleanup.set(iterationDurationOpenKeyCleanup);
  }

  public void setIterationOpenKeysDeleted(long iterationOpenKeysDeleted) {
    this.iterationOpenKeysDeleted.set(iterationOpenKeysDeleted);
  }

  public void setIterationOpenKeysCommitted(long iterationOpenKeysCommitted) {
    this.iterationOpenKeysCommitted.set(iterationOpenKeysCommitted);
  }

  public void setOpenKeyCleanupIterationMetrics(long runcount, long startTime, long duration,
                                                long keysDeleted, long keysCommitted) {
    setIterationRunCountOpenKeyCleanup(runcount);
    setIterationStartTimeOpenKeyCleanup(startTime);
    setIterationDurationOpenKeyCleanup(duration);
    setIterationOpenKeysDeleted(keysDeleted);
    setIterationOpenKeysCommitted(keysCommitted);
  }

  @Metric("Total no. of keys deleted by OpenKeyCleanupService since last restart")
  private MutableGaugeLong totalOpenKeysDeleted;
  @Metric("Total no. of keys committed by OpenKeyCleanupService since last restart")
  private MutableGaugeLong totalOpenKeysCommitted;

  public void setTotalOpenKeysDeleted(long openKeysDeleted) {
    this.totalOpenKeysDeleted.incr(openKeysDeleted);
  }

  public void setTotalOpenKeysCommitted(long openKeysCommitted) {
    this.totalOpenKeysCommitted.incr(openKeysCommitted);
  }

  public void setOpenKeyCleanupTotalMetrics(long keysDeleted, long keysCommitted) {
    setTotalOpenKeysDeleted(keysDeleted);
    setTotalOpenKeysCommitted(keysCommitted);
  }
}