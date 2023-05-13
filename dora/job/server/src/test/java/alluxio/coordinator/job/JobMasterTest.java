/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.coordinator.job;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.coordinator.AlwaysStandbyPrimarySelector;
import alluxio.coordinator.MasterContext;
import alluxio.coordinator.NoopUfsManager;
import alluxio.coordinator.job.command.CommandManager;
import alluxio.coordinator.job.plan.PlanCoordinator;
import alluxio.coordinator.journal.noop.NoopJournalSystem;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.grpc.ListAllPOptions;
import alluxio.job.CmdConfig;
import alluxio.job.JobConfig;
import alluxio.job.JobServerContext;
import alluxio.job.SleepJobConfig;
import alluxio.job.TestPlanConfig;
import alluxio.job.cmd.load.LoadCliConfig;
import alluxio.job.plan.PlanConfig;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.Status;
import alluxio.job.workflow.composite.CompositeConfig;
import alluxio.underfs.UfsManager;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Tests {@link JobMaster}.
 */
public final class JobMasterTest {
  private static final int TEST_JOB_MASTER_JOB_CAPACITY = 100;
  private JobMaster mJobMaster;
  private PlanCoordinator mMockPlanCoordinator;
  private MockedStatic<FileSystem.Factory> mMockStaticFactory;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    // Can't use ConfigurationRule due to conflicts with Mock.
    Configuration.set(PropertyKey.JOB_MASTER_JOB_CAPACITY, TEST_JOB_MASTER_JOB_CAPACITY);
    mMockStaticFactory = mockStatic(FileSystem.Factory.class);
    FileSystem fs = mock(FileSystem.class);
    when(FileSystem.Factory.create(any(FileSystemContext.class)))
            .thenReturn(fs);
    mJobMaster = new JobMaster(new MasterContext<>(new NoopJournalSystem(),
        new AlwaysStandbyPrimarySelector(), new NoopUfsManager()),
        mock(FileSystem.class), mock(FileSystemContext.class), mock(UfsManager.class));
    mJobMaster.start(true);
  }

  @After
  public void after() throws Exception {
    mJobMaster.stop();
    Configuration.reloadProperties();
    mMockStaticFactory.close();
  }

  @Test
  public void runNonExistingJobConfig() throws Exception {
    try {
      mJobMaster.run(new DummyPlanConfig());
      Assert.fail("cannot run non-existing job");
    } catch (JobDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage("dummy"),
          e.getMessage());
    }
  }

  @Test
  public void runNestedNonExistingJobConfig() throws Exception {
    JobConfig innerJobConfig = new CompositeConfig(
        Lists.newArrayList(new DummyPlanConfig()), true);

    CompositeConfig jobConfig = new CompositeConfig(Lists.newArrayList(innerJobConfig), true);
    long jobId = mJobMaster.run(jobConfig);
    JobInfo status = mJobMaster.getStatus(jobId);
    Assert.assertEquals(Status.FAILED, status.getStatus());
    List<JobInfo> children = status.getChildren();
    Assert.assertEquals(1, children.size());
    JobInfo child = children.get(0);
    Assert.assertEquals(Status.FAILED, child.getStatus());
    Assert.assertEquals(0, child.getChildren().size());
  }

  @Test
  public void run() throws Exception {
    try (MockedStatic<PlanCoordinator> mockStaticPlanCoordinator = mockPlanCoordinator()) {
      TestPlanConfig jobConfig = new TestPlanConfig("/test");
      List<Long> jobIdList = new ArrayList<>();
      for (long i = 0; i < TEST_JOB_MASTER_JOB_CAPACITY; i++) {
        jobIdList.add(mJobMaster.run(jobConfig));
      }
      final List<Long> list = mJobMaster.list(ListAllPOptions.getDefaultInstance());
      Assert.assertEquals(jobIdList, list);
      Assert.assertEquals(TEST_JOB_MASTER_JOB_CAPACITY,
          mJobMaster.list(ListAllPOptions.getDefaultInstance()).size());
    }
  }

  @Test
  public void list() throws Exception {
    try (MockedStatic<PlanCoordinator> mockStaticPlanCoordinator = mockPlanCoordinator()) {
      TestPlanConfig jobConfig = new TestPlanConfig("/test");
      List<Long> jobIdList = new ArrayList<>();
      for (long i = 0; i < TEST_JOB_MASTER_JOB_CAPACITY; i++) {
        jobIdList.add(mJobMaster.run(jobConfig));
      }
      final List<Long> list = mJobMaster.list(ListAllPOptions.getDefaultInstance());
      Assert.assertEquals(jobIdList, list);
      Assert.assertEquals(TEST_JOB_MASTER_JOB_CAPACITY,
          mJobMaster.list(ListAllPOptions.getDefaultInstance()).size());
    }
  }

  @Test
  public void flowControl() throws Exception {
    try (MockedStatic<PlanCoordinator> mockStaticPlanCoordinator = mockPlanCoordinator()) {
      TestPlanConfig jobConfig = new TestPlanConfig("/test");
      for (long i = 0; i < TEST_JOB_MASTER_JOB_CAPACITY; i++) {
        mJobMaster.run(jobConfig);
      }
      try {
        mJobMaster.run(jobConfig);
        Assert.fail("should not be able to run more jobs than job master capacity");
      } catch (ResourceExhaustedException e) {
        Assert.assertEquals(ExceptionMessage.JOB_MASTER_FULL_CAPACITY
                .getMessage(Configuration.get(PropertyKey.JOB_MASTER_JOB_CAPACITY)),
            e.getMessage());
      }
    }
  }

  @Test
  public void cancelNonExistingJob() {
    try {
      mJobMaster.cancel(1);
      Assert.fail("cannot cancel non-existing job");
    } catch (JobDoesNotExistException e) {
      Assert.assertEquals(ExceptionMessage.JOB_DOES_NOT_EXIST.getMessage(1), e.getMessage());
    }
  }

  @Test
  public void cancel() throws Exception {
    try (MockedStatic<PlanCoordinator> mockStaticPlanCoordinator = mockPlanCoordinator()) {
      SleepJobConfig config = new SleepJobConfig(10000);
      long jobId = mJobMaster.run(config);
      mJobMaster.cancel(jobId);
      verify(mMockPlanCoordinator).cancel();
    }
  }

  @Test
  public void submitAndList() throws Exception {
    CmdConfig config = new LoadCliConfig("/path/to/load", 3, 1, Collections.EMPTY_SET,
            Collections.EMPTY_SET, Collections.EMPTY_SET, Collections.EMPTY_SET, true);
    List<Long> jobIdList = new ArrayList<>();
    for (long i = 0; i < TEST_JOB_MASTER_JOB_CAPACITY; i++) {
      long jobControlId = mJobMaster.submit(config);
      jobIdList.add(jobControlId);
    }
    final List<Long> list = mJobMaster.listCmds(ListAllPOptions.getDefaultInstance());
    Assert.assertEquals(jobIdList, list);
    Assert.assertEquals(TEST_JOB_MASTER_JOB_CAPACITY,
            mJobMaster.listCmds(ListAllPOptions.getDefaultInstance()).size());
  }

  private MockedStatic<PlanCoordinator> mockPlanCoordinator() {
    mMockPlanCoordinator = mock(PlanCoordinator.class);
    MockedStatic<PlanCoordinator> mockedStaticPlanCoordinator = mockStatic(PlanCoordinator.class);
    mockedStaticPlanCoordinator.when(() ->
        PlanCoordinator.create(any(CommandManager.class), any(JobServerContext.class),
            anyList(), anyLong(), any(JobConfig.class), any(Consumer.class)))
        .thenReturn(mMockPlanCoordinator);
    return mockedStaticPlanCoordinator;
  }

  private static class DummyPlanConfig implements PlanConfig {
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "dummy";
    }

    @Override
    public Collection<String> affectedPaths() {
      return Collections.EMPTY_LIST;
    }
  }
}
