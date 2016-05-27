package com.bestpay.cf.batch.core.repository.impl;

import com.bestpay.cf.batch.core.repository.BatchJobRepository;
import com.bestpay.cf.batch.dao.service.BatchExecutionContextService;
import com.bestpay.cf.batch.dao.service.BatchJobExecutionService;
import com.bestpay.cf.batch.dao.service.BatchJobInstanceService;
import com.bestpay.cf.batch.dao.service.BatchStepExecutionService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by perdonare on 2016/5/27.
 */
public class BatchJobRepositoryImpl implements BatchJobRepository {
    private static final Log logger = LogFactory.getLog(BatchJobRepository.class);

    private BatchJobInstanceService jobInstanceService;

    private BatchJobExecutionService jobExecutionService;

    private BatchStepExecutionService stepExecutionService;

    private BatchExecutionContextService executionContextService;

    BatchJobRepositoryImpl() {

    }

    public BatchJobRepositoryImpl(BatchJobInstanceService jobInstanceService, BatchJobExecutionService jobExecutionService,
                                  BatchStepExecutionService stepExecutionService, BatchExecutionContextService executionContextService) {
        super();
        this.jobInstanceService = jobInstanceService;
        this.jobExecutionService = jobExecutionService;
        this.stepExecutionService = stepExecutionService;
        this.executionContextService = executionContextService;
    }

    @Override
    public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
        return jobInstanceService.getJobInstance(jobName, jobParameters) != null;
    }

    @Override
    public JobExecution createJobExecution(String jobName, JobParameters jobParameters)
            throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

		/*
		 * Find all jobs matching the runtime information.
		 *
		 * If this method is transactional, and the isolation level is
		 * REPEATABLE_READ or better, another launcher trying to start the same
		 * job in another thread or process will block until this transaction
		 * has finished.
		 */

        JobInstance jobInstance = jobInstanceService.getJobInstance(jobName, jobParameters);
        ExecutionContext executionContext;

        // existing job instance found
        if (jobInstance != null) {

            List<JobExecution> executions = jobExecutionService.findJobExecutions(jobInstance);

            // check for running executions and find the last started
            for (JobExecution execution : executions) {
                if (execution.isRunning()) {
                    throw new JobExecutionAlreadyRunningException("A job execution for this job is already running: "
                            + jobInstance);
                }

                BatchStatus status = execution.getStatus();
                if (execution.getJobParameters().getParameters().size() > 0 && (status == BatchStatus.COMPLETED || status == BatchStatus.ABANDONED)) {
                    throw new JobInstanceAlreadyCompleteException(
                            "A job instance already exists and is complete for parameters=" + jobParameters
                                    + ".  If you want to run this job again, change the parameters.");
                }
            }
            executionContext = executionContextService.getExecutionContext(jobExecutionService.getLastJobExecution(jobInstance));
        }
        else {
            // no job found, create one
            jobInstance = jobInstanceService.createJobInstance(jobName, jobParameters);
            executionContext = new ExecutionContext();
        }

        JobExecution jobExecution = new JobExecution(jobInstance, jobParameters, null);
        jobExecution.setExecutionContext(executionContext);
        jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        // Save the JobExecution so that it picks up an ID (useful for clients
        // monitoring asynchronous executions):
        jobExecutionService.saveJobExecution(jobExecution);
        executionContextService.saveExecutionContext(jobExecution);

        return jobExecution;

    }

    @Override
    public void update(JobExecution jobExecution) {

        Assert.notNull(jobExecution, "JobExecution cannot be null.");
        Assert.notNull(jobExecution.getJobId(), "JobExecution must have a Job ID set.");
        Assert.notNull(jobExecution.getId(), "JobExecution must be already saved (have an id assigned).");

        jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        jobExecutionService.synchronizeStatus(jobExecution);
        jobExecutionService.updateJobExecution(jobExecution);
    }

    @Override
    public void add(StepExecution stepExecution) {
        validateStepExecution(stepExecution);

        stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
        stepExecutionService.saveStepExecution(stepExecution);
        executionContextService.saveExecutionContext(stepExecution);
    }

    @Override
    public void addAll(Collection<StepExecution> stepExecutions) {
        Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");
        for (StepExecution stepExecution : stepExecutions) {
            validateStepExecution(stepExecution);
            stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
        }
        stepExecutionService.saveStepExecutions(stepExecutions);
        executionContextService.saveExecutionContexts(stepExecutions);
    }

    @Override
    public void update(StepExecution stepExecution) {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");

        stepExecution.setLastUpdated(new Date(System.currentTimeMillis()));
        stepExecutionService.updateStepExecution(stepExecution);
        checkForInterruption(stepExecution);
    }

    private void validateStepExecution(StepExecution stepExecution) {
        Assert.notNull(stepExecution, "StepExecution cannot be null.");
        Assert.notNull(stepExecution.getStepName(), "StepExecution's step name cannot be null.");
        Assert.notNull(stepExecution.getJobExecutionId(), "StepExecution must belong to persisted JobExecution");
    }

    @Override
    public void updateExecutionContext(StepExecution stepExecution) {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");
        executionContextService.updateExecutionContext(stepExecution);
    }

    @Override
    public void updateExecutionContext(JobExecution jobExecution) {
        executionContextService.updateExecutionContext(jobExecution);
    }

    @Override
    public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
        List<JobExecution> jobExecutions = jobExecutionService.findJobExecutions(jobInstance);
        List<StepExecution> stepExecutions = new ArrayList<StepExecution>(jobExecutions.size());

        for (JobExecution jobExecution : jobExecutions) {
            stepExecutionService.addStepExecutions(jobExecution);
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                if (stepName.equals(stepExecution.getStepName())) {
                    stepExecutions.add(stepExecution);
                }
            }
        }

        StepExecution latest = null;
        for (StepExecution stepExecution : stepExecutions) {
            if (latest == null) {
                latest = stepExecution;
            }
            if (latest.getStartTime().getTime() < stepExecution.getStartTime().getTime()) {
                latest = stepExecution;
            }
        }

        if (latest != null) {
            ExecutionContext stepExecutionContext = executionContextService.getExecutionContext(latest);
            latest.setExecutionContext(stepExecutionContext);
            ExecutionContext jobExecutionContext = executionContextService.getExecutionContext(latest.getJobExecution());
            latest.getJobExecution().setExecutionContext(jobExecutionContext);
        }

        return latest;
    }

    /**
     * @return number of executions of the step within given job instance
     */
    @Override
    public int getStepExecutionCount(JobInstance jobInstance, String stepName) {
        int count = 0;
        List<JobExecution> jobExecutions = jobExecutionService.findJobExecutions(jobInstance);
        for (JobExecution jobExecution : jobExecutions) {
            stepExecutionService.addStepExecutions(jobExecution);
            for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                if (stepName.equals(stepExecution.getStepName())) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Check to determine whether or not the JobExecution that is the parent of
     * the provided StepExecution has been interrupted. If, after synchronizing
     * the status with the database, the status has been updated to STOPPING,
     * then the job has been interrupted.
     *
     * @param stepExecution
     */
    private void checkForInterruption(StepExecution stepExecution) {
        JobExecution jobExecution = stepExecution.getJobExecution();
        jobExecutionService.synchronizeStatus(jobExecution);
        if (jobExecution.isStopping()) {
            logger.info("Parent JobExecution is stopped, so passing message on to StepExecution");
            stepExecution.setTerminateOnly();
        }
    }

    @Override
    public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
        JobInstance jobInstance = jobInstanceService.getJobInstance(jobName, jobParameters);
        if (jobInstance == null) {
            return null;
        }
        JobExecution jobExecution = jobExecutionService.getLastJobExecution(jobInstance);

        if (jobExecution != null) {
            jobExecution.setExecutionContext(executionContextService.getExecutionContext(jobExecution));
            stepExecutionService.addStepExecutions(jobExecution);
        }
        return jobExecution;

    }

    @Override
    public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        Assert.notNull(jobName, "A job name is required to create a JobInstance");
        Assert.notNull(jobParameters, "Job parameters are required to create a JobInstance");

        JobInstance jobInstance = jobInstanceService.createJobInstance(jobName, jobParameters);

        return jobInstance;
    }

    @Override
    public JobExecution createJobExecution(JobInstance jobInstance,
                                           JobParameters jobParameters, String jobConfigurationLocation) {

        Assert.notNull(jobInstance, "A JobInstance is required to associate the JobExecution with");
        Assert.notNull(jobParameters, "A JobParameters object is required to create a JobExecution");

        JobExecution jobExecution = new JobExecution(jobInstance, jobParameters, jobConfigurationLocation);
        ExecutionContext executionContext = new ExecutionContext();
        jobExecution.setExecutionContext(executionContext);
        jobExecution.setLastUpdated(new Date(System.currentTimeMillis()));

        // Save the JobExecution so that it picks up an ID (useful for clients
        // monitoring asynchronous executions):
        jobExecutionService.saveJobExecution(jobExecution);
        executionContextService.saveExecutionContext(jobExecution);

        return jobExecution;
    }
}
