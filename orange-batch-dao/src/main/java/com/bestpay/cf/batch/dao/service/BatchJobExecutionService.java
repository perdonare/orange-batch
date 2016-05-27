package com.bestpay.cf.batch.dao.service;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.repository.dao.JobExecutionDao;

import java.util.List;
import java.util.Set;

/**
 * Created by perdonare on 2016/5/27.
 */
public class BatchJobExecutionService implements JobExecutionDao {
    @Override
    public void saveJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void updateJobExecution(JobExecution jobExecution) {

    }

    @Override
    public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
        return null;
    }

    @Override
    public JobExecution getLastJobExecution(JobInstance jobInstance) {
        return null;
    }

    @Override
    public Set<JobExecution> findRunningJobExecutions(String jobName) {
        return null;
    }

    @Override
    public JobExecution getJobExecution(Long executionId) {
        return null;
    }

    @Override
    public void synchronizeStatus(JobExecution jobExecution) {

    }
}
