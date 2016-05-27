package com.bestpay.cf.batch.dao.service;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;

import java.util.List;

/**
 * Created by perdonare on 2016/5/27.
 */
public class BatchJobInstanceService implements JobInstanceDao{
    @Override
    public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
        return null;
    }

    @Override
    public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
        return null;
    }

    @Override
    public JobInstance getJobInstance(Long instanceId) {
        return null;
    }

    @Override
    public JobInstance getJobInstance(JobExecution jobExecution) {
        return null;
    }

    @Override
    public List<JobInstance> getJobInstances(String jobName, int start, int count) {
        return null;
    }

    @Override
    public List<String> getJobNames() {
        return null;
    }

    @Override
    public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
        return null;
    }

    @Override
    public int getJobInstanceCount(String jobName) throws NoSuchJobException {
        return 0;
    }
}
