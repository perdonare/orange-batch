package com.bestpay.cf.batch.dao.service;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;

import java.util.Collection;

/**
 * Created by perdonare on 2016/5/27.
 */
public class BatchStepExecutionService implements StepExecutionDao {
    @Override
    public void saveStepExecution(StepExecution stepExecution) {

    }

    @Override
    public void saveStepExecutions(Collection<StepExecution> stepExecutions) {

    }

    @Override
    public void updateStepExecution(StepExecution stepExecution) {

    }

    @Override
    public StepExecution getStepExecution(JobExecution jobExecution, Long stepExecutionId) {
        return null;
    }

    @Override
    public void addStepExecutions(JobExecution jobExecution) {

    }
}
