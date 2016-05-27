package com.bestpay.cf.batch.dao.service;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;

import java.util.Collection;

/**
 * Created by perdonare on 2016/5/27.
 */
public class BatchExecutionContextService implements ExecutionContextDao {
    @Override
    public ExecutionContext getExecutionContext(JobExecution jobExecution) {
        return null;
    }

    @Override
    public ExecutionContext getExecutionContext(StepExecution stepExecution) {
        return null;
    }

    @Override
    public void saveExecutionContext(JobExecution jobExecution) {

    }

    @Override
    public void saveExecutionContext(StepExecution stepExecution) {

    }

    @Override
    public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {

    }

    @Override
    public void updateExecutionContext(JobExecution jobExecution) {

    }

    @Override
    public void updateExecutionContext(StepExecution stepExecution) {

    }
}
