package com.bestpay.cf.batch.core.datasource;

import com.bestpay.cf.batch.core.common.DataSourceContext;
import com.bestpay.cf.batch.core.connection.BatchConnection;

/**
 * Created by perdonare on 2016/5/27.
 */
public interface BatchDataSource {
    BatchConnection getConnection(DataSourceContext dataSourceContext) ;
}
