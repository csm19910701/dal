package com.ctrip.platform.dal.dao.task;

import com.ctrip.framework.dal.cluster.client.hint.RouteHints;
import com.ctrip.framework.dal.cluster.client.parameter.NamedSqlParameters;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.StatementParameters;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public class ClusterBatchInsertTask<T> extends InsertTaskAdapter<T> implements BulkTask<int[], T> {
    @Override
    public int[] getEmptyValue() {
        return new int[0];
    }

    @Override
    public int[] execute(DalHints hints, Map<Integer, Map<String, ?>> daoPojos, DalBulkTaskContext<T> taskContext) throws SQLException {
//        StatementParameters[] parametersList = new StatementParameters[daoPojos.size()];
        RouteHints routeHints=buildRouteHints(hints);
        NamedSqlParameters[] namedSqlParameters=new NamedSqlParameters[daoPojos.size()];
        int i = 0;

        Set<String> unqualifiedColumns = taskContext.getUnqualifiedColumns();

        for (Integer index :daoPojos.keySet()) {
            Map<String, ?> pojo = daoPojos.get(index);
            removeUnqualifiedColumns(pojo, unqualifiedColumns);

            StatementParameters parameters = new StatementParameters();
            addParameters(parameters, pojo);
            namedSqlParameters[i++] = parameters;
        }

//        String tableName = getRawTableName(hints);
//        if (taskContext instanceof DalContextConfigure) {
//            ((DalContextConfigure) taskContext).addTables(tableName);
//            ((DalContextConfigure) taskContext).setShardingCategory(shardingCategory);
//        }

//        String batchInsertSql = buildBatchInsertSql(hints, unqualifiedColumns, tableName);

//        int[] result;
//        if (client instanceof DalContextClient)
//            result = ((DalContextClient) client).batchUpdate(batchInsertSql, parametersList, hints, taskContext);
//        else
//            throw new DalRuntimeException("The client is not instance of DalClient");
//        return cluster.batchInsert(rawTableName,namedSqlParameters,routeHints);
        return null;
    }


    @Override
    public BulkTaskResultMerger<int[]> createMerger() {
        return new ShardedIntArrayResultMerger();
    }

}
