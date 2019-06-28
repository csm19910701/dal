package com.ctrip.platform.dal.dao.task;

import com.ctrip.framework.dal.cluster.client.ClusterKeyHolder;
import com.ctrip.framework.dal.cluster.client.hint.RouteHints;
import com.ctrip.framework.dal.cluster.client.parameter.NamedSqlParameters;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.StatementParameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterCombinedInsertTask<T> extends InsertTaskAdapter<T> implements BulkTask<Integer, T>, KeyHolderAwaredTask {
    @Override
    public Integer getEmptyValue() {
        return 0;
    }


    @Override
    public Integer execute(DalHints hints, Map<Integer, Map<String, ?>> daoPojos, DalBulkTaskContext<T> taskContext) throws SQLException {
        final KeyHolder generatedKeyHolder = hints.getKeyHolder();
        RouteHints routeHints = buildRouteHints(hints);
//        StringBuilder values = new StringBuilder();

        Set<String> unqualifiedColumns = taskContext.getUnqualifiedColumns();

        NamedSqlParameters[] namedSqlParameters=new NamedSqlParameters[daoPojos.size()];

        List<String> finalInsertableColumns = buildValidColumnsForInsert(unqualifiedColumns);

//        String insertColumns = combineColumns(finalInsertableColumns, COLUMN_SEPARATOR);

        List<Map<String, Object>> identityFields = new ArrayList<>();

//        int startIndex = 1;
        for (Integer index : daoPojos.keySet()) {
            Map<String, ?> pojo = daoPojos.get(index);
            removeUnqualifiedColumns(pojo, unqualifiedColumns);

            Map<String, Object> identityField = getIdentityField(pojo);
            if (identityField != null) {
                identityFields.add(identityField);
            }
            StatementParameters parameters = new StatementParameters();
            addParameters(parameters, pojo, finalInsertableColumns.toArray(new String[finalInsertableColumns.size()]));
            namedSqlParameters[index] = parameters;
        }

        // Put identityFields and pojos count into context
        if (taskContext instanceof DefaultTaskContext) {
            ((DefaultTaskContext) taskContext).setIdentityFields(identityFields);
            ((DefaultTaskContext) taskContext).setPojosCount(daoPojos.size());
        }

//        String tableName = getRawTableName(hints);
//        if (taskContext instanceof DalContextConfigure) {
//            ((DalContextConfigure) taskContext).addTables(tableName);
//            ((DalContextConfigure) taskContext).setShardingCategory(shardingCategory);
//        }
//
//        String sql = String.format(getSqlTpl(),
//                quote(tableName), insertColumns,
//                values.substring(0, values.length() - 2) + ")");
//
//        if (client instanceof DalContextClient)
//            return ((DalContextClient) client).update(sql, parameters, hints, taskContext);
//        else
//            throw new DalRuntimeException("The client is not instance of DalClient");
        if(generatedKeyHolder==null)
//            return cluster.combinedInsert(logicDbName,namedSqlParameters,routeHints);
            return 0;

        else {
            ClusterKeyHolder keyHolder = new ClusterKeyHolder();
//            int rows = cluster.combinedInsert(rawTableName, namedSqlParameters, routeHints, keyHolder);
            int rows=0;

            int pojosCount = 0;
            List<Map<String, Object>> presetKeys = null;
            List<Map<String, Object>> dbReturnedKeys = null;
            if (taskContext != null) {
                pojosCount = taskContext.getPojosCount();
                presetKeys = taskContext.getIdentityFields();
            }

            dbReturnedKeys = keyHolder.getKeyList();

            int actualKeySize = 0;
            List<Map<String, Object>> returnedKeys = getFinalGeneratedKeys(dbReturnedKeys, presetKeys, pojosCount);
            if (returnedKeys != null) {
                generatedKeyHolder.addKeys(returnedKeys);
                actualKeySize = returnedKeys.size();
            }
            generatedKeyHolder.addEmptyKeys(pojosCount - actualKeySize);
            return rows;
        }
    }

    @Override
    public BulkTaskResultMerger<Integer> createMerger() {
        return new ShardedIntResultMerger();
    }


}
