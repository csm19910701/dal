package com.ctrip.platform.dal.dao.task;

import com.ctrip.framework.dal.cluster.client.ClusterKeyHolder;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.StatementParameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSingleInsertTask<T> extends InsertTaskAdapter<T> implements SingleTask<T>, KeyHolderAwaredTask {

    @Override
    public int execute(DalHints hints, Map<String, ?> fields, T rawPojo, DalTaskContext dalTaskContext) throws SQLException {
        final KeyHolder generatedKeyHolder = hints.getKeyHolder();

        List<Map<String, ?>> pojoList = new ArrayList<>();
        List<T> rawPojos = new ArrayList<>();

        pojoList.add(fields);
        rawPojos.add(rawPojo);

        Set<String> unqualifiedColumns = filterUnqualifiedColumns(hints, pojoList, rawPojos);
        removeUnqualifiedColumns(fields, unqualifiedColumns);

        // Put identityFields into context
        List<Map<String, Object>> identityFields = new ArrayList<>();
        Map<String, Object> identityField = getIdentityField(fields);
        if (identityField != null) {
            identityFields.add(identityField);
        }
        if (dalTaskContext instanceof DefaultTaskContext) {
            ((DefaultTaskContext) dalTaskContext).setIdentityFields(identityFields);
            ((DefaultTaskContext) dalTaskContext).setPojosCount(1);
        }

//        String tableName = getRawTableName(hints, fields);
//        if (taskContext instanceof DalContextConfigure)
//            ((DalContextConfigure) taskContext).addTables(tableName);

        /*
         * In case fields is empty, the final sql will be like "insert into tableName () values()".
         * We do not report error or simply return 0, but just let DB decide what to do.
         * For MS Sql server, sql like this is illegal, but for mysql, this works however
         */
//        String insertSql = buildInsertSql(hints, fields, tableName);

        StatementParameters parameters = new StatementParameters();
        addParameters(parameters, fields);

        if (generatedKeyHolder == null)
            return cluster.insert(rawTableName, parameters);

        else {
            ClusterKeyHolder keyHolder = new ClusterKeyHolder();
            int rows = cluster.insert(rawTableName, parameters, keyHolder);

            int pojosCount = 0;
            List<Map<String, Object>> presetKeys = null;
            List<Map<String, Object>> dbReturnedKeys = null;
            if (dalTaskContext != null) {
                pojosCount = dalTaskContext.getPojosCount();
                presetKeys = dalTaskContext.getIdentityFields();
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

//        if (client instanceof DalContextClient)
//            return ((DalContextClient) client).update(insertSql, parameters, hints, taskContext);
//        else
//            throw new DalRuntimeException("The client is not instance of DalClient");
    }

    private String buildInsertSql(DalHints hints, Map<String, ?> fields, String effectiveTableName) throws SQLException {
        Set<String> remainedColumns = fields.keySet();
        String cloumns = combineColumns(remainedColumns, COLUMN_SEPARATOR);
        String values = combine(PLACE_HOLDER, remainedColumns.size(), COLUMN_SEPARATOR);

        return String.format(getSqlTpl(), quote(effectiveTableName), cloumns, values);
    }

    protected abstract String getSqlTpl();

}
