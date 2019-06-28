package com.ctrip.platform.dal.dao.task;

import com.ctrip.framework.dal.cluster.client.ClusterKeyHolder;
import com.ctrip.framework.dal.cluster.client.exception.UndefinedIdGeneratorException;
import com.ctrip.framework.dal.cluster.client.hint.RouteHints;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.StatementParameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterSingleInsertTask<T> extends InsertTaskAdapter<T> implements SingleTask<T>,KeyHolderAwaredTask{
    @Override
    public int execute(DalHints hints, Map<String, ?> fields, T rawPojo, DalTaskContext dalTaskContext) throws SQLException {
        final KeyHolder generatedKeyHolder = hints.getKeyHolder();
        RouteHints routeHints = buildRouteHints(hints);

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
            return cluster.insert(rawTableName, parameters, routeHints);

        else {
            ClusterKeyHolder keyHolder = new ClusterKeyHolder();
            int rows = cluster.insert(rawTableName, parameters, keyHolder, routeHints);

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

    @Override
    public void initIdGenerator(){

    }

    @Override
    public void processIdentityField(DalHints hints, List<Map<String, ?>> pojos) {
        if (parser.isAutoIncrement()) {
            String identityFieldName = parser.getPrimaryKeyNames()[0];
            int identityFieldType = getColumnType(identityFieldName);
            boolean identityInsertDisabled = hints.isIdentityInsertDisabled();
            for (Map pojo : pojos) {
                if (identityInsertDisabled || null == pojo.get(identityFieldName)) {
                    Number id = null;
                    try {
                        id = cluster.getSequenceId(rawTableName);
                    } catch (UndefinedIdGeneratorException e) {
                        useIdGen = false;
                    }
                    if (id != null) {
                        useIdGen = true;
                        checkIdentityTypes(identityFieldType, id);
                        pojo.put(identityFieldName, id);
                    }
                }
            }
        }
    }
}
