package com.ctrip.platform.dal.dao.task;

import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.client.LogContext;
import com.ctrip.platform.dal.exceptions.DalException;
import com.ctrip.platform.dal.exceptions.ErrorCode;
import java.sql.SQLException;
import java.util.concurrent.*;

public class ClusterRequestExecutor extends AbstractRequestExecutor {

    protected <T> T internalExecute(DalHints hints, DalRequest<T> request, boolean nullable) throws SQLException {
        T result = null;
        Throwable error = null;

        LogContext logContext = logger.start(request);

        try {
            request.validateAndPrepare();
            result = execute(logContext, hints, request);
            if(result == null && !nullable)
                throw new DalException(ErrorCode.AssertNull);

            request.endExecution();
        } catch (Throwable e) {
            error = e;
        }

        logger.end(logContext, error);

        handleCallback(hints, result, error);
        if(error != null)
            throw DalException.wrap(error);

        return result;
    }

    private <T> T execute(LogContext logContext, DalHints hints, DalRequest<T> request) throws Exception {
        logContext.setSingleTask(true);
        Callable<T> task = new RequestTaskWrapper<T>(NA, request.createTask(), logContext);
        return task.call();
    }


}
