package com.ctrip.platform.dal.dao.task;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.ctrip.platform.dal.dao.DalClientFactory;
import com.ctrip.platform.dal.dao.DalHintEnum;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.DalResultCallback;
import com.ctrip.platform.dal.dao.ResultMerger;
import com.ctrip.platform.dal.dao.client.DalLogger;
import com.ctrip.platform.dal.dao.client.LogContext;
import com.ctrip.platform.dal.exceptions.DalException;
import com.ctrip.platform.dal.exceptions.ErrorCode;

/**
 * Common request executor that support execute request that is of pojo or
 * sql in single, all or multiple shards
 * 
 * @author jhhe
 */
public class DalRequestExecutor extends AbstractRequestExecutor {

	@Override
	protected  <T> T internalExecute(DalHints hints, DalRequest<T> request, boolean nullable) throws SQLException {
		T result = null;
		Throwable error = null;
		
		LogContext logContext = logger.start(request);
		
		try {
			request.validateAndPrepare();
			
			if(request.isCrossShard())
				result = crossShardExecute(logContext, hints, request);
			else
				result = nonCrossShardExecute(logContext, hints, request);

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

	private <T> T nonCrossShardExecute(LogContext logContext, DalHints hints, DalRequest<T> request) throws Exception {
        logContext.setSingleTask(true);
	    Callable<T> task = new RequestTaskWrapper<T>(NA, request.createTask(), logContext);
		return task.call();
	}
	
	private <T> T crossShardExecute(LogContext logContext, DalHints hints, DalRequest<T> request) throws Exception {
        Map<String, Callable<T>> tasks = request.createTasks();
        logContext.setShards(tasks.keySet());

        boolean isSequentialExecution = hints.is(DalHintEnum.sequentialExecution);
        logContext.setSeqencialExecution(isSequentialExecution);
        
        ResultMerger<T> merger = request.getMerger();
        
	    logger.startCrossShardTasks(logContext, isSequentialExecution);
		
		T result = null;
		Throwable error = null;

		try {
            result = isSequentialExecution?
            		seqncialExecute(hints, tasks, merger, logContext):
            		parallelExecute(hints, tasks, merger, logContext);

        } catch (Throwable e) {
            error = e;
        }
		
		logger.endCrossShards(logContext, error);

		if(error != null)
            throw DalException.wrap(error);
		
		return result;

	}

	private <T> T parallelExecute(DalHints hints, Map<String, Callable<T>> tasks, ResultMerger<T> merger, LogContext logContext) throws SQLException {
		Map<String, Future<T>> resultFutures = new HashMap<>();
		
		for(final String shard: tasks.keySet())
			resultFutures.put(shard, serviceRef.get().submit(new RequestTaskWrapper<T>(shard, tasks.get(shard), logContext)));

		for(Map.Entry<String, Future<T>> entry: resultFutures.entrySet()) {
			try {
				merger.addPartial(entry.getKey(), entry.getValue().get());
			} catch (Throwable e) {
				hints.handleError("There is error during parallel execution: ", e);
			}
		}
		
		return merger.merge();
	}

	private <T> T seqncialExecute(DalHints hints, Map<String, Callable<T>> tasks, ResultMerger<T> merger, LogContext logContext) throws SQLException {
		for(final String shard: tasks.keySet()) {
			try {
				merger.addPartial(shard, new RequestTaskWrapper<T>(shard, tasks.get(shard), logContext).call());
			} catch (Throwable e) {
				hints.handleError("There is error during sequential execution: ", e);
			}
		}
		
		return merger.merge();
	}
}