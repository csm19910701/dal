package com.ctrip.platform.dal.dao.task;

import com.ctrip.platform.dal.dao.*;
import com.ctrip.platform.dal.dao.client.DalLogger;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractRequestExecutor implements RequestExecutor {
    protected static AtomicReference<ExecutorService> serviceRef = new AtomicReference<>();

    public static final String MAX_POOL_SIZE = "maxPoolSize";
    public static final String KEEP_ALIVE_TIME = "keepAliveTime";

    // To be consist with default connection max active size
    public static final int DEFAULT_MAX_POOL_SIZE = 500;

    public static final int DEFAULT_KEEP_ALIVE_TIME = 10;

    protected DalLogger logger = DalClientFactory.getDalLogger();

    protected final static String NA = "N/A";

    public static void init(String maxPoolSizeStr, String keepAliveTimeStr){
        if(serviceRef.get() != null)
            return;

        synchronized (DalRequestExecutor.class) {
            if(serviceRef.get() != null)
                return;

            int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
            if(maxPoolSizeStr != null)
                maxPoolSize = Integer.parseInt(maxPoolSizeStr);

            int keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;
            if(keepAliveTimeStr != null)
                keepAliveTime = Integer.parseInt(keepAliveTimeStr);

            ThreadPoolExecutor executer = new ThreadPoolExecutor(maxPoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
                AtomicInteger atomic = new AtomicInteger();
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "DalRequestExecutor-Worker-" + this.atomic.getAndIncrement());
                }
            });
            executer.allowCoreThreadTimeOut(true);

            serviceRef.set(executer);
        }
    }

    public static void shutdown() {
        if (serviceRef.get() == null)
            return;

        synchronized (DalRequestExecutor.class) {
            if (serviceRef.get() == null)
                return;

            serviceRef.get().shutdown();
            serviceRef.set(null);
        }
    }

    public <T> T execute(final DalHints hints, final DalRequest<T> request) throws SQLException {
        return execute(hints, request, false);
    }

    public <T> T execute(final DalHints hints, final DalRequest<T> request, final boolean nullable) throws SQLException {
        if (hints.isAsyncExecution()) {
            Future<T> future = serviceRef.get().submit(new Callable<T>() {
                public T call() throws Exception {
                    return internalExecute(hints, request, nullable);
                }
            });

            if(hints.isAsyncExecution())
                hints.set(DalHintEnum.futureResult, future);
            return null;
        }

        return internalExecute(hints, request, nullable);
    }

    abstract protected  <T> T internalExecute(DalHints hints, DalRequest<T> request, boolean nullable) throws SQLException;


    protected  <T> void handleCallback(final DalHints hints, T result, Throwable error) {
        DalResultCallback qc = (DalResultCallback)hints.get(DalHintEnum.resultCallback);
        if (qc == null)
            return;

        if(error == null)
            qc.onResult(result);
        else
            qc.onError(error);
    }

    public static int getPoolSize() {
        ThreadPoolExecutor executer = (ThreadPoolExecutor)serviceRef.get();
        if (serviceRef.get() == null)
            return 0;

        return executer.getPoolSize();
    }
}
