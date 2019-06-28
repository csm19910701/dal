package com.ctrip.platform.dal.dao.task;


import com.ctrip.framework.dal.cluster.client.Cluster;

import com.ctrip.framework.dal.cluster.client.DefaultClusterManager;
import com.ctrip.framework.dal.cluster.client.cluster.FakeCluster;
import com.ctrip.platform.dal.common.enums.ShardingCategory;
import com.ctrip.platform.dal.dao.*;
import java.sql.SQLException;
import static com.ctrip.platform.dal.dao.helper.DalShardingHelper.*;

/**
 * Created by lilj on 2018/9/12.
 */
public class BaseTaskAdapter {
    protected DalClient client;
    protected Cluster cluster;
    protected String logicDbName;
    public ShardingCategory shardingCategory;
    public boolean shardingEnabled;

    public void initialize(String logicDbName) {
        this.logicDbName = logicDbName;
        this.cluster = DalClientFactory.getCluster(logicDbName);
        if (cluster instanceof FakeCluster) {
            this.client = DalClientFactory.getClient(logicDbName);
            shardingEnabled = isShardingEnabled(logicDbName);
            initShardingCategory();
        }
    }

    public DefaultTaskContext createTaskContext() throws SQLException {
        DefaultTaskContext taskContext = new DefaultTaskContext();
        taskContext.setShardingCategory(shardingCategory);
        return taskContext;
    }

    public void initShardingCategory() {
        if (shardingEnabled)
            shardingCategory = ShardingCategory.DBShard;
        else
            shardingCategory = ShardingCategory.NoShard;
    }
}
