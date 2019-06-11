package com.ctrip.platform.dal.dao.configure;

import com.ctrip.framework.dal.cluster.client.cluster.ClusterConfig;
import com.ctrip.framework.dal.cluster.client.config.ClusterConfigChangedListener;
import com.ctrip.framework.dal.cluster.client.config.ClusterConfigProvider;
import com.ctrip.platform.dal.dao.DalClientFactory;

public class DatabaseSetConfigProvider implements ClusterConfigProvider {
    private DatabaseSet  databaseSet;

    public DatabaseSetConfigProvider(String databaseSetName){
        databaseSet= DalClientFactory.getDalConfigure().getDatabaseSet(databaseSetName);
    }

    @Override
    public ClusterConfig getClusterConfig() {
        return databaseSet;
    }

    @Override
    public void addListener(ClusterConfigChangedListener listener) {

    }
}
