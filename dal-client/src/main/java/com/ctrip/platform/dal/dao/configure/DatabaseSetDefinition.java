package com.ctrip.platform.dal.dao.configure;

import com.ctrip.framework.dal.cluster.client.config.ClusterConfigProvider;
import com.ctrip.framework.dal.cluster.client.config.ClusterDefinition;

public class DatabaseSetDefinition implements ClusterDefinition {
    private String databaseSetName;

    public DatabaseSetDefinition(String databaseSetName) {
        this.databaseSetName = databaseSetName;
    }

    @Override
    public String getClusterId() {
        return databaseSetName;
    }

    @Override
    public ClusterConfigProvider getClusterConfigProvider() {
        return new DatabaseSetConfigProvider(databaseSetName);
    }
}
