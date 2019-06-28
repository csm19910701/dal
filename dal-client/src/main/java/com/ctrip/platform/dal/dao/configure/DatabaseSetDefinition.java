//package com.ctrip.platform.dal.dao.configure;
//
//import com.ctrip.framework.dal.cluster.client.config.ClusterConfigProvider;
//import com.ctrip.framework.dal.cluster.client.config.ClusterDefinition;
//
//public class DatabaseSetDefinition implements ClusterDefinition {
//    private DatabaseSet databaseSet;
//
//    public DatabaseSetDefinition(DatabaseSet databaseSet) {
//        this.databaseSet = databaseSet;
//    }
//
//    @Override
//    public String getClusterId() {
//        return databaseSet.getName();
//    }
//
//    @Override
//    public ClusterConfigProvider getClusterConfigProvider() {
//        return new DatabaseSetConfigProvider(databaseSet);
//    }
//}
