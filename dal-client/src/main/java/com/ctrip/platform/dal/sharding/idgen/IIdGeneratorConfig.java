package com.ctrip.platform.dal.sharding.idgen;

import com.ctrip.framework.dal.cluster.client.idgen.ClusterIdGenerator;

public interface IIdGeneratorConfig extends ClusterIdGenerator {

    IdGenerator getIdGenerator(String name);

    String getSequenceDbName();

    int warmUp();

}
