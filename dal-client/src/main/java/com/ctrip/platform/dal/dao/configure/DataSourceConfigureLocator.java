package com.ctrip.platform.dal.dao.configure;

import com.ctrip.platform.dal.common.enums.IPDomainStatus;
import com.ctrip.platform.dal.dao.helper.Ordered;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

public interface DataSourceConfigureLocator extends Ordered {
    void addUserPoolPropertiesConfigure(String name, DalPoolPropertiesConfigure configure);

    DalPoolPropertiesConfigure getUserPoolPropertiesConfigure(String name);

    DataSourceConfigure getDataSourceConfigure(String name);

    Set<String> getDataSourceConfigureKeySet();

    Map<String, DalConnectionString> getAllConnectionStrings();

    Map<String, DalConnectionString> getSuccessfulConnectionStrings();

    Map<String, DalConnectionString> getFailedConnectionStrings();

    void setIPDomainStatus(IPDomainStatus status);

    IPDomainStatus getIPDomainStatus();

    void setConnectionStrings(Map<String, DalConnectionString> map);

    DalConnectionString setConnectionString(String name, DalConnectionString connectionString);

    Properties setPoolProperties(DalPoolPropertiesConfigure configure);

    DataSourceConfigure mergeDataSourceConfigure(DalConnectionString connectionString);

}
