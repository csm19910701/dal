package com.ctrip.platform.dal.dao.configure;

import com.ctrip.framework.dal.cluster.client.datasource.DataSourceConfig;
import com.ctrip.platform.dal.common.enums.DatabaseCategory;
import com.ctrip.platform.dal.dao.helper.EncryptionHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class DataSourceConfigure
        implements DataSourceConfigureConstants, ConnectionStringConfigure, PoolPropertiesConfigure, DataSourceConfig {
    private String name;
    private Properties properties = new Properties();
    private String version;
    private DalConnectionString connectionString;

    private static final String MYSQL_URL_PREFIX = "jdbc:mysql://";

    public DataSourceConfigure() {}

    public DataSourceConfigure(String name) {
        this.name = name;
    }

    public DataSourceConfigure(String name, Properties properties) {
        this(name);
        this.properties = properties;
    }

    public DataSourceConfigure(String name, Map<String, String> propertyMap) {
        this(name);
        merge(propertyMap);
    }

    public void merge(Properties properties) {
        for (Object keyObj : properties.keySet()) {
            String key = (String) keyObj;
            setProperty(key, properties.getProperty(key));
        }
    }

    public void merge(Map<String, String> propertyMap) {
        if (propertyMap != null) {
            for (Map.Entry<String, String> entry : propertyMap.entrySet())
                properties.setProperty(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getUserName() {
        return getProperty(USER_NAME);
    }

    @Override
    public String getUsername() {
        return getUserName();
    }

    public void setUserName(String userName) {
        setProperty(USER_NAME, userName);
    }

    @Override
    public String getPassword() {
        return getProperty(PASSWORD);
    }

    public void setPassword(String password) {
        setProperty(PASSWORD, password);
    }

    @Override
    public String getConnectionUrl() {
        return getProperty(CONNECTION_URL);
    }

    public void setConnectionUrl(String connectionUrl) {
        setProperty(CONNECTION_URL, connectionUrl);
    }

    @Override
    public String getDriverClass() {
        return getProperty(DRIVER_CLASS_NAME);
    }

    @Override
    public String getDriverClassName() {
        return getDriverClass();
    }

    public void setDriverClass(String driverClass) {
        setProperty(DRIVER_CLASS_NAME, driverClass);
    }

    @Override
    public String getVersion() {
        return version;
    }

    public void setHostName(String hostName) {
        setProperty(HOST_NAME, hostName);
    }

    @Override
    public String getHostName() {
        return getProperty(HOST_NAME);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public DalConnectionString getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(DalConnectionString connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    @Override
    public int getIntProperty(String key, int defaultValue) {
        return properties.containsKey(key) ? Integer.parseInt(getProperty(key)) : defaultValue;
    }

    @Override
    public long getLongProperty(String key, long defaultValue) {
        return properties.containsKey(key) ? Long.parseLong(getProperty(key)) : defaultValue;
    }

    @Override
    public boolean getBooleanProperty(String key, boolean defaultValue) {
        return properties.containsKey(key) ? Boolean.parseBoolean(getProperty(key)) : defaultValue;
    }

    public String toConnectionUrl() {
        return String.format("{ConnectionUrl:%s,Version:%s,CRC:%s}", getConnectionUrl(), version, getCRC());
    }

    public Properties toProperties() {
        Properties p = new Properties();
        Set<String> set = new HashSet<>();
        set.add(USER_NAME);
        set.add(PASSWORD);
        set.add(CONNECTION_URL);
        set.add(DRIVER_CLASS_NAME);
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (!set.contains(entry.getKey())) {
                p.setProperty(entry.getKey().toString(), entry.getValue().toString());
            }
        }

        return p;
    }

    public boolean dynamicPoolPropertiesEnabled() {
        if (properties == null || properties.isEmpty())
            return false;

        String value = properties.getProperty(ENABLE_DYNAMIC_POOL_PROPERTIES);
        if (value == null)
            return false;

        return Boolean.parseBoolean(value);
    }

    public DatabaseCategory getDatabaseCategory() {
        if (getConnectionUrl().startsWith(MYSQL_URL_PREFIX)) {
            return DatabaseCategory.MySql;
        }

        return DatabaseCategory.SqlServer;
    }

    // Rule: username concat password,and then take 8 characters of md5 code from beginning
    private String getCRC() {
        String crc = null;
        String userName = getUserName();
        String pass = getPassword();
        try {
            userName.concat(pass);
            crc = EncryptionHelper.getCRC(userName);
        } catch (Throwable e) {
        }
        return crc;
    }

}
