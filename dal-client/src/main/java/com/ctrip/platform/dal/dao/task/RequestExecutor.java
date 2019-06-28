package com.ctrip.platform.dal.dao.task;

import com.ctrip.platform.dal.dao.DalHints;

import java.sql.SQLException;

public interface RequestExecutor {
    <T> T execute(final DalHints hints, final DalRequest<T> request) throws SQLException;
    <T> T execute(final DalHints hints, final DalRequest<T> request, final boolean nullable) throws SQLException;
}
