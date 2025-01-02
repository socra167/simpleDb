package com.simpledb;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

public class SimpleDb {

    private String url;
    private String user;
    private String password;
    private String database;
    private boolean devMode;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public SimpleDb(String url, String user, String password, String database) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.database = database;

        try {
            DriverManager.getConnection("jdbc:mysql://%s/%s?user=%s&password=%s".formatted(url, database, user, password));
        } catch (SQLException ex) {
            logger.warning("SQLException: " + ex.getMessage());
            logger.warning("SQLState: " + ex.getSQLState());
            logger.warning("VendorError: " + ex.getErrorCode());
        }
    }

    /**
     * SQL문을 실행한다
     */
    public void run(String statement) {
    }

    public void run(String s, Object... objects) {
    }

    public void setDevMode(boolean devMode) {
        this.devMode = devMode;
    }

    public Sql genSql() {
        return new Sql();
    }

    public void startTransaction() {
    }

    public void commit() {
    }

    public void rollback() {
    }

    public void close() {
    }
}
