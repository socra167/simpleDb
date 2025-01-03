package com.simpledb;

import com.simpledb.Entity.Article;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Sql {

    private Connection conn;
    private StringBuilder statementBuilder;
    List<Object> params;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public Sql(Connection conn) {
        this.conn = conn;
        this.statementBuilder = new StringBuilder();
        params = new ArrayList<>();
    }

    public Sql append(final String statement) {
        statementBuilder.append(statement);
        statementBuilder.append(' ');
        return this;
    }

    /**
     * SQL문에 포함된 '?'를 파라미터 값으로 대체
     * @param statement PreparedStatement
     * @param objects values
     */
    public Sql append(final String statement, final Object ... objects) {
        statementBuilder.append(statement);
        statementBuilder.append(' ');
        for (Object object : objects) {
            params.add(object);
        }
        return this;
    }

    public Sql appendIn(String statement, Object ... object) {
        return this;
    }

    /**
     *
     * @return AUTO_INCREMENT 에 의해서 생성된 주키
     */
    public long insert() {
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            for (int i = 0; i < params.size(); i++) {
                psmt.setObject(i + 1, params.get(i));
            }
            return psmt.executeUpdate();
        } catch (SQLException e) {
            logger.warning("Failed to execute INSERT query : " + e.getMessage());
            return 0;
        }
    }

    public int update() {
        return 0;
    }

    public int delete() {
        return 0;
    }

    public Map<String, Object> selectRow() {
        return new HashMap<>();
    }

    public LocalDateTime selectDatetime() {
        return LocalDateTime.now();
    }

    public Long selectLong() {
        return 0L;
    }

    public String selectString() {
        return "";
    }

    public Boolean selectBoolean() {
        return false;
    }

    public List<Long> selectLongs() {
        return new ArrayList<>();
    }

    public List<Map<String, Object>> selectRows() {
        return new ArrayList<>();
    }

    public List<Article> selectRows(Class<Article> articleClass) {
        return new ArrayList<>();
    }

    public Article selectRow(Class<Article> articleClass) {
        return new Article();
    }
}
