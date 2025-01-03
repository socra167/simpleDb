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
     * SQL문의 Statement, 인자값 추가
     * @param statement SQL statement
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
     * INSERT문 실행
     * @return AUTO_INCREMENT 에 의해서 생성된 주키
     */
    public long insert() {
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString(), Statement.RETURN_GENERATED_KEYS);
            setObjectsToStatement(psmt);
            psmt.executeUpdate();
            try (ResultSet generatedKeys = psmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    // 자동 생성된 키 값 (AUTO_INCREMENT id)
                    long generatedId = generatedKeys.getLong(1);
                    conn.close();
                    return generatedId;
                }
                conn.close();
            }
        } catch (SQLException e) {
            logger.warning("Failed to execute INSERT query : " + e.getMessage());
        }
        return 0;
    }

    /**
     * UPDATE문 실행
     * @return 수정된 row 개수
     */
    public int update() {
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            int affectedRow = psmt.executeUpdate();
            psmt.close();
            conn.close();
            return affectedRow;
        } catch (SQLException e) {
            logger.warning("Failed to execute UPDATE query : " + e.getMessage());
            return 0;
        }
    }

    /**
     * DELETE문 실행
     * @return 삭제된 row 개수
     */
    public int delete() {
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            int affectedRow = psmt.executeUpdate();
            psmt.close();
            conn.close();
            return affectedRow;
        } catch (SQLException e) {
            logger.warning("Failed to execute DELETE query : " + e.getMessage());
            return 0;
        }
    }

    /**
     * 하나의 Row에 대한 SELECT문 실행
     * @return SELECT 결과 Map
     */
    public Map<String, Object> selectRow() {
        Map<String, Object> resultMap = new HashMap<>();
        ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnSize = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columnSize; i++) {
                    resultMap.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
                }
            }
            close(rs, psmt);
            return resultMap;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return resultMap;
    }

    /**
     * 여러 Row에 대한 SELECT문 실행
     * @return SELECT 결과 Map의 List
     */
    public List<Map<String, Object>> selectRows() {
        List<Map<String, Object>> resultList = new ArrayList<>();
        Map<String, Object> resultMap;
        ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnSize = rsmd.getColumnCount();
            while (rs.next()) {
                resultMap = new HashMap<>();
                for (int i = 0; i < columnSize; i++) {
                    resultMap.put(rsmd.getColumnName(i + 1), rs.getObject(i + 1));
                }
                resultList.add(resultMap);
            }
            close(rs, psmt);
            return resultList;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return resultList;
    }

    /**
     * 결과값이 Long인 SELECT문 실행
     * @return SELECT 결과
     */
    public Long selectLong() {
        Long result = 0L;
        ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            rs.next();
            result = rs.getLong(1);
            close(rs, psmt);
            return result;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return result;
    }

    private void close(ResultSet rs, PreparedStatement psmt) throws SQLException {
        rs.close();
        psmt.close();
        conn.close();
    }

    public String selectString() {
        String result = "";
        ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            rs.next();
            result = rs.getString(1);
            close(rs, psmt);
            return result;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return result;
    }

    public Boolean selectBoolean() {
        Boolean result = false;
        ResultSet rs;
        try {
            PreparedStatement psmt = conn.prepareStatement(statementBuilder.toString());
            setObjectsToStatement(psmt);
            // ResultMap에 조회된 데이터 추가
            rs = psmt.executeQuery();
            rs.next();
            result = rs.getBoolean(1);
            close(rs, psmt);
            return result;
        } catch (SQLException e) {
            logger.warning("Failed to execute SELECT query : " + e.getMessage());
        }
        return result;
    }

    public LocalDateTime selectDatetime() {
        return LocalDateTime.now();
    }

    public List<Long> selectLongs() {
        return new ArrayList<>();
    }

    public List<Article> selectRows(Class<Article> articleClass) {
        return new ArrayList<>();
    }

    public Article selectRow(Class<Article> articleClass) {
        return new Article();
    }

    private void setObjectsToStatement(PreparedStatement psmt) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            psmt.setObject(i + 1, params.get(i));
        }
    }
}
