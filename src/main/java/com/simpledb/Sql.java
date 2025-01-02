package com.simpledb;

import com.simpledb.Entity.Article;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Sql {

    private StringBuilder statementBuilder;
    Logger logger = Logger.getLogger(this.getClass().getName());

    public Sql() {
        this.statementBuilder = new StringBuilder();
    }

    public Sql append(final String statement) {
        statementBuilder.append(statement);
        return this;
    }

    /**
     * SQL문에 포함된 '?'를 파라미터 값으로 대체
     * @param statement PreparedStatement
     * @param objects values
     */
    public Sql append(final String statement, final Object ... objects) {
        StringBuilder stringBuilder = new StringBuilder(statement);
        try {
            for (Object object : objects) {
                int index = stringBuilder.indexOf("?");
                stringBuilder.replace(index, index + 1, object.toString());
            }
        } catch (Exception e) {
            logger.warning("인자의 수가 불일치하거나 인자를 문자로 바꿀 수 없습니다");
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

        return 0L;
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
