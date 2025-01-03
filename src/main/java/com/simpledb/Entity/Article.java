package com.simpledb.Entity;

import java.time.LocalDateTime;

public class Article {
    private Long id;
    private String title;
    private String body;
    private LocalDateTime createdDate;
    private LocalDateTime modifiedDate;

    public Article setId(Long id) {
        this.id = id;
        return this;
    }

    public Article setTitle(String title) {
        this.title = title;
        return this;
    }

    public Article setBody(String body) {
        this.body = body;
        return this;
    }

    public Article setCreatedDate(LocalDateTime createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public Article setModifiedDate(LocalDateTime modifiedDate) {
        this.modifiedDate = modifiedDate;
        return this;
    }

    public Long getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getBody() {
        return body;
    }

    public LocalDateTime getCreatedDate() {
        return createdDate;
    }

    public LocalDateTime getModifiedDate() {
        return modifiedDate;
    }

    public boolean isBlind() {
        return false;
    }
}
