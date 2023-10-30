package com.snowflake.connectors.sdk.example_github_connector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubIssue {
    private int id;
    private int number;
    private String title;
    private String state;
    private String url;


    public void setId(int id) {
        this.id = id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

    public int getId() {
        return id;
    }

    public String getNode_id() {
        return node_id;
    }

    private String node_id;

    public void setState(String state) {
        this.state = state;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }


    public String getState() {
        return state;
    }

    public String getUrl() {
        return url;
    }

    public boolean isLocked() {
        return locked;
    }

    private boolean locked;



    public GithubIssue() {
    }

    public int getNumber() {
        return number;
    }

    public String getTitle() {
        return title;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}
