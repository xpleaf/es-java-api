package cn.xpleaf.es._3doc.domain;

import java.util.Date;

public class User {
    private String user;
    private Date postDate;
    private String message;

    public User() {
    }

    public User(String user, Date postDate, String message) {
        this.user = user;
        this.postDate = postDate;
        this.message = message;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Date getPostDate() {
        return postDate;
    }

    public void setPostDate(Date postDate) {
        this.postDate = postDate;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
