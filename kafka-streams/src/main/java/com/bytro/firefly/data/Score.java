package com.bytro.firefly.data;

/**
 * Created by yoldeta on 2016-11-20.
 */
public class Score {
    private int userID;
    private int gameID;
    private String scoreID;
    private int scoreValue;

    public Score(int userID, int gameID, String scoreID, int scoreValue) {
        this.userID = userID;
        this.gameID = gameID;
        this.scoreID = scoreID;
        this.scoreValue = scoreValue;
    }

    public int getUserID() {
        return userID;
    }

    public Score setUserID(int userID) {
        this.userID = userID;
        return this;
    }

    public int getGameID() {
        return gameID;
    }

    public Score setGameID(int gameID) {
        this.gameID = gameID;
        return this;
    }

    public String getScoreID() {
        return scoreID;
    }

    public Score setScoreID(String scoreID) {
        this.scoreID = scoreID;
        return this;
    }

    public int getScoreValue() {
        return scoreValue;
    }

    public Score setScoreValue(int scoreValue) {
        this.scoreValue = scoreValue;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Score score = (Score) o;

        if (userID != score.userID) return false;
        if (gameID != score.gameID) return false;
        if (scoreValue != score.scoreValue) return false;
        return scoreID.equals(score.scoreID);

    }

    @Override
    public int hashCode() {
        int result = userID;
        result = 31 * result + gameID;
        result = 31 * result + scoreID.hashCode();
        result = 31 * result + scoreValue;
        return result;
    }
}
