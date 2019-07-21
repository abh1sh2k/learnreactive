package com.abhishek.trending;

public class WordCount implements java.io.Serializable {
    private String word ;
    private int  count;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
