package com.bytro.firefly;

/**
 * Created by yoldeta on 2016-11-20.
 */
public class Launcher {
    public static void main(String... args) {
        System.out.println("hello");
        new Launcher().launch();
    }

    public void launch() {
        startStream();
        startRest();
    }

    private void startStream() {
        new Streamer().launch();
    }

    private void startRest() {
        new Rester().launch();
    }
}
