package com.microsoft.azure.storage.samples;

import java.util.Random;

public class CancellationSample {
    public static void main(String[] args) {
        // Create 100 MB of random data.
        byte[] data = new byte[1024 * 1024 * 100];
        new Random().nextBytes(data);


    }
}
