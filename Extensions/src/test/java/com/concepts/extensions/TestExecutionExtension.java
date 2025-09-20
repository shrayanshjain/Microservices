package com.concepts.extensions;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Instant;

public class TestExecutionExtension implements BeforeTestExecutionCallback,
        AfterTestExecutionCallback {

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        System.out.println("Starting test at: " + Instant.now());
    }

    @Override
    public void afterTestExecution(ExtensionContext context) {
        System.out.println("Finished test at: " + Instant.now());
    }
}
