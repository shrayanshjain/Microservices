package com.concepts.extensions;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;

public class CustomExceptionHandler implements TestExecutionExceptionHandler {

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable)
            throws Throwable {

        System.out.println("Exception occurred in test: " + context.getDisplayName());
        System.out.println("Exception message: " + throwable.getMessage());

        // for now, just rethrowing the exception
        throw throwable;
    }
}


