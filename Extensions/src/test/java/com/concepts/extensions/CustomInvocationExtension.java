package com.concepts.extensions;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;

public class CustomInvocationExtension implements InvocationInterceptor {

    @Override
    public void interceptTestMethod(Invocation<Void> invocation,
                                    ReflectiveInvocationContext<Method> context,
                                    ExtensionContext extensionContext) throws Throwable {

        System.out.println("Pre-processing before test");
        invocation.proceed();  // actual test runs here
        System.out.println("Post-processing after test");
    }
}


