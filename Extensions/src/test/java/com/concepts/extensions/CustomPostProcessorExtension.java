package com.concepts.extensions;

import com.concepts.MyServiceTest;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

public class CustomPostProcessorExtension implements TestInstancePostProcessor {

    @Override
    public void postProcessTestInstance(Object testObj, ExtensionContext context) {
        if (testObj instanceof MyServiceTest) {
            MyServiceTest myServiceTestObj = (MyServiceTest) testObj;
        }
    }
}


