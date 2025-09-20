package com.concepts.extensions;

import org.junit.jupiter.api.extension.*;

public class LifecycleExtension implements BeforeAllCallback, AfterAllCallback,
        BeforeEachCallback, AfterEachCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
        System.out.println("Extension: Before all tests");
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        System.out.println("Extension: After all tests");
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        System.out.println("Extension: After each tests");
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        System.out.println("Extension: Before each tests");
    }
}


