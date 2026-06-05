/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.io;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;

/**
 * Reflective access to {@code sun.misc.Unsafe.invokeCleaner} (and a Java 8 fallback path) is the only way to release a
 * direct {@link ByteBuffer}'s native memory eagerly without waiting for GC. The
 * {@link java.lang.reflect.AccessibleObject#setAccessible(boolean)} calls below are required for that and are
 * intentional; suppressing S3011 at the class level keeps the rest of the codebase honest about reflection usage.
 *
 * <p>{@code invokeCleaner} is valid only on a <em>root</em> direct buffer (one that owns its native memory). It throws
 * for slices, duplicates, and buffers viewing memory owned elsewhere, so this releaser must only be applied to buffers
 * the built-in backend itself allocated.
 */
@Slf4j
@SuppressWarnings("java:S3011")
final class DirectByteBufferCleaner {

    private static final Object UNSAFE;
    private static final Method INVOKE_CLEANER;

    static {
        Object unsafe = null;
        Method invokeCleaner = null;
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = theUnsafe.get(null);
            invokeCleaner = findInvokeCleaner(unsafeClass);
        } catch (Exception e) {
            log.warn("Could not get access to sun.misc.Unsafe. Direct buffer memory release will rely on GC.", e);
        }
        UNSAFE = unsafe;
        INVOKE_CLEANER = invokeCleaner;
    }

    /** Resolves {@code Unsafe.invokeCleaner}, available since Java 9; returns null on a runtime that lacks it. */
    private static Method findInvokeCleaner(Class<?> unsafeClass) {
        try {
            return unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        } catch (NoSuchMethodException e) {
            // Java 8 lacks invokeCleaner; not reached on Java 17, handled for safety.
            return null;
        }
    }

    private DirectByteBufferCleaner() {
        // no-op
    }

    /**
     * Attempts to immediately release the memory of a direct ByteBuffer.
     *
     * @param buffer the buffer to release
     */
    static void releaseDirectBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }

        if (UNSAFE != null && INVOKE_CLEANER != null) {
            try {
                INVOKE_CLEANER.invoke(UNSAFE, buffer);
                return;
            } catch (Exception e) {
                log.debug("Failed to invoke cleaner via Unsafe", e);
            }
        }

        // Fallback to classic reflection (Java 8 style) or if Unsafe failed.
        // This will likely fail on Java 17+ without --add-opens, but we try anyway.
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);

            if (cleaner != null) {
                Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            // InaccessibleObjectException (Java 9+) or other errors
            log.debug("Failed to release direct buffer memory via reflection", e);
        }
    }
}
