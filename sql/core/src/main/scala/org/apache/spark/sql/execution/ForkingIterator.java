package org.apache.spark.sql.execution;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

// TODO size estimation
public final class ForkingIterator<T> implements Iterator<T> {

    private final LinkedBlockingDeque<T> buffer;
    private final Iterator<T> innerIterator;

    public ForkingIterator(Iterator<T> innerIterator, int capacity) {
        checkNotNull(innerIterator);

        this.buffer = new LinkedBlockingDeque<T>(capacity);
        this.innerIterator = new SynchronizedIterator<T>(innerIterator);
    }

    @Override
    public boolean hasNext() {
        return innerIterator.hasNext();
    }

    @Override
    public T next() {
        T obj = innerIterator.next();
        try {
            buffer.putLast(obj);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return obj;
    }

    public Iterator<T> forkIterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return !buffer.isEmpty() || ForkingIterator.this.hasNext();
            }

            @Override
            public T next() {
                try {
                    return buffer.takeFirst();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private static class SynchronizedIterator<T> implements Iterator<T> {

        private final Iterator<T> innerIterator;

        public SynchronizedIterator(Iterator<T> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public synchronized boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public synchronized T next() {
            return innerIterator.next();
        }

    }

}
