/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.nats;


import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import org.jspecify.annotations.Nullable;

public class NatsBenchmarkProducer implements BenchmarkProducer {
    private final String topic;

    private final Connection connection;
    private final @Nullable JetStream jetStream;
    private final @Nullable Semaphore pending;

    public NatsBenchmarkProducer(
            Connection connection, JetStream jetStream, String topic, int maxPending) {
        this.connection = connection;
        this.jetStream = jetStream;
        this.topic = topic;
        this.pending = maxPending > 0 ? new Semaphore(maxPending) : null;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        byte[] data = Arrays.copyOf(payload, payload.length);
        writeLongToBytes(System.currentTimeMillis(), data);
        if (jetStream == null) {
            try {
                connection.publish(topic, data);
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(e);
                return f;
            }
        }
        if (pending == null) {
            return jetStream.publishAsync(topic, data).thenApply(x -> null);
        }
        pending.acquireUninterruptibly();
        return jetStream
                .publishAsync(topic, data)
                .whenComplete((a, e) -> pending.release())
                .thenApply(x -> null);
    }

    @Override
    public void close() throws Exception {}

    public static void writeLongToBytes(long l, byte[] dst) {
        for (int i = 7; i >= 0; i--) {
            dst[i] = (byte) (l & 0xFF);
            l >>= 8;
        }
    }
}
