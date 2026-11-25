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


import io.nats.client.api.StorageType;

public class NatsConfig {
    public String natsHostUrl;

    public int replicationFactor;

    public StorageType storageType = StorageType.File;

    // -1 is unlimited
    public long maxBytes = -1;

    /** Which NATS mode to use (jetstream or core). */
    public String mode = "jetstream";
    /**
     * Whether all producers should share a connection ("shared") or have a unique one each
     * ("per-producer").
     */
    public String producerConnection = "shared";
    /**
     * Whether all consumers should share a connection ("shared") or have a unique one each
     * ("per-consumer").
     */
    public String consumerConnection = "shared";
    /** 0 is unbounded. */
    public int maxPendingPublishes = 0;
    /** 0 is the server default of 1000. */
    public long maxAckPending = 0;
    /** The message ack interval. <=1 means ack every message. */
    public int ackEvery = 0;
}
