import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, existsSync } from 'fs';
import { rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

import { FileSystemRequestQueueClient } from '../lib.js';

describe('FileSystemRequestQueueClient', () => {
    let storageDir: string;

    beforeEach(() => {
        storageDir = mkdtempSync(join(tmpdir(), 'crawlee-rq-test-'));
    });

    afterEach(async () => {
        await rm(storageDir, { recursive: true, force: true }).catch(() => {});
    });

    it('should add and fetch a request', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        const response = await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'https://example.com',
                    url: 'https://example.com',
                    method: 'GET',
                },
            ],
            false,
        );

        expect(response.processedRequests.length).toBe(1);
        expect(response.processedRequests[0].wasAlreadyPresent).toBe(false);

        // Fetch the request
        const fetched = await client.fetchNextRequest();
        expect(fetched).not.toBeNull();
        expect(fetched!.url).toBe('https://example.com');

        // Queue should have no more requests to fetch
        const next = await client.fetchNextRequest();
        expect(next).toBeNull();
    });

    it('should deduplicate requests', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        const req = {
            uniqueKey: 'https://example.com',
            url: 'https://example.com',
            method: 'GET',
        };

        await client.addBatchOfRequests([req], false);
        const response = await client.addBatchOfRequests([req], false);

        expect(response.processedRequests[0].wasAlreadyPresent).toBe(true);
    });

    it('should mark request as handled', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'req1',
                    url: 'https://example.com/1',
                    method: 'GET',
                },
            ],
            false,
        );

        const request = await client.fetchNextRequest();
        expect(request).not.toBeNull();

        const result = await client.markRequestAsHandled(request!);
        expect(result).not.toBeNull();

        expect(await client.isEmpty()).toBe(true);
    });

    it('should reclaim a request', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'req1',
                    url: 'https://example.com/1',
                    method: 'GET',
                },
            ],
            false,
        );

        const request = await client.fetchNextRequest();
        expect(request).not.toBeNull();

        // Reclaim it
        const result = await client.reclaimRequest(request!, false);
        expect(result).not.toBeNull();

        // Should be fetchable again
        const refetched = await client.fetchNextRequest();
        expect(refetched).not.toBeNull();
    });

    it('should handle forefront requests', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        // Add regular request first
        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'regular',
                    url: 'https://example.com/regular',
                    method: 'GET',
                },
            ],
            false,
        );

        // Add forefront request
        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'priority',
                    url: 'https://example.com/priority',
                    method: 'GET',
                },
            ],
            true,
        );

        // Forefront should come first
        const first = await client.fetchNextRequest();
        expect(first!.uniqueKey).toBe('priority');
    });

    it('should get request by unique_key', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'req1',
                    url: 'https://example.com/1',
                    method: 'GET',
                },
            ],
            false,
        );

        const request = await client.getRequest('req1');
        expect(request).not.toBeNull();
        expect(request!.url).toBe('https://example.com/1');

        // Non-existent request
        const missing = await client.getRequest('nonexistent');
        expect(missing).toBeNull();
    });

    it('should report isEmpty / isFinished correctly', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        expect(await client.isEmpty()).toBe(true);
        expect(await client.isFinished()).toBe(true);

        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'req1',
                    url: 'https://example.com/1',
                    method: 'GET',
                },
            ],
            false,
        );

        // Pending request => not empty, not finished.
        expect(await client.isEmpty()).toBe(false);
        expect(await client.isFinished()).toBe(false);

        // Fetch (locks it).
        const request = await client.fetchNextRequest();

        // Only a locked/in-progress request remains:
        // - isEmpty() is TRUE (nothing fetchable right now).
        // - isFinished() is FALSE (work is still outstanding).
        expect(await client.isEmpty()).toBe(true);
        expect(await client.isFinished()).toBe(false);

        await client.markRequestAsHandled(request!);
        expect(await client.isEmpty()).toBe(true);
        expect(await client.isFinished()).toBe(true);
    });

    it('should expose a non-null requestId on processed requests', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        const response = await client.addBatchOfRequests(
            [{ uniqueKey: 'abc', url: 'https://example.com/abc', method: 'GET' }],
            false,
        );

        const processed = response.processedRequests[0];
        expect(typeof processed.requestId).toBe('string');
        expect(processed.requestId.length).toBeGreaterThan(0);
    });

    it('should persist orderNo and id inside the request file', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        await client.addBatchOfRequests(
            [{ uniqueKey: 'req1', url: 'https://example.com/1', method: 'GET' }],
            false,
        );

        const stored = await client.getRequest('req1');
        expect(stored).not.toBeNull();
        expect(typeof stored!.id).toBe('string');
        expect(typeof stored!.orderNo).toBe('number');
    });

    it('should purge all requests', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'req1',
                    url: 'https://example.com/1',
                    method: 'GET',
                },
                {
                    uniqueKey: 'req2',
                    url: 'https://example.com/2',
                    method: 'GET',
                },
            ],
            false,
        );

        const meta = await client.getMetadata();
        expect(meta.totalRequestCount).toBe(2);

        await client.purge();

        const metaAfter = await client.getMetadata();
        expect(metaAfter.totalRequestCount).toBe(0);
        expect(await client.isEmpty()).toBe(true);
    });

    it('should drop storage entirely', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'req1',
                    url: 'https://example.com/1',
                    method: 'GET',
                },
            ],
            false,
        );

        await client.dropStorage();
        expect(existsSync(client.pathToRq)).toBe(false);
    });

    it('should persist and restore state across reopen', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        await client.addBatchOfRequests(
            [
                {
                    uniqueKey: 'req1',
                    url: 'https://example.com/1',
                    method: 'GET',
                },
                {
                    uniqueKey: 'req2',
                    url: 'https://example.com/2',
                    method: 'GET',
                },
            ],
            false,
        );

        const meta = await client.getMetadata();
        expect(meta.totalRequestCount).toBe(2);

        // Reopen
        const client2 = await FileSystemRequestQueueClient.open(null, null, null, storageDir);
        const meta2 = await client2.getMetadata();
        expect(meta2.totalRequestCount).toBe(2);
        expect(meta2.pendingRequestCount).toBe(2);
    });

    it('should return metadata with correct fields', async () => {
        const client = await FileSystemRequestQueueClient.open(null, null, null, storageDir);

        const meta = await client.getMetadata();
        expect(meta.id).toBeTruthy();
        expect(meta.createdAt).toBeTruthy();
        expect(meta.modifiedAt).toBeTruthy();
        expect(meta.accessedAt).toBeTruthy();
        expect(meta.hadMultipleClients).toBe(false);
        expect(meta.handledRequestCount).toBe(0);
        expect(meta.pendingRequestCount).toBe(0);
        expect(meta.totalRequestCount).toBe(0);
    });

    it('should handle alias vs name correctly', async () => {
        const named = await FileSystemRequestQueueClient.open(null, 'my-queue', null, storageDir);
        expect((await named.getMetadata()).name).toBe('my-queue');

        const aliased = await FileSystemRequestQueueClient.open(null, null, 'my-alias', storageDir);
        expect((await aliased.getMetadata()).name).toBeNull();
    });
});
