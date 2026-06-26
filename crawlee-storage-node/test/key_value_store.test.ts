import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, existsSync } from 'fs';
import { rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

import { FileSystemKeyValueStoreClient } from '../lib.js';

describe('FileSystemKeyValueStoreClient', () => {
    let storageDir: string;

    beforeEach(() => {
        storageDir = mkdtempSync(join(tmpdir(), 'crawlee-kvs-test-'));
    });

    afterEach(async () => {
        await rm(storageDir, { recursive: true, force: true }).catch(() => {});
    });

    it('should set and get a JSON value as Buffer', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from(JSON.stringify({ hello: 'world' }));
        await client.setValue('my-key', data, 'application/json');

        const record = await client.getValue('my-key');
        expect(record).not.toBeNull();
        expect(record!.key).toBe('my-key');
        expect(record!.contentType).toBe('application/json');
        expect(Buffer.isBuffer(record!.value)).toBe(true);
        expect(JSON.parse(record!.value.toString())).toEqual({ hello: 'world' });
    });

    it('should set and get a text value as Buffer', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('greeting', Buffer.from('hello'), 'text/plain');

        const record = await client.getValue('greeting');
        expect(record).not.toBeNull();
        expect(record!.contentType).toBe('text/plain');
        expect(record!.value.toString()).toBe('hello');
    });

    it('should set and get binary data', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x89, 0xff]);
        await client.setValue('binary-key', data, 'application/octet-stream');

        const record = await client.getValue('binary-key');
        expect(record).not.toBeNull();
        expect(record!.contentType).toBe('application/octet-stream');
        expect(Buffer.isBuffer(record!.value)).toBe(true);
        expect(record!.value).toEqual(data);
    });

    it('should default content type to application/octet-stream', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
        await client.setValue('binary-key', data);

        const record = await client.getValue('binary-key');
        expect(record).not.toBeNull();
        expect(record!.contentType).toBe('application/octet-stream');
        expect(record!.value).toEqual(data);
    });

    it('should delete a value', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('key1', Buffer.from('42'));
        expect(await client.recordExists('key1')).toBe(true);

        await client.deleteValue('key1');
        expect(await client.recordExists('key1')).toBe(false);
    });

    it('should return null for missing keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const record = await client.getValue('nonexistent');
        expect(record).toBeNull();
    });

    it('should iterate keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));
        await client.setValue('beta', Buffer.from('2'));
        await client.setValue('gamma', Buffer.from('3'));

        const iterator = await client.iterateKeys(null, null, 2);
        const keys: string[] = [];
        let entry;
        while ((entry = await iterator.next()) !== null) {
            keys.push(entry.key);
        }

        expect(keys.length).toBe(3);
        expect(keys).toEqual([...keys].sort());
    });

    it('should support for-await-of on key iterator', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));
        await client.setValue('beta', Buffer.from('2'));
        await client.setValue('gamma', Buffer.from('3'));

        const iterator = await client.iterateKeys(null, null, 2);
        const keys: string[] = [];
        for await (const entry of iterator) {
            keys.push(entry.key);
        }

        expect(keys.length).toBe(3);
        expect(keys).toEqual([...keys].sort());
    });

    it('should iterate keys with limit', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));
        await client.setValue('beta', Buffer.from('2'));
        await client.setValue('gamma', Buffer.from('3'));

        const iterator = await client.iterateKeys(null, 2, 1000);
        const keys: string[] = [];
        let entry;
        while ((entry = await iterator.next()) !== null) {
            keys.push(entry.key);
        }

        expect(keys.length).toBe(2);
    });

    it('should iterate keys filtered by prefix', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('foo:1', Buffer.from('1'));
        await client.setValue('foo:2', Buffer.from('2'));
        await client.setValue('bar:1', Buffer.from('3'));

        // prefix is the 4th positional arg (exclusiveStartKey, limit, pageSize, prefix).
        const iterator = await client.iterateKeys(null, null, 1000, 'foo:');
        const keys: string[] = [];
        for await (const entry of iterator) {
            keys.push(entry.key);
        }

        expect(keys).toEqual(['foo:1', 'foo:2']);
    });

    it('should get public URL', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const url = await client.getPublicUrl('my-key');
        expect(url).toMatch(/^file:\/\//);
        // Hyphen is in the unreserved set (quote(safe='') keeps `. - _ ~`), so it
        // is preserved verbatim rather than encoded to %2D.
        expect(url).toContain('my-key');
    });

    it('should purge all values but keep metadata', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('key1', Buffer.from('1'));
        await client.setValue('key2', Buffer.from('2'));
        expect(await client.recordExists('key1')).toBe(true);

        await client.purge();
        expect(await client.recordExists('key1')).toBe(false);
        expect(await client.recordExists('key2')).toBe(false);
        expect(existsSync(client.pathToMetadata)).toBe(true);
    });

    it('should keep listed keys when purging with a keep list', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('INPUT', Buffer.from('in'));
        await client.setValue('other', Buffer.from('x'));

        await client.purge(['INPUT']);
        expect(await client.recordExists('INPUT')).toBe(true);
        expect(await client.recordExists('other')).toBe(false);
        expect(existsSync(client.pathToMetadata)).toBe(true);
    });

    it('should read a sidecar-less file only when requireRecordMetadata is false', async () => {
        const { writeFile } = await import('fs/promises');
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        // Hand-place a bare value file (no metadata sidecar), like a CLI-written INPUT.json.
        const payload = Buffer.from(JSON.stringify({ foo: 'bar' }));
        await writeFile(join(client.pathToKvs, 'INPUT.json'), payload);

        // Default (strict): invisible.
        expect(await client.getValue('INPUT.json')).toBeNull();
        expect(await client.recordExists('INPUT.json')).toBe(false);

        // Opt-in: served with generic content type, no extension inference.
        const record = await client.getValue('INPUT.json', false);
        expect(record).not.toBeNull();
        expect(record!.key).toBe('INPUT.json');
        expect(record!.contentType).toBe('application/octet-stream');
        expect(Buffer.isBuffer(record!.value)).toBe(true);
        expect(record!.value.equals(payload)).toBe(true);
        expect(await client.recordExists('INPUT.json', false)).toBe(true);
    });

    it('should drop storage entirely', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('key1', Buffer.from('1'));
        await client.dropStorage();

        expect(existsSync(client.pathToKvs)).toBe(false);
    });

    it('should handle special characters in keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('path/to/key with spaces', Buffer.from('value'), 'text/plain');

        const record = await client.getValue('path/to/key with spaces');
        expect(record).not.toBeNull();
        expect(record!.value.toString()).toBe('value');
    });

    it('should handle alias vs name correctly', async () => {
        const named = await FileSystemKeyValueStoreClient.open(null, 'my-store', null, storageDir);
        expect((await named.getMetadata()).name).toBe('my-store');

        const aliased = await FileSystemKeyValueStoreClient.open(
            null,
            null,
            'my-alias',
            storageDir,
        );
        expect((await aliased.getMetadata()).name).toBeUndefined();
    });

    it('should return metadata datetimes as native Dates', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);
        const meta = await client.getMetadata();
        expect(meta.createdAt).toBeInstanceOf(Date);
        expect(meta.modifiedAt).toBeInstanceOf(Date);
        expect(meta.accessedAt).toBeInstanceOf(Date);
        expect(Number.isNaN(meta.createdAt.getTime())).toBe(false);
    });

    // ─── Streaming tests ─────────────────────────────────────────────────

    it('should stream a value via getValueStream', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from('hello streaming world');
        await client.setValue('stream-key', data, 'text/plain');

        const result = await client.getValueStream('stream-key');
        expect(result).not.toBeNull();
        expect(result!.key).toBe('stream-key');
        expect(result!.contentType).toBe('text/plain');
        expect(result!.size).toBe(data.length);

        // Read the entire stream
        const reader = result!.stream.getReader();
        const chunks: Uint8Array[] = [];
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
        }

        const combined = Buffer.concat(chunks);
        expect(combined).toEqual(data);
    });

    it('should return null from getValueStream for missing keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const result = await client.getValueStream('nonexistent');
        expect(result).toBeNull();
    });

    it('should write a value via setValueStream', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from('hello from a stream');
        const stream = new ReadableStream({
            start(controller) {
                controller.enqueue(data);
                controller.close();
            },
        });

        await client.setValueStream('stream-write-key', stream, 'text/plain');

        const record = await client.getValue('stream-write-key');
        expect(record).not.toBeNull();
        expect(record!.contentType).toBe('text/plain');
        expect(record!.value.toString()).toBe('hello from a stream');
    });

    it('should write multi-chunk data via setValueStream', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const chunk1 = new Uint8Array([0x00, 0x01, 0x02]);
        const chunk2 = new Uint8Array([0x03, 0x04, 0x05]);
        const stream = new ReadableStream({
            start(controller) {
                controller.enqueue(chunk1);
                controller.enqueue(chunk2);
                controller.close();
            },
        });

        await client.setValueStream('binary-stream', stream, 'application/octet-stream');

        const record = await client.getValue('binary-stream');
        expect(record).not.toBeNull();
        expect(record!.contentType).toBe('application/octet-stream');
        expect(record!.value).toEqual(Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05]));
    });

    it('should roundtrip via getValueStream', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const json = JSON.stringify({ hello: 'world' });
        await client.setValue('json-stream', Buffer.from(json), 'application/json');

        const result = await client.getValueStream('json-stream');
        expect(result).not.toBeNull();
        expect(result!.contentType).toBe('application/json');

        const reader = result!.stream.getReader();
        const chunks: Uint8Array[] = [];
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
        }

        const text = Buffer.concat(chunks).toString('utf-8');
        expect(JSON.parse(text)).toEqual({ hello: 'world' });
    });
});
