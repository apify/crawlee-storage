import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, existsSync } from 'fs';
import { rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

import { FileSystemKeyValueStoreClient } from '../index.js';

describe('FileSystemKeyValueStoreClient', () => {
    let storageDir: string;

    beforeEach(() => {
        storageDir = mkdtempSync(join(tmpdir(), 'crawlee-kvs-test-'));
    });

    afterEach(async () => {
        await rm(storageDir, { recursive: true, force: true }).catch(() => {});
    });

    it('should create and set a JSON value', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('my-key', { hello: 'world' });

        const record = await client.getValue('my-key');
        expect(record).not.toBeNull();
        expect(record!.key).toBe('my-key');
        expect(record!.content_type).toBe('application/json');
        expect(record!.value).toEqual({ hello: 'world' });
    });

    it('should set a text value', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('greeting', 'hello', 'text/plain');

        const record = await client.getValue('greeting');
        expect(record).not.toBeNull();
        expect(record!.content_type).toBe('text/plain');
        expect(record!.value).toBe('hello');
    });

    it('should handle null values', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('empty', null);

        const record = await client.getValue('empty');
        expect(record).not.toBeNull();
        expect(record!.content_type).toBe('application/x-none');
        expect(record!.value).toBeNull();
    });

    it('should delete a value', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('key1', 42);
        expect(await client.recordExists('key1')).toBe(true);

        await client.deleteValue('key1');
        expect(await client.recordExists('key1')).toBe(false);
    });

    it('should return null for missing keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const record = await client.getValue('nonexistent');
        expect(record).toBeNull();
    });

    it('should handle binary values via setValueBuffer', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x89, 0xff]);
        await client.setValueBuffer('binary-key', data, 'application/octet-stream');

        const record = await client.getValue('binary-key');
        expect(record).not.toBeNull();
        expect(record!.content_type).toBe('application/octet-stream');

        // Binary values come back with __binary__ marker
        expect(record!.__binary__).toBe(true);
        // Value is an array of byte values
        const bytes = record!.value;
        expect(bytes).toEqual([0x00, 0x01, 0x02, 0x03, 0x89, 0xff]);
    });

    it('should iterate keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', 1);
        await client.setValue('beta', 2);
        await client.setValue('gamma', 3);

        const iterator = await client.iterateKeys(null, null, 2);
        const keys: string[] = [];
        let entry;
        while ((entry = await iterator.next()) !== null) {
            keys.push(entry.key);
        }

        expect(keys.length).toBe(3);
        // Keys should be sorted
        expect(keys).toEqual([...keys].sort());
    });

    it('should iterate keys with limit', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', 1);
        await client.setValue('beta', 2);
        await client.setValue('gamma', 3);

        const iterator = await client.iterateKeys(null, 2, 1000);
        const keys: string[] = [];
        let entry;
        while ((entry = await iterator.next()) !== null) {
            keys.push(entry.key);
        }

        expect(keys.length).toBe(2);
    });

    it('should get public URL', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const url = await client.getPublicUrl('my-key');
        expect(url).toMatch(/^file:\/\//);
        expect(url).toContain('my%2Dkey');
    });

    it('should purge all values but keep metadata', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('key1', 1);
        await client.setValue('key2', 2);
        expect(await client.recordExists('key1')).toBe(true);

        await client.purge();
        expect(await client.recordExists('key1')).toBe(false);
        expect(await client.recordExists('key2')).toBe(false);
        expect(existsSync(client.pathToMetadata)).toBe(true);
    });

    it('should drop storage entirely', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('key1', 1);
        await client.dropStorage();

        expect(existsSync(client.pathToKvs)).toBe(false);
    });

    it('should handle special characters in keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('path/to/key with spaces', 'value', 'text/plain');

        const record = await client.getValue('path/to/key with spaces');
        expect(record).not.toBeNull();
        expect(record!.value).toBe('value');
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
        expect((await aliased.getMetadata()).name).toBeNull();
    });
});
