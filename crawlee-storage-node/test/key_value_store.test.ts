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
        expect(record).not.toBeUndefined();
        expect(record!.key).toBe('my-key');
        expect(record!.contentType).toBe('application/json');
        expect(Buffer.isBuffer(record!.value)).toBe(true);
        expect(JSON.parse(record!.value.toString())).toEqual({ hello: 'world' });
    });

    it('should set and get a text value as Buffer', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('greeting', Buffer.from('hello'), 'text/plain');

        const record = await client.getValue('greeting');
        expect(record).not.toBeUndefined();
        expect(record!.contentType).toBe('text/plain');
        expect(record!.value.toString()).toBe('hello');
    });

    it('should set and get binary data', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x89, 0xff]);
        await client.setValue('binary-key', data, 'application/octet-stream');

        const record = await client.getValue('binary-key');
        expect(record).not.toBeUndefined();
        expect(record!.contentType).toBe('application/octet-stream');
        expect(Buffer.isBuffer(record!.value)).toBe(true);
        expect(record!.value).toEqual(data);
    });

    it('should default content type to application/octet-stream', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const data = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
        await client.setValue('binary-key', data);

        const record = await client.getValue('binary-key');
        expect(record).not.toBeUndefined();
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

    it('should return undefined for missing keys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const record = await client.getValue('nonexistent');
        expect(record).toBeUndefined();
    });

    it('should list keys across pages via listKeys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));
        await client.setValue('beta', Buffer.from('2'));
        await client.setValue('gamma', Buffer.from('3'));

        // Drain every key one page at a time, following nextExclusiveStartKey.
        const keys: string[] = [];
        let cursor: string | null | undefined = null;
        do {
            const page = await client.listKeys(cursor, 2);
            keys.push(...page.items.map((i) => i.key));
            cursor = page.isTruncated ? page.nextExclusiveStartKey : undefined;
        } while (cursor);

        expect(keys.length).toBe(3);
        expect(keys).toEqual([...keys].sort());
    });

    it('should honor a limit on listKeys', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));
        await client.setValue('beta', Buffer.from('2'));
        await client.setValue('gamma', Buffer.from('3'));

        // A single page capped at 2 keys.
        const page = await client.listKeys(null, 2);
        expect(page.items.length).toBe(2);
    });

    it('should list keys filtered by prefix', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('foo:1', Buffer.from('1'));
        await client.setValue('foo:2', Buffer.from('2'));
        await client.setValue('bar:1', Buffer.from('3'));

        // prefix is the 3rd positional arg (exclusiveStartKey, limit, prefix).
        const page = await client.listKeys(null, 1000, 'foo:');
        expect(page.items.map((i) => i.key)).toEqual(['foo:1', 'foo:2']);
    });

    it('should paginate from a valid exclusiveStartKey', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));
        await client.setValue('beta', Buffer.from('2'));
        await client.setValue('gamma', Buffer.from('3'));

        const page = await client.listKeys('beta', 1000);
        expect(page.items.map((i) => i.key)).toEqual(['gamma']);
    });

    it('should throw the crawlee contract error for an unknown exclusiveStartKey', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));

        await expect(client.listKeys('does-not-exist', 1000)).rejects.toThrow(
            'exclusiveStartKey "does-not-exist" was not found in the key-value store. ' +
                'This is likely a bug — the key may have been deleted between paginated listKeys calls.',
        );
    });

    it('listKeys returns a truncated page carrying nextExclusiveStartKey, then a final page', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'));
        await client.setValue('beta', Buffer.from('2'));
        await client.setValue('gamma', Buffer.from('3'));

        // Truncated first page: limit 2 of 3 keys.
        const page1 = await client.listKeys(null, 2);
        expect(page1.items.map((i) => i.key)).toEqual(['alpha', 'beta']);
        expect(page1.count).toBe(2);
        expect(page1.limit).toBe(2);
        expect(page1.exclusiveStartKey).toBeUndefined();
        expect(page1.isTruncated).toBe(true);
        expect(page1.nextExclusiveStartKey).toBe('beta');

        // Final page via the returned cursor: no truncation, no next cursor.
        const page2 = await client.listKeys(page1.nextExclusiveStartKey, 2);
        expect(page2.items.map((i) => i.key)).toEqual(['gamma']);
        expect(page2.count).toBe(1);
        expect(page2.exclusiveStartKey).toBe('beta');
        expect(page2.isTruncated).toBe(false);
        expect(page2.nextExclusiveStartKey).toBeUndefined();
    });

    it('should get public URL', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        // getPublicUrl is existence-aware: a missing record yields undefined.
        expect(await client.getPublicUrl('my-key')).toBeUndefined();

        // Once the record exists, it returns a file:// URL.
        await client.setValue('my-key', Buffer.from('v'));
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

    it('getValue/recordExists are strict: a sidecar-less file is invisible to them', async () => {
        const { writeFile } = await import('fs/promises');
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        // Hand-place a bare value file (no metadata sidecar), like a CLI-written INPUT.json.
        const payload = Buffer.from(JSON.stringify({ foo: 'bar' }));
        await writeFile(join(client.pathToKvs, 'INPUT.json'), payload);

        // getValue / recordExists only ever see tracked records (value + sidecar),
        // so a bare file is invisible to them — reaching it is resolveValue's job.
        expect(await client.getValue('INPUT.json')).toBeUndefined();
        expect(await client.recordExists('INPUT.json')).toBe(false);
    });

    it('listKeys surfaces caller-declared bare files alongside tracked records', async () => {
        const { writeFile } = await import('fs/promises');
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('alpha', Buffer.from('1'), 'application/json');
        const payload = Buffer.from(JSON.stringify({ foo: 'bar' }));
        await writeFile(join(client.pathToKvs, 'INPUT.json'), payload);

        // Without declaring the fallback, the bare file is invisible to listing.
        const bareless = await client.listKeys(null, 1000);
        expect(bareless.items.map((e) => e.key)).toEqual(['alpha']);

        // Declaring the bare file by its on-disk name surfaces it under that key.
        const fallbacks = [{ name: 'INPUT.json', contentType: 'application/json' }];
        const page = await client.listKeys(null, 1000, null, fallbacks);
        const entries = page.items;
        const input = entries.find((e) => e.key === 'INPUT.json');
        expect(input).toBeDefined();
        expect(input!.contentType).toBe('application/json');
        expect(input!.size).toBe(payload.length);
        expect(entries.map((e) => e.key).sort()).toEqual(['INPUT.json', 'alpha']);
    });

    it('resolveValue prefers a tracked record over bare-file fallbacks', async () => {
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        await client.setValue('INPUT', Buffer.from(JSON.stringify({ x: 1 })), 'application/json');

        const fallbacks = [
            { extension: '', contentType: '' },
            { extension: '.json', contentType: 'application/json; charset=utf-8' },
        ];
        const record = await client.resolveValue('INPUT', fallbacks);
        expect(record).not.toBeUndefined();
        expect(record!.key).toBe('INPUT');
        // The tracked sidecar content type wins — fallback types are NOT applied.
        expect(record!.contentType).toBe('application/json');
    });

    it('resolveValue falls back to a bare file and applies the declared content type', async () => {
        const { writeFile } = await import('fs/promises');
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const payload = Buffer.from(JSON.stringify({ foo: 'bar' }));
        await writeFile(join(client.pathToKvs, 'INPUT.json'), payload);

        const fallbacks = [
            { extension: '', contentType: '' },
            { extension: '.json', contentType: 'application/json; charset=utf-8' },
            { extension: '.bin', contentType: '' },
        ];
        const record = await client.resolveValue('INPUT', fallbacks);
        expect(record).not.toBeUndefined();
        // Re-keyed to the requested key, not the on-disk "INPUT.json".
        expect(record!.key).toBe('INPUT');
        expect(record!.contentType).toBe('application/json; charset=utf-8');
        expect(record!.value.equals(payload)).toBe(true);

        // Nothing resolves → undefined.
        expect(await client.resolveValue('missing', fallbacks)).toBeUndefined();
    });

    it('resolveExistingKey returns the matched on-disk key', async () => {
        const { writeFile } = await import('fs/promises');
        const client = await FileSystemKeyValueStoreClient.open(null, null, null, storageDir);

        const extensions = ['', '.json', '.txt', '.bin'];

        await client.setValue('tracked', Buffer.from('x'), 'text/plain');
        expect(await client.resolveExistingKey('tracked', extensions)).toBe('tracked');

        await writeFile(join(client.pathToKvs, 'INPUT.json'), Buffer.from('{}'));
        expect(await client.resolveExistingKey('INPUT', extensions)).toBe('INPUT.json');

        expect(await client.resolveExistingKey('nope', extensions)).toBeUndefined();
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
        expect(record).not.toBeUndefined();
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
        expect(record).not.toBeUndefined();
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
        expect(record).not.toBeUndefined();
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
