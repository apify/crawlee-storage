import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { mkdtempSync, existsSync } from 'fs';
import { rm } from 'fs/promises';
import { join } from 'path';
import { tmpdir } from 'os';

import { FileSystemDatasetClient } from '../index.js';

describe('FileSystemDatasetClient', () => {
    let storageDir: string;

    beforeEach(() => {
        storageDir = mkdtempSync(join(tmpdir(), 'crawlee-ds-test-'));
    });

    afterEach(async () => {
        await rm(storageDir, { recursive: true, force: true }).catch(() => {});
    });

    it('should create and push data', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            null,
            null,
            storageDir,
        );

        // Push a single item
        await client.pushData({ name: 'Alice', age: 30 });

        const meta = await client.getMetadata();
        expect(meta.item_count).toBe(1);

        // Push multiple items
        await client.pushData([
            { name: 'Bob', age: 25 },
            { name: 'Charlie', age: 35 },
        ]);

        const meta2 = await client.getMetadata();
        expect(meta2.item_count).toBe(3);
    });

    it('should paginate get_data', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            null,
            null,
            storageDir,
        );

        for (let i = 1; i <= 5; i++) {
            await client.pushData({ index: i });
        }

        // Get first 2 items
        const page1 = await client.getData(0, 2, false, false);
        expect(page1.count).toBe(2);
        expect(page1.total).toBe(5);
        expect(page1.items[0].index).toBe(1);
        expect(page1.items[1].index).toBe(2);

        // Get items 3-4
        const page2 = await client.getData(2, 2, false, false);
        expect(page2.count).toBe(2);
        expect(page2.items[0].index).toBe(3);

        // Descending order
        const descPage = await client.getData(0, 5, true, false);
        expect(descPage.items[0].index).toBe(5);
        expect(descPage.items[4].index).toBe(1);
    });

    it('should purge data but keep metadata', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            null,
            null,
            storageDir,
        );

        await client.pushData({ x: 1 });
        expect((await client.getMetadata()).item_count).toBe(1);

        await client.purge();
        expect((await client.getMetadata()).item_count).toBe(0);

        // Metadata file should still exist
        expect(existsSync(client.pathToMetadata)).toBe(true);
    });

    it('should drop storage entirely', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            null,
            null,
            storageDir,
        );

        await client.pushData({ x: 1 });
        await client.dropStorage();

        expect(existsSync(client.pathToDataset)).toBe(false);
    });

    it('should reopen existing dataset', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            'my-ds',
            null,
            storageDir,
        );
        await client.pushData({ x: 1 });

        const meta = await client.getMetadata();
        const id = meta.id;

        // Reopen by name
        const client2 = await FileSystemDatasetClient.open(
            null,
            'my-ds',
            null,
            storageDir,
        );
        expect((await client2.getMetadata()).item_count).toBe(1);

        // Reopen by id
        const client3 = await FileSystemDatasetClient.open(
            id,
            null,
            null,
            storageDir,
        );
        expect((await client3.getMetadata()).item_count).toBe(1);
    });

    it('should handle alias vs name correctly', async () => {
        // Open via name — metadata.name should be set
        const named = await FileSystemDatasetClient.open(
            null,
            'my-name',
            null,
            storageDir,
        );
        expect((await named.getMetadata()).name).toBe('my-name');

        // Open via alias — metadata.name should be null
        const aliased = await FileSystemDatasetClient.open(
            null,
            null,
            'my-alias',
            storageDir,
        );
        expect((await aliased.getMetadata()).name).toBeNull();

        // But the directory should exist
        expect(
            existsSync(join(storageDir, 'datasets', 'my-alias')),
        ).toBe(true);
    });

    it('should reject multiple exclusive args', async () => {
        await expect(
            FileSystemDatasetClient.open(null, 'name', 'alias', storageDir),
        ).rejects.toThrow();

        await expect(
            FileSystemDatasetClient.open('id', null, 'alias', storageDir),
        ).rejects.toThrow();
    });

    it('should iterate items with iterator', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            null,
            null,
            storageDir,
        );

        for (let i = 1; i <= 5; i++) {
            await client.pushData({ index: i });
        }

        const iterator = await client.iterateItems(0, null, false, false, 2);
        const items: Record<string, unknown>[] = [];
        let item;
        while ((item = await iterator.next()) !== null) {
            items.push(item);
        }

        expect(items.length).toBe(5);
        expect(items[0].index).toBe(1);
        expect(items[4].index).toBe(5);
    });

    it('should iterate items with limit', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            null,
            null,
            storageDir,
        );

        for (let i = 1; i <= 5; i++) {
            await client.pushData({ index: i });
        }

        const iterator = await client.iterateItems(0, 3, false, false, 2);
        const items: Record<string, unknown>[] = [];
        let item;
        while ((item = await iterator.next()) !== null) {
            items.push(item);
        }

        expect(items.length).toBe(3);
    });

    it('should use default storage dir', async () => {
        const client = await FileSystemDatasetClient.open(
            null,
            'test-default-dir',
            null,
            storageDir,
        );
        const meta = await client.getMetadata();
        expect(meta.id).toBeTruthy();
        await client.dropStorage();
    });
});
