import { createReadStream, createWriteStream } from 'fs';
import { unlink } from 'fs/promises';
import { Readable, Writable } from 'stream';

import {
    DatasetItemIterator,
    FileSystemDatasetClient,
    FileSystemKeyValueStoreClient,
    FileSystemRequestQueueClient,
    KvsKeyIterator,
} from './index.js';

// The core library stores datetimes as ISO-8601 strings (the on-disk format),
// so they cross the FFI boundary as strings. Convert the standard metadata
// datetime fields to native JS `Date`s before handing the object to callers.
const METADATA_DATE_FIELDS = ['accessedAt', 'createdAt', 'modifiedAt'];

function convertMetadataDates(meta) {
    if (meta) {
        for (const field of METADATA_DATE_FIELDS) {
            if (typeof meta[field] === 'string') {
                meta[field] = new Date(meta[field]);
            }
        }
    }
    return meta;
}

// Wrap getMetadata on each client to coerce datetime strings into `Date`s.
for (const Client of [
    FileSystemDatasetClient,
    FileSystemKeyValueStoreClient,
    FileSystemRequestQueueClient,
]) {
    const origGetMetadata = Client.prototype.getMetadata;
    Client.prototype.getMetadata = async function (...args) {
        return convertMetadataDates(await origGetMetadata.apply(this, args));
    };
}

// Add Symbol.asyncIterator to DatasetItemIterator so users can write:
//   for await (const item of client.iterateItems()) { ... }
DatasetItemIterator.prototype[Symbol.asyncIterator] = function () {
    return {
        next: async () => {
            const value = await this.next();
            if (value === null) {
                return { done: true, value: undefined };
            }
            return { done: false, value };
        },
    };
};

// Same for KvsKeyIterator.
KvsKeyIterator.prototype[Symbol.asyncIterator] = function () {
    return {
        next: async () => {
            const value = await this.next();
            if (value === null) {
                return { done: true, value: undefined };
            }
            return { done: false, value };
        },
    };
};

// Wrap getValue to convert the byte-array value to a real Buffer.
const origGetValue = FileSystemKeyValueStoreClient.prototype.getValue;
FileSystemKeyValueStoreClient.prototype.getValue = async function (...args) {
    const record = await origGetValue.apply(this, args);
    if (record) {
        record.value = Buffer.from(record.value);
    }
    return record;
};

// getValueStream: returns { key, contentType, size, stream } or null.
// The stream is a Web ReadableStream<Uint8Array> created from the file on disk.
FileSystemKeyValueStoreClient.prototype.getValueStream = async function (key) {
    const info = await this._getValueFileInfo(key);
    if (info === null) {
        return null;
    }

    const nodeStream = createReadStream(info.filePath);
    const stream = Readable.toWeb(nodeStream);

    return {
        key: info.key,
        contentType: info.contentType,
        size: info.size,
        stream,
    };
};

// setValueStream: pipes a ReadableStream directly to a temp file on disk,
// then atomically renames it into place. No buffering in memory.
FileSystemKeyValueStoreClient.prototype.setValueStream = async function (key, stream, contentType) {
    const tempPath = this._getTempFilePath();
    const ws = createWriteStream(tempPath);
    const writable = Writable.toWeb(ws);

    let size = 0;
    const transform = new TransformStream({
        transform(chunk, controller) {
            size += chunk.byteLength;
            controller.enqueue(chunk);
        },
    });

    try {
        await stream.pipeThrough(transform).pipeTo(writable);
    } catch (err) {
        await unlink(tempPath).catch(() => {});
        throw err;
    }

    const ct = contentType ?? 'application/octet-stream';
    return this._finalizeStreamedValue(key, tempPath, size, ct);
};

export * from './index.js';
