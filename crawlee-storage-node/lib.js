const { createReadStream, createWriteStream } = require('fs');
const { unlink } = require('fs/promises');
const { Readable, Writable } = require('stream');

const native = require('./index.js');

// Add Symbol.asyncIterator to DatasetItemIterator so users can write:
//   for await (const item of client.iterateItems()) { ... }
native.DatasetItemIterator.prototype[Symbol.asyncIterator] = function () {
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
native.KvsKeyIterator.prototype[Symbol.asyncIterator] = function () {
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
const origGetValue = native.FileSystemKeyValueStoreClient.prototype.getValue;
native.FileSystemKeyValueStoreClient.prototype.getValue = async function (...args) {
    const record = await origGetValue.apply(this, args);
    if (record) {
        record.value = Buffer.from(record.value);
    }
    return record;
};

// getValueStream: returns { key, contentType, size, stream } or null.
// The stream is a Web ReadableStream<Uint8Array> created from the file on disk.
native.FileSystemKeyValueStoreClient.prototype.getValueStream = async function (key) {
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
native.FileSystemKeyValueStoreClient.prototype.setValueStream = async function (
    key,
    stream,
    contentType,
) {
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

module.exports = native;
