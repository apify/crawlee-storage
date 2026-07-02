import { createReadStream, createWriteStream } from 'fs';
import { unlink } from 'fs/promises';
import { Readable, Writable } from 'stream';

import { FileSystemKeyValueStoreClient } from './index.js';

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
