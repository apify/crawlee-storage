export * from './index';

import { DatasetItemIterator, FileSystemKeyValueStoreClient, KvsKeyIterator } from './index';
import type { KeyValueStoreRecordMetadata, KeyValueStoreStreamRecord } from './index';

declare module './index' {
    interface DatasetItemIterator {
        [Symbol.asyncIterator](): AsyncIterator<Record<string, unknown>>;
    }

    interface KvsKeyIterator {
        [Symbol.asyncIterator](): AsyncIterator<KeyValueStoreRecordMetadata>;
    }

    interface FileSystemKeyValueStoreClient {
        /** Get a value as a ReadableStream of bytes. Returns null if the key doesn't exist. */
        getValueStream(key: string): Promise<KeyValueStoreStreamRecord | null>;
        /** Set a value from a ReadableStream. Consumes the entire stream and writes atomically. */
        setValueStream(
            key: string,
            stream: ReadableStream<Uint8Array>,
            contentType?: string | null,
        ): Promise<void>;
    }
}
