export * from './index';

import { DatasetItemIterator, KvsKeyIterator } from './index';
import type { KeyValueStoreRecordMetadata } from './index';

declare module './index' {
    interface DatasetItemIterator {
        [Symbol.asyncIterator](): AsyncIterator<Record<string, unknown>>;
    }

    interface KvsKeyIterator {
        [Symbol.asyncIterator](): AsyncIterator<KeyValueStoreRecordMetadata>;
    }
}
