export interface DatasetMetadata {
    id: string;
    name: string | null;
    accessedAt: Date;
    createdAt: Date;
    modifiedAt: Date;
    itemCount: number;
}

export interface DatasetItemsListPage {
    count: number;
    offset: number;
    limit: number;
    total: number;
    desc: boolean;
    items: Record<string, unknown>[];
}

export interface KeyValueStoreMetadata {
    id: string;
    name: string | null;
    accessedAt: Date;
    createdAt: Date;
    modifiedAt: Date;
}

export interface KeyValueStoreRecord {
    key: string;
    contentType: string;
    size: number | null;
    value: Buffer;
}

export interface KeyValueStoreStreamRecord {
    key: string;
    contentType: string;
    size: number | null;
    stream: ReadableStream<Uint8Array>;
}

export interface KeyValueStoreRecordMetadata {
    key: string;
    contentType: string;
    size: number | null;
}

export interface RequestQueueMetadata {
    id: string;
    name: string | null;
    accessedAt: Date;
    createdAt: Date;
    modifiedAt: Date;
    hadMultipleClients: boolean;
    handledRequestCount: number;
    pendingRequestCount: number;
    totalRequestCount: number;
}

export interface ProcessedRequest {
    requestId: string;
    uniqueKey: string;
    wasAlreadyPresent: boolean;
    wasAlreadyHandled: boolean;
}

export interface UnprocessedRequest {
    uniqueKey: string;
    url: string;
    method?: string | null;
}

export interface AddRequestsResponse {
    processedRequests: ProcessedRequest[];
    unprocessedRequests: UnprocessedRequest[];
}

// The following `interface` declarations merge with the napi-generated `declare class`es
// further down in this file, adding the JS-side wrappers defined in `lib.js` directly onto
// the class signatures.

export interface DatasetItemIterator {
    [Symbol.asyncIterator](): AsyncIterator<Record<string, unknown>>;
}

export interface KvsKeyIterator {
    [Symbol.asyncIterator](): AsyncIterator<KeyValueStoreRecordMetadata>;
}

export interface FileSystemKeyValueStoreClient {
    /** Get a value as a ReadableStream of bytes. Returns null if the key doesn't exist. */
    getValueStream(key: string): Promise<KeyValueStoreStreamRecord | null>;
    /** Set a value from a ReadableStream. Consumes the entire stream and writes atomically. */
    setValueStream(
        key: string,
        stream: ReadableStream<Uint8Array>,
        contentType?: string | null,
    ): Promise<void>;
}
