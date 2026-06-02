export interface DatasetMetadata {
    id: string;
    name: string | null;
    accessedAt: string;
    createdAt: string;
    modifiedAt: string;
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
    accessedAt: string;
    createdAt: string;
    modifiedAt: string;
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
    accessedAt: string;
    createdAt: string;
    modifiedAt: string;
    hadMultipleClients: boolean;
    handledRequestCount: number;
    pendingRequestCount: number;
    totalRequestCount: number;
}

export interface ProcessedRequest {
    id: string | null;
    uniqueKey: string;
    wasAlreadyPresent: boolean;
    wasAlreadyHandled: boolean;
}

export interface AddRequestsResponse {
    processedRequests: ProcessedRequest[];
    unprocessedRequests: unknown[];
}
