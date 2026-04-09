export interface DatasetMetadata {
    id: string;
    name: string | null;
    accessed_at: string;
    created_at: string;
    modified_at: string;
    item_count: number;
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
    accessed_at: string;
    created_at: string;
    modified_at: string;
}

export interface KeyValueStoreRecord {
    key: string;
    content_type: string;
    size: number | null;
    value: unknown;
    /** Present and true when value is binary (an array of byte values) */
    __binary__?: boolean;
}

export interface KeyValueStoreRecordMetadata {
    key: string;
    content_type: string;
    size: number | null;
}

export interface RequestQueueMetadata {
    id: string;
    name: string | null;
    accessed_at: string;
    created_at: string;
    modified_at: string;
    had_multiple_clients: boolean;
    handled_request_count: number;
    pending_request_count: number;
    total_request_count: number;
}

export interface ProcessedRequest {
    id: string | null;
    unique_key: string;
    was_already_present: boolean;
    was_already_handled: boolean;
}

export interface AddRequestsResponse {
    processed_requests: ProcessedRequest[];
    unprocessed_requests: unknown[];
}
export declare class DatasetItemIterator {
    /** Fetch the next item. Returns null when iteration is exhausted. */
    next(): Promise<Record<string, unknown> | null>;
}

export declare class FileSystemDatasetClient {
    static open(
        id?: string | undefined | null,
        name?: string | undefined | null,
        alias?: string | undefined | null,
        storageDir?: string | undefined | null,
    ): Promise<FileSystemDatasetClient>;
    get pathToDataset(): string;
    get pathToMetadata(): string;
    getMetadata(): Promise<DatasetMetadata>;
    dropStorage(): Promise<void>;
    purge(): Promise<void>;
    pushData(data: Record<string, unknown> | Record<string, unknown>[]): Promise<void>;
    getData(
        offset?: number | undefined | null,
        limit?: number | undefined | null,
        desc?: boolean | undefined | null,
        skipEmpty?: boolean | undefined | null,
    ): Promise<DatasetItemsListPage>;
    iterateItems(
        offset?: number | undefined | null,
        limit?: number | undefined | null,
        desc?: boolean | undefined | null,
        skipEmpty?: boolean | undefined | null,
        pageSize?: number | undefined | null,
    ): Promise<DatasetItemIterator>;
}

export declare class FileSystemKeyValueStoreClient {
    static open(
        id?: string | undefined | null,
        name?: string | undefined | null,
        alias?: string | undefined | null,
        storageDir?: string | undefined | null,
    ): Promise<FileSystemKeyValueStoreClient>;
    get pathToKvs(): string;
    get pathToMetadata(): string;
    getMetadata(): Promise<KeyValueStoreMetadata>;
    dropStorage(): Promise<void>;
    purge(): Promise<void>;
    getValue(key: string): Promise<KeyValueStoreRecord | null>;
    setValue(key: string, value: unknown, contentType?: string | undefined | null): Promise<void>;
    /** Set a binary value (Buffer) for a key. */
    setValueBuffer(
        key: string,
        value: Buffer,
        contentType?: string | undefined | null,
    ): Promise<void>;
    deleteValue(key: string): Promise<void>;
    iterateKeys(
        exclusiveStartKey?: string | undefined | null,
        limit?: number | undefined | null,
        pageSize?: number | undefined | null,
    ): Promise<KvsKeyIterator>;
    getPublicUrl(key: string): Promise<string>;
    recordExists(key: string): Promise<boolean>;
}

export declare class FileSystemRequestQueueClient {
    static open(
        id?: string | undefined | null,
        name?: string | undefined | null,
        alias?: string | undefined | null,
        storageDir?: string | undefined | null,
    ): Promise<FileSystemRequestQueueClient>;
    get pathToRq(): string;
    get pathToMetadata(): string;
    getMetadata(): Promise<RequestQueueMetadata>;
    dropStorage(): Promise<void>;
    purge(): Promise<void>;
    addBatchOfRequests(
        requests: Record<string, unknown>[],
        forefront?: boolean | undefined | null,
    ): Promise<AddRequestsResponse>;
    getRequest(uniqueKey: string): Promise<Record<string, unknown> | null>;
    fetchNextRequest(): Promise<Record<string, unknown> | null>;
    markRequestAsHandled(request: Record<string, unknown>): Promise<ProcessedRequest | null>;
    reclaimRequest(
        request: Record<string, unknown>,
        forefront?: boolean | undefined | null,
    ): Promise<ProcessedRequest | null>;
    isEmpty(): Promise<boolean>;
    persistState(): Promise<void>;
}

export declare class KvsKeyIterator {
    /** Fetch the next key metadata entry. Returns null when iteration is exhausted. */
    next(): Promise<KeyValueStoreRecordMetadata | null>;
}
