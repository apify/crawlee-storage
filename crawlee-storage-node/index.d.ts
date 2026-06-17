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
export declare class DatasetItemIterator {
  /** Fetch the next item. Returns null when iteration is exhausted. */
  next(): Promise<Record<string, unknown> | null>
}

export declare class FileSystemDatasetClient {
  static open(id?: string | undefined | null, name?: string | undefined | null, alias?: string | undefined | null, storageDir?: string | undefined | null): Promise<FileSystemDatasetClient>
  get pathToDataset(): string
  get pathToMetadata(): string
  getMetadata(): Promise<DatasetMetadata>
  dropStorage(): Promise<void>
  purge(): Promise<void>
  pushData(data: Record<string, unknown> | Record<string, unknown>[]): Promise<void>
  getData(offset?: number | undefined | null, limit?: number | undefined | null, desc?: boolean | undefined | null, skipEmpty?: boolean | undefined | null): Promise<DatasetItemsListPage>
  iterateItems(offset?: number | undefined | null, limit?: number | undefined | null, desc?: boolean | undefined | null, skipEmpty?: boolean | undefined | null, pageSize?: number | undefined | null): Promise<DatasetItemIterator>
}

export declare class FileSystemKeyValueStoreClient {
  static open(id?: string | undefined | null, name?: string | undefined | null, alias?: string | undefined | null, storageDir?: string | undefined | null): Promise<FileSystemKeyValueStoreClient>
  get pathToKvs(): string
  get pathToMetadata(): string
  getMetadata(): Promise<KeyValueStoreMetadata>
  dropStorage(): Promise<void>
  purge(): Promise<void>
  /** Get a record by key. Returns the raw value bytes as a Buffer. */
  getValue(key: string): Promise<KeyValueStoreRecord | null>
  /** Set a value from a Buffer. */
  setValue(key: string, value: Buffer, contentType?: string | undefined | null): Promise<void>
  deleteValue(key: string): Promise<void>
  iterateKeys(exclusiveStartKey?: string | undefined | null, limit?: number | undefined | null, pageSize?: number | undefined | null): Promise<KvsKeyIterator>
  getPublicUrl(key: string): Promise<string>
  recordExists(key: string): Promise<boolean>
}

export declare class FileSystemRequestQueueClient {
  static open(id?: string | undefined | null, name?: string | undefined | null, alias?: string | undefined | null, storageDir?: string | undefined | null): Promise<FileSystemRequestQueueClient>
  get pathToRq(): string
  get pathToMetadata(): string
  getMetadata(): Promise<RequestQueueMetadata>
  dropStorage(): Promise<void>
  purge(): Promise<void>
  addBatchOfRequests(requests: Record<string, unknown>[], forefront?: boolean | undefined | null): Promise<AddRequestsResponse>
  getRequest(uniqueKey: string): Promise<Record<string, unknown> | null>
  fetchNextRequest(): Promise<Record<string, unknown> | null>
  markRequestAsHandled(request: Record<string, unknown>): Promise<ProcessedRequest | null>
  reclaimRequest(request: Record<string, unknown>, forefront?: boolean | undefined | null): Promise<ProcessedRequest | null>
  isEmpty(): Promise<boolean>
  isFinished(): Promise<boolean>
  setExpectedRequestProcessingTime(secs: number): Promise<void>
  persistState(): Promise<void>
}

export declare class KvsKeyIterator {
  /** Fetch the next key metadata entry. Returns null when iteration is exhausted. */
  next(): Promise<KeyValueStoreRecordMetadata | null>
}
