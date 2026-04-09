export interface DatasetMetadata {
  id: string
  name: string | null
  accessed_at: string
  created_at: string
  modified_at: string
  item_count: number
}

export interface DatasetItemsListPage {
  count: number
  offset: number
  limit: number
  total: number
  desc: boolean
  items: Record<string, unknown>[]
}

export interface KeyValueStoreMetadata {
  id: string
  name: string | null
  accessed_at: string
  created_at: string
  modified_at: string
}

export interface KeyValueStoreRecord {
  key: string
  content_type: string
  size: number | null
  value: unknown
  /** Present and true when value is binary (an array of byte values) */
  __binary__?: boolean
}

export interface KeyValueStoreRecordMetadata {
  key: string
  content_type: string
  size: number | null
}

export interface RequestQueueMetadata {
  id: string
  name: string | null
  accessed_at: string
  created_at: string
  modified_at: string
  had_multiple_clients: boolean
  handled_request_count: number
  pending_request_count: number
  total_request_count: number
}

export interface ProcessedRequest {
  id: string | null
  unique_key: string
  was_already_present: boolean
  was_already_handled: boolean
}

export interface AddRequestsResponse {
  processed_requests: ProcessedRequest[]
  unprocessed_requests: unknown[]
}
