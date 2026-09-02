export interface EventEnvelope<T> {
  eventId: string;
  ingestedAt: string;
  payload: T;
}

export interface AcceptedEventResponse {
  success: true;
  eventId: string;
  topic: string;
  acceptedAt: string;
}

export interface ErrorResponse {
  success: false;
  errorCode: ErrorCode;
  message: string;
  details?: unknown;
  timestamp: string;
  path: string;
}

export type ErrorCode =
  | 'VALIDATION_ERROR'
  | 'UNAUTHORIZED'
  | 'RATE_LIMITED'
  | 'BROKER_UNAVAILABLE'
  | 'INTERNAL_ERROR';
