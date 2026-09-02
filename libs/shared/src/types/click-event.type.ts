export interface ClickEventMetadata {
  referrer?: string;
  device?: string;
  ip?: string;
}

export interface ClickEventPayload {
  eventId: string;
  userId: string;
  sessionId: string;
  eventType: 'product_click' | 'page_view';
  productId?: string;
  pageUrl: string;
  occurredAt: string;
  metadata?: ClickEventMetadata;
}
