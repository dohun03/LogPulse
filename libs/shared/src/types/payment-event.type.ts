export interface PaymentEventPayload {
  eventId: string;
  orderId: string;
  userId: string;
  amount: number;
  currency: string;
  paymentMethod: string;
  status: 'completed' | 'failed' | 'canceled';
  occurredAt: string;
}
