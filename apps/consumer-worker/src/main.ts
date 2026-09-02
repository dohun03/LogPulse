import { PaymentEventPayload } from '@logpulse/shared';

export function usesSharedType(event: PaymentEventPayload) {
  return event.status;
}
