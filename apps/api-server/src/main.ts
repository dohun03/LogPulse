import { ClickEventPayload } from '@logpulse/shared';

export function usesSharedType(event: ClickEventPayload) {
  return event.eventType;
}
