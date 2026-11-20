import { describe, expect, test } from 'bun:test';
import { EVENTS_GROUP_PROTOCOLS } from './kafka';

describe('events consumer group', () => {
  // Every member of a group must advertise a protocol in common. Releases up
  // to the switch to @platformatic/kafka advertise this one, and a rolling
  // upgrade or a rollback puts both in the group at once.
  test('advertises the protocol name kafkajs members use', () => {
    expect(EVENTS_GROUP_PROTOCOLS).toEqual([
      { name: 'RoundRobinAssigner', version: 0 },
    ]);
  });
});
