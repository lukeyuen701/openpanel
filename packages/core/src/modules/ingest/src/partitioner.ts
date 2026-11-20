// Every release before this one produced with kafkajs's default partitioner.
// A key has to land where that one put it: while old and new producers run side
// by side, a different hash splits one device's events across two partitions,
// and so across two consumers that race on its session state.
// `partitioner.test.ts` holds this to the mapping kafkajs recorded.
export { compatibilityPartitioner as eventsPartitioner } from '@platformatic/kafka';
