package queue

import "testing"

func TestMemoryQueueContract(t *testing.T) {
	RunBackendContractTests(t, func(t *testing.T) Backend {
		t.Helper()
		return NewMemoryQueue()
	})
}

func TestAcquireMovesReadyToProcessing(t *testing.T) {
	testAcquireMovesReadyToProcessing(t, newMemoryBackend)
}

func TestCompleteRemovesProcessingTask(t *testing.T) {
	testCompleteRemovesProcessingTask(t, newMemoryBackend)
}

func TestRequeueMovesProcessingTaskBackToReady(t *testing.T) {
	testRequeueMovesProcessingTaskBackToReady(t, newMemoryBackend)
}

func TestCompleteMissingProcessingTaskFails(t *testing.T) {
	testCompleteMissingProcessingTaskFails(t, newMemoryBackend)
}

func TestRequeueMissingProcessingTaskFails(t *testing.T) {
	testRequeueMissingProcessingTaskFails(t, newMemoryBackend)
}

func TestMemoryQueueStatsReportsReadyAndProcessing(t *testing.T) {
	testStatsReportsReadyAndProcessing(t, newMemoryBackend)
}

func TestMemoryQueuePayloadCloningStillWorks(t *testing.T) {
	testPayloadCloningPreventsExternalMutation(t, newMemoryBackend)
}

func newMemoryBackend(t *testing.T) Backend {
	t.Helper()
	return NewMemoryQueue()
}
