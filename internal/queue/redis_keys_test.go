package queue

import "testing"

func TestRedisQueueKeysUseClusterSafeHashTag(t *testing.T) {
	keys := newRedisQueueKeys("jobs")

	if keys.ready != "moxy:{jobs}:ready" {
		t.Fatalf("ready key = %q, want %q", keys.ready, "moxy:{jobs}:ready")
	}
	if keys.processing != "moxy:{jobs}:processing" {
		t.Fatalf("processing key = %q, want %q", keys.processing, "moxy:{jobs}:processing")
	}
	if keys.dead != "moxy:{jobs}:dead" {
		t.Fatalf("dead key = %q, want %q", keys.dead, "moxy:{jobs}:dead")
	}

	wantTag := "{jobs}"
	for name, key := range map[string]string{
		"ready":      keys.ready,
		"processing": keys.processing,
		"dead":       keys.dead,
	} {
		if got := redisHashTag(key); got != wantTag {
			t.Fatalf("%s key hash tag = %q, want %q", name, got, wantTag)
		}
	}
}
