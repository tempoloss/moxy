package main

import "testing"

func TestJournalFileNameRejectsNamesThatEscapeTheDirectory(t *testing.T) {
	// Queue names arrive over the network and become file names, so anything
	// that could resolve outside the journal directory has to be refused rather
	// than rewritten into something that might collide with another queue.
	for _, name := range []string{
		"",
		"..",
		"../escape",
		"nested/queue",
		`windows\queue`,
		"queue.with.dots",
		"queue name",
		"queue\x00null",
	} {
		if _, err := journalFileName(name); err == nil {
			t.Errorf("journalFileName(%q) was accepted, want it rejected", name)
		}
	}
}

func TestJournalFileNameAcceptsOrdinaryNames(t *testing.T) {
	for name, want := range map[string]string{
		"jobs":         "jobs.wal",
		"email-send":   "email-send.wal",
		"batch_2":      "batch_2.wal",
		"MixedCase123": "MixedCase123.wal",
	} {
		got, err := journalFileName(name)
		if err != nil {
			t.Errorf("journalFileName(%q) returned error: %v", name, err)
			continue
		}
		if got != want {
			t.Errorf("journalFileName(%q) = %q, want %q", name, got, want)
		}
	}
}
