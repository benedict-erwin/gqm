package tui

import (
	"strings"
	"testing"
)

func TestJobStale(t *testing.T) {
	tests := []struct {
		name string
		job  Job
		want bool
	}{
		{name: "absent", job: Job{"status": "processing"}, want: false},
		{name: "bool true", job: Job{"status": "processing", "stale": true}, want: true},
		{name: "bool false", job: Job{"status": "processing", "stale": false}, want: false},
		{name: "string true", job: Job{"status": "processing", "stale": "true"}, want: true},
		{name: "string false", job: Job{"status": "processing", "stale": "false"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.job.stale(); got != tt.want {
				t.Errorf("stale() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJobStatusCell_Stale(t *testing.T) {
	tests := []struct {
		name       string
		job        Job
		wantText   string
		unwantText string
	}{
		{
			name:       "stale processing job",
			job:        Job{"status": "processing", "stale": true},
			wantText:   "stale",
			unwantText: "processing",
		},
		{
			name:     "live processing job",
			job:      Job{"status": "processing"},
			wantText: "processing",
		},
		{
			name:     "terminal job",
			job:      Job{"status": "completed"},
			wantText: "completed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := jobStatusCell(tt.job)
			if !strings.Contains(got, tt.wantText) {
				t.Errorf("jobStatusCell() = %q, want it to contain %q", got, tt.wantText)
			}
			if tt.unwantText != "" && strings.Contains(got, tt.unwantText) {
				t.Errorf("jobStatusCell() = %q, want it not to contain %q", got, tt.unwantText)
			}
		})
	}
}

func TestJobsView_RendersStaleMarker(t *testing.T) {
	v := &jobsView{
		queue: "default",
		jobs: []Job{
			{"id": "zombie", "type": "email.send", "status": "processing", "stale": true},
		},
	}

	out := v.render(120, 20)
	if !strings.Contains(out, "stale") {
		t.Errorf("jobs view did not mark the stale job:\n%s", out)
	}
}
