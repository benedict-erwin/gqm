package main

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMapGet(t *testing.T) {
	m := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		{Kind: yaml.ScalarNode, Value: "name"},
		{Kind: yaml.ScalarNode, Value: "admin"},
		{Kind: yaml.ScalarNode, Value: "role"},
		{Kind: yaml.ScalarNode, Value: "viewer"},
	}}

	tests := []struct {
		name string
		key  string
		want string
	}{
		{"existing key", "name", "admin"},
		{"another key", "role", "viewer"},
		{"missing key", "missing", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapGet(m, tt.key)
			if tt.want == "" {
				if got != nil {
					t.Errorf("mapGet(%q) = %q, want nil", tt.key, got.Value)
				}
				return
			}
			if got == nil || got.Value != tt.want {
				t.Errorf("mapGet(%q) = %v, want %q", tt.key, got, tt.want)
			}
		})
	}

	t.Run("non-mapping node", func(t *testing.T) {
		seq := &yaml.Node{Kind: yaml.SequenceNode}
		if got := mapGet(seq, "key"); got != nil {
			t.Errorf("mapGet on sequence = %v, want nil", got)
		}
	})
}

func TestMapSet(t *testing.T) {
	t.Run("update existing key", func(t *testing.T) {
		m := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: "old"},
		}}
		mapSet(m, "name", "new")
		got := mapGet(m, "name")
		if got == nil || got.Value != "new" {
			t.Errorf("mapSet update: got %v, want 'new'", got)
		}
	})

	t.Run("append new key", func(t *testing.T) {
		m := &yaml.Node{Kind: yaml.MappingNode}
		mapSet(m, "key", "value")
		got := mapGet(m, "key")
		if got == nil || got.Value != "value" {
			t.Errorf("mapSet append: got %v, want 'value'", got)
		}
	})
}

func TestMapGetOrCreate(t *testing.T) {
	t.Run("existing key", func(t *testing.T) {
		m := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "section"},
			{Kind: yaml.MappingNode},
		}}
		got := mapGetOrCreate(m, "section", yaml.MappingNode)
		if got.Kind != yaml.MappingNode {
			t.Errorf("got kind %d, want MappingNode", got.Kind)
		}
		if len(m.Content) != 2 {
			t.Errorf("should not add new entries, got %d", len(m.Content))
		}
	})

	t.Run("create new key", func(t *testing.T) {
		m := &yaml.Node{Kind: yaml.MappingNode}
		got := mapGetOrCreate(m, "new_section", yaml.SequenceNode)
		if got.Kind != yaml.SequenceNode {
			t.Errorf("got kind %d, want SequenceNode", got.Kind)
		}
		if len(m.Content) != 2 {
			t.Errorf("should have 2 entries (key+value), got %d", len(m.Content))
		}
	})
}

func TestNewMappingFromPairs(t *testing.T) {
	m := newMappingFromPairs("name", "admin", "role", "viewer")
	if m.Kind != yaml.MappingNode {
		t.Fatalf("got kind %d, want MappingNode", m.Kind)
	}
	if len(m.Content) != 4 {
		t.Fatalf("got %d children, want 4", len(m.Content))
	}
	if got := mapGet(m, "name"); got == nil || got.Value != "admin" {
		t.Errorf("name = %v, want 'admin'", got)
	}
	if got := mapGet(m, "role"); got == nil || got.Value != "viewer" {
		t.Errorf("role = %v, want 'viewer'", got)
	}
}

func TestSeqFindMapping(t *testing.T) {
	entry1 := newMappingFromPairs("name", "grafana", "key", "gqm_ak_111")
	entry2 := newMappingFromPairs("name", "tui", "key", "gqm_ak_222")
	seq := &yaml.Node{Kind: yaml.SequenceNode, Content: []*yaml.Node{entry1, entry2}}

	t.Run("find existing", func(t *testing.T) {
		got, idx := seqFindMapping(seq, "name", "tui")
		if got == nil || idx != 1 {
			t.Errorf("seqFindMapping = (%v, %d), want (node, 1)", got, idx)
		}
	})

	t.Run("not found", func(t *testing.T) {
		got, idx := seqFindMapping(seq, "name", "missing")
		if got != nil || idx != -1 {
			t.Errorf("seqFindMapping = (%v, %d), want (nil, -1)", got, idx)
		}
	})

	t.Run("non-sequence", func(t *testing.T) {
		m := &yaml.Node{Kind: yaml.MappingNode}
		got, idx := seqFindMapping(m, "key", "value")
		if got != nil || idx != -1 {
			t.Errorf("seqFindMapping on mapping = (%v, %d), want (nil, -1)", got, idx)
		}
	})
}

func TestLoadSaveConfigNode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")

	content := "# comment\napp:\n  name: gqm\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	doc, err := loadConfigNode(path)
	if err != nil {
		t.Fatalf("loadConfigNode: %v", err)
	}
	if doc.Kind != yaml.DocumentNode {
		t.Fatalf("got kind %d, want DocumentNode", doc.Kind)
	}

	// Modify and save.
	root := doc.Content[0]
	app := mapGet(root, "app")
	if app == nil {
		t.Fatal("app section not found")
	}
	mapSet(app, "name", "gqm-updated")

	if err := saveConfigNode(path, doc); err != nil {
		t.Fatalf("saveConfigNode: %v", err)
	}

	// Re-read and verify.
	doc2, err := loadConfigNode(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	root2 := doc2.Content[0]
	app2 := mapGet(root2, "app")
	got := mapGet(app2, "name")
	if got == nil || got.Value != "gqm-updated" {
		t.Errorf("after save: name = %v, want 'gqm-updated'", got)
	}
}

func TestLoadConfigNode_Errors(t *testing.T) {
	t.Run("file not found", func(t *testing.T) {
		_, err := loadConfigNode("/nonexistent/path.yaml")
		if err == nil {
			t.Error("expected error for missing file")
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bad.yaml")
		os.WriteFile(path, []byte(":\n  :\n    - [invalid"), 0o644)
		_, err := loadConfigNode(path)
		if err == nil {
			t.Error("expected error for invalid yaml")
		}
	})
}
