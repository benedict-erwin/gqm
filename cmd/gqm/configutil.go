package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

const restartNotice = "\n⚠  Restart your GQM server for changes to take effect.\n"

// loadConfigNode reads a YAML config file into a yaml.Node tree,
// preserving comments and formatting.
func loadConfigNode(path string) (*yaml.Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parsing config yaml: %w", err)
	}

	// yaml.Unmarshal produces a Document node wrapping the actual mapping.
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, fmt.Errorf("unexpected yaml structure in %s", path)
	}

	return &doc, nil
}

// saveConfigNode writes a yaml.Node tree back to the config file,
// preserving comments and formatting.
func saveConfigNode(path string, doc *yaml.Node) error {
	data, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshaling config yaml: %w", err)
	}

	// Preserve original file permissions.
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat config file: %w", err)
	}

	if err := os.WriteFile(path, data, info.Mode().Perm()); err != nil {
		return fmt.Errorf("writing config file: %w", err)
	}

	return nil
}

// mapGet returns the value node for the given key in a mapping node.
// Returns nil if not found.
func mapGet(mapping *yaml.Node, key string) *yaml.Node {
	if mapping.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(mapping.Content)-1; i += 2 {
		if mapping.Content[i].Value == key {
			return mapping.Content[i+1]
		}
	}
	return nil
}

// mapSet sets a scalar value for the given key in a mapping node.
// If the key does not exist, it is appended.
func mapSet(mapping *yaml.Node, key, value string) {
	for i := 0; i < len(mapping.Content)-1; i += 2 {
		if mapping.Content[i].Value == key {
			mapping.Content[i+1].Value = value
			return
		}
	}
	// Key not found — append new key-value pair.
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Value: value},
	)
}

// mapGetOrCreate returns the value node for the given key in a mapping node,
// creating it as the specified kind if it does not exist.
func mapGetOrCreate(mapping *yaml.Node, key string, kind yaml.Kind) *yaml.Node {
	if v := mapGet(mapping, key); v != nil {
		return v
	}
	valNode := &yaml.Node{Kind: kind}
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		valNode,
	)
	return valNode
}

// newMappingFromPairs creates a yaml.Node mapping from key-value string pairs.
func newMappingFromPairs(pairs ...string) *yaml.Node {
	m := &yaml.Node{Kind: yaml.MappingNode}
	for i := 0; i < len(pairs)-1; i += 2 {
		m.Content = append(m.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: pairs[i]},
			&yaml.Node{Kind: yaml.ScalarNode, Value: pairs[i+1]},
		)
	}
	return m
}

// seqFindMapping searches a sequence node for a mapping where key == value.
// Returns the mapping node and its index, or (nil, -1) if not found.
func seqFindMapping(seq *yaml.Node, key, value string) (*yaml.Node, int) {
	if seq.Kind != yaml.SequenceNode {
		return nil, -1
	}
	for i, item := range seq.Content {
		if item.Kind == yaml.MappingNode {
			if v := mapGet(item, key); v != nil && v.Value == value {
				return item, i
			}
		}
	}
	return nil, -1
}
