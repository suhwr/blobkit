package blobkit_test

import (
	"testing"

	"github.com/suhwr/blobkit"
)

func TestEscapeURLPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "file.txt", "file.txt"},
		{"leading slash", "/file.txt", "file.txt"},
		{"nested path", "images/avatars/user.png", "images/avatars/user.png"},
		{"query in key", "doc?version=1.pdf", "doc%3Fversion=1.pdf"},
		{"fragment in key", "readme#section.md", "readme%23section.md"},
		{"space in key", "my report 2026.pdf", "my%20report%202026.pdf"},
		{"nested with special chars", "reports/2026/q1?draft#final.docx", "reports/2026/q1%3Fdraft%23final.docx"},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := blobkit.EscapeURLPath(tt.input)
			if got != tt.expected {
				t.Fatalf("EscapeURLPath(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestCDNResolver_Escaping(t *testing.T) {
	resolver := blobkit.NewCDNResolver("https://cdn.example.com")
	got := resolver.ResolveURL("user/avatar?size=large.png")
	expected := "https://cdn.example.com/user/avatar%3Fsize=large.png"
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}

func TestS3PathResolver_Escaping(t *testing.T) {
	resolver := blobkit.NewS3PathResolver("https://s3.us-east-1.amazonaws.com", "my-bucket")
	got := resolver.ResolveURL("docs/whitepaper#v2.pdf")
	expected := "https://s3.us-east-1.amazonaws.com/my-bucket/docs/whitepaper%23v2.pdf"
	if got != expected {
		t.Fatalf("got %q, want %q", got, expected)
	}
}
