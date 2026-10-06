package blobkit

import (
	"github.com/suhwr/blobkit/key"
)

// Option represents a functional configuration option for the BlobKit Client.
type Option func(*Client)

// WithDriver sets a single fixed storage driver for the client.
func WithDriver(driver Driver) Option {
	return func(c *Client) {
		c.router = &defaultFixedRouter{driver: driver}
	}
}

// WithRouter sets a custom routing policy (failover, namespace, etc.).
func WithRouter(r Router) Option {
	return func(c *Client) {
		c.router = r
	}
}

// WithKeyGenerator configures the physical key naming strategy.
func WithKeyGenerator(gen key.Generator) Option {
	return func(c *Client) {
		c.keyGen = gen
	}
}

// WithURLResolver configures the public delivery URL resolver.
func WithURLResolver(resolver URLResolver) Option {
	return func(c *Client) {
		c.urlResolver = resolver
	}
}

// WithRegistry attaches an optional database metadata registry.
func WithRegistry(reg MetadataStore) Option {
	return func(c *Client) {
		c.registry = reg
	}
}

// WithDefaultVisibility sets the default visibility for uploaded objects.
func WithDefaultVisibility(vis Visibility) Option {
	return func(c *Client) {
		c.defaultVisibility = vis
	}
}

// WithPolicy sets the default upload security and validation policy for the client.
func WithPolicy(p Policy) Option {
	return func(c *Client) {
		c.policy = &p
	}
}

// WithObserver sets a vendor-neutral observability instrumentation hook.
func WithObserver(obs Observer) Option {
	return func(c *Client) {
		c.observer = obs
	}
}

// WithCache sets an optional caching layer for object metadata and negative lookups.
func WithCache(cache Cache) Option {
	return func(c *Client) {
		c.cache = cache
	}
}

