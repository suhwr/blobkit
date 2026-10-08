package blobkit

// SessionLockCount returns the number of active entries in sessionLocks (for testing).
func (c *Client) SessionLockCount() int {
	count := 0
	c.sessionLocks.Range(func(key, value any) bool {
		count++
		return true
	})
	return count
}

// HasSessionLock checks if a specific session ID has a lock in sessionLocks (for testing).
func (c *Client) HasSessionLock(sessionID string) bool {
	_, ok := c.sessionLocks.Load(sessionID)
	return ok
}
