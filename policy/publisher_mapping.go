package policy

// ResolvePublisher maps a conn_group to a publisher name using the provided map.
// If the conn_group is not found in the map, the conn_group itself is returned
// as a fallback so that policies can also match directly on group name.
func ResolvePublisher(connGroup string, publisherMap map[string]string) string {
	if pub, ok := publisherMap[connGroup]; ok {
		return pub
	}
	return connGroup
}

// ParsePublisherMapping converts a flat string map (loaded from configuration)
// into the canonical publisher mapping type used throughout the policy package.
// It is a thin wrapper provided for clarity at call sites.
func ParsePublisherMapping(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	return m
}
