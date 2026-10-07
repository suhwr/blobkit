package webdav

import (
	"encoding/xml"
	"strings"
	"time"
)

type multistatusXML struct {
	XMLName   xml.Name      `xml:"multistatus"`
	Responses []responseXML `xml:"response"`
}

type responseXML struct {
	Href     string        `xml:"href"`
	Propstat []propstatXML `xml:"propstat"`
}

type propstatXML struct {
	Prop   propXML `xml:"prop"`
	Status string  `xml:"status"`
}

type propXML struct {
	ContentLength int64           `xml:"getcontentlength"`
	ContentType   string          `xml:"getcontenttype"`
	ETag          string          `xml:"getetag"`
	LastModified  string          `xml:"getlastmodified"`
	ResourceType  resourceTypeXML `xml:"resourcetype"`
}

type resourceTypeXML struct {
	Collection *struct{} `xml:"collection"`
}

// isDir returns true if the resource represents a WebDAV collection (directory).
func (p *propXML) isDir() bool {
	return p.ResourceType.Collection != nil
}

// parseWebDAVTime parses various timestamp formats returned by WebDAV servers.
func parseWebDAVTime(val string) time.Time {
	val = strings.TrimSpace(val)
	if val == "" {
		return time.Time{}
	}

	formats := []string{
		time.RFC1123,
		time.RFC1123Z,
		time.RFC850,
		time.ANSIC,
		time.RFC3339,
		time.RFC3339Nano,
	}

	for _, fmtStr := range formats {
		if t, err := time.Parse(fmtStr, val); err == nil {
			return t.UTC()
		}
	}

	return time.Time{}
}
