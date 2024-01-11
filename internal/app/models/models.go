package models

type APIUploadFQDNRequest struct {
	FQDN string `json:"fqdn"`
}

type APIGetFQDNsRequest struct {
	IPAddresses []string
}

type APIGetFQDNsResponse struct {
	IPAddresses map[string][]string
}

type ResolverResponse struct {
	DNSName   string
	IPAddress string
	ExpiresAt string
}
