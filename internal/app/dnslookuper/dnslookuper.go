package dnslookuper

import (
	"fmt"
	"github.com/miekg/dns"
	"time"
)

type ResolverResponse struct {
	IPAddress string
	ExpiresAt string
}

func ResolveDNSWithTTL(dnsName string) (result []ResolverResponse, err error) {
	config, err := dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil {
		return nil, fmt.Errorf("resolveDNSWithTTL: error reading /etc/resolv.conf: %w", err)
	}
	c := new(dns.Client)
	m := new(dns.Msg)

	m.SetQuestion(dns.Fqdn(dnsName), dns.TypeA)
	m.RecursionDesired = true

	r, _, err := c.Exchange(m, config.Servers[0]+":"+config.Port)
	if err != nil {
		return nil, fmt.Errorf("resolveDNSWithTTL: synchronous query for DNS %s failed: %w", dnsName, err)
	}
	if len(r.Answer) == 0 {
		return nil, fmt.Errorf("resolveDNSWithTTL: no answers received for DNS name %s", dnsName)
	}

	result = make([]ResolverResponse, len(r.Answer))

	for pos, ans := range r.Answer {
		if a, ok := ans.(*dns.A); ok {
			// Мы возвращаем IP A-записи и её TTL
			ip := a.A.String()
			ttl := time.Duration(a.Header().Ttl) * time.Second
			expiresAt := time.Now().Add(ttl).Format("2006-01-02 15:04:05")
			result[pos] = ResolverResponse{IPAddress: ip, ExpiresAt: expiresAt}
		}
	}

	return result, nil
}
