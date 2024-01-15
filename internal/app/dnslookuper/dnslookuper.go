package dnslookuper

import (
	"fmt"
	"github.com/miekg/dns"
	"github.com/vancho-go/rDNS/internal/app/models"
	"time"
)

type ResolverResponse struct {
	IPAddress string
	ExpiresAt string
}

func ResolveDNSWithTTLBatch(dnsNames []string, errorChannel chan<- error) (results map[string][]models.ResolverResponse, err error) {
	config, err := dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil {
		return nil, fmt.Errorf("resolveDNSWithTTLBatch: error reading /etc/resolv.conf: %w", err)
	}
	c := new(dns.Client)

	results = make(map[string][]models.ResolverResponse)

	for _, dnsName := range dnsNames {
		m := new(dns.Msg)
		m.SetQuestion(dns.Fqdn(dnsName), dns.TypeA)
		m.RecursionDesired = true

		r, _, err := c.Exchange(m, config.Servers[0]+":"+config.Port)
		if err != nil {
			errorChannel <- fmt.Errorf("resolveDNSWithTTLBatch: error querying address: %v", err)
			continue
		}
		if len(r.Answer) == 0 {
			errorChannel <- fmt.Errorf("prepareAndUpdateDNSRecordBatch: error resolving fqdn  %s", dnsName)
			results[dnsName] = []models.ResolverResponse{} // Нет ответов для данного DNS-имени
			continue
		}

		var resolverResponses []models.ResolverResponse
		for _, ans := range r.Answer {
			if a, ok := ans.(*dns.A); ok {
				ip := a.A.String()
				ttl := time.Duration(a.Header().Ttl) * time.Second
				expiresAt := time.Now().Add(ttl).Format("2006-01-02 15:04:05")
				resolverResponses = append(resolverResponses, models.ResolverResponse{DNSName: dnsName, IPAddress: ip, ExpiresAt: expiresAt})
			}
		}
		results[dnsName] = resolverResponses
	}

	return results, nil
}
