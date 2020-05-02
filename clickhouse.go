package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/nikepan/go-datastructures/queue"
)

// ClickhouseServer - clickhouse server instance object struct
type ClickhouseServer struct {
	URL         string
	Host        string
	LastRequest time.Time
	Bad         bool
	Client      *http.Client
}

// Clickhouse - main clickhouse sender object
type Clickhouse struct {
	Servers        []*ClickhouseServer
	Queue          *queue.Queue
	mu             sync.Mutex
	DownTimeout    int
	ConnectTimeout int
	Dumper         Dumper
	wg             sync.WaitGroup
}

// ClickhouseRequest - request struct for queue
type ClickhouseRequest struct {
	Params  string
	Query   string
	Content string
	Count   int
}

// ErrServerIsDown - signals about server is down
var ErrServerIsDown = errors.New("server is down")

// ErrNoServers - signals about no working servers
var ErrNoServers = errors.New("No working clickhouse servers")

// NewClickhouse - get clickhouse object
func NewClickhouse(downTimeout int, connectTimeout int) (c *Clickhouse) {
	c = new(Clickhouse)
	c.DownTimeout = downTimeout
	c.ConnectTimeout = connectTimeout
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10
	}
	c.Servers = make([]*ClickhouseServer, 0)
	c.Queue = queue.New(1000)
	go c.Run()
	return c
}

// AddServer - add clickhouse server url
func (c *Clickhouse) AddServer(urlIn string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	serverIps, host, _ := resolveServer(urlIn)
	if serverIps != nil {
		for _, serverIP := range serverIps {
			urlWithIP, _ := url.Parse(urlIn)
			urlWithIP.Host = serverIP.String() + ":" + urlWithIP.Port()
			tlsConfig := tls.Config{
				ServerName: host,
			}
			tr := &http.Transport{
				TLSClientConfig: &tlsConfig,
			}
			c.Servers = append(c.Servers, &ClickhouseServer{URL: urlWithIP.String(), Host: host, Client: &http.Client{
				Timeout: time.Second * time.Duration(c.ConnectTimeout), Transport: tr,
			}})
		}
	} else {
		c.Servers = append(c.Servers, &ClickhouseServer{URL: urlIn, Client: &http.Client{
			Timeout: time.Second * time.Duration(c.ConnectTimeout),
		}})
	}
}

func resolveServer(urlIn string) (ips []net.IP, host string, ttl uint32) {
	if host, err := url.Parse(urlIn); err == nil {
		h := host.Hostname()
		i := net.ParseIP(h)
		if i != nil { // it's IP so return just IP with TTL 0
			out := make([]net.IP, 1)
			out[0] = i
			return out, h, 0
		}
		conf, err := dns.ClientConfigFromFile("/etc/resolv.conf")
		if err != nil {
			fmt.Println(err)
		}
		nameserver := conf.Servers[0]
		port := 53
		if i := net.ParseIP(nameserver); i != nil {
			nameserver = net.JoinHostPort(nameserver, strconv.Itoa(port))
		} else {
			nameserver = dns.Fqdn(nameserver) + ":" + strconv.Itoa(port)
		}
		m := new(dns.Msg)
		m.Id = dns.Id()
		m.RecursionDesired = true
		m.Question = make([]dns.Question, 1)
		m.Question[0] = dns.Question{dns.Fqdn(h), dns.TypeA, dns.ClassINET}
		c := new(dns.Client)
		if in, rtt, err := c.Exchange(m, nameserver); err == nil {
			var out []net.IP
			var ttl uint32
			for _, ans := range in.Answer {
				if a, ok := ans.(*dns.A); ok {
					out = append(out, a.A.To4())
				}
				ttl = ans.Header().Ttl
			}
			log.Printf("INFO: resolved (%+v) in %+v to IPs: %+v\n", h, rtt, out)
			return out, h, ttl
		}
	} else {
		log.Fatal(err)
		return nil, "", 0
	}
	return nil, "", 0
}

// DumpServers - dump servers state to prometheus
func (c *Clickhouse) DumpServers() {
	c.mu.Lock()
	defer c.mu.Unlock()
	good := 0
	bad := 0
	for _, s := range c.Servers {
		if s.Bad {
			bad++
		} else {
			good++
		}
	}
	goodServers.Set(float64(good))
	badServers.Set(float64(bad))
}

// GetNextServer - getting next server for request
func (c *Clickhouse) GetNextServer() (srv *ClickhouseServer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tnow := time.Now()
	for _, s := range c.Servers {
		if s.Bad {
			if tnow.Sub(s.LastRequest) > time.Second*time.Duration(c.DownTimeout) {
				s.Bad = false
			} else {
				continue
			}
		}
		if srv != nil {
			if srv.LastRequest.Sub(s.LastRequest) > 0 {
				srv = s
			}
		} else {
			srv = s
		}
	}
	if srv != nil {
		srv.LastRequest = time.Now()
	}
	return srv

}

// Send - send request to next server
func (c *Clickhouse) Send(r *ClickhouseRequest) {
	c.wg.Add(1)
	c.Queue.Put(r)
}

// Dump - save query to file
func (c *Clickhouse) Dump(params string, content string, response string, prefix string, status int) error {
	dumpCounter.Inc()
	if c.Dumper != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.Dumper.Dump(params, content, response, prefix, status)
	}
	return nil
}

// Len - returns queries queue length
func (c *Clickhouse) Len() int64 {
	return c.Queue.Len()
}

// Empty - check if queue is empty
func (c *Clickhouse) Empty() bool {
	return c.Queue.Empty()
}

// Run server
func (c *Clickhouse) Run() {
	var err error
	var datas []interface{}
	for {
		datas, err = c.Queue.Poll(1, time.Second*5)
		if err == nil {
			data := datas[0].(*ClickhouseRequest)
			resp, status, err := c.SendQuery(data)
			if err != nil {
				log.Printf("ERROR: Send (%+v) %+v; response %+v\n", status, err, resp)
				prefix := "1"
				if status >= 400 && status < 502 {
					prefix = "2"
				}
				c.Dump(data.Params, data.Content, resp, prefix, status)
			} else {
				sentCounter.Inc()
			}
			c.DumpServers()
			c.wg.Done()
		}
	}
}

// WaitFlush - wait for flush ends
func (c *Clickhouse) WaitFlush() (err error) {
	c.wg.Wait()
	return nil
}

// SendQuery - sends query to server and return result
func (srv *ClickhouseServer) SendQuery(r *ClickhouseRequest) (response string, status int, err error) {
	if srv.URL != "" {
		url := srv.URL
		if r.Params != "" {
			url += "?" + r.Params
		}
		log.Printf("INFO: send %+v rows to %+v of %+v\n", r.Count, srv.URL, r.Query)
		req, err := http.NewRequest("POST", url, strings.NewReader(r.Content))
		req.Header.Set("Host", srv.Host)
		resp, err := srv.Client.Do(req)
		if err != nil {
			srv.Bad = true
			return err.Error(), http.StatusBadGateway, ErrServerIsDown
		}
		buf, _ := ioutil.ReadAll(resp.Body)
		s := string(buf)
		if resp.StatusCode >= 502 {
			srv.Bad = true
			err = ErrServerIsDown
		} else if resp.StatusCode >= 400 {
			err = fmt.Errorf("Wrong server status %+v:\nresponse: %+v\nrequest: %#v", resp.StatusCode, s, r.Content)
		}
		return s, resp.StatusCode, err
	}

	return "", http.StatusOK, err
}

// SendQuery - sends query to server and return result (with server cycle)
func (c *Clickhouse) SendQuery(r *ClickhouseRequest) (response string, status int, err error) {
	for {
		s := c.GetNextServer()
		if s != nil {
			response, status, err = s.SendQuery(r)
			if errors.Is(err, ErrServerIsDown) {
				log.Printf("ERROR: server down (%+v): %+v\n", status, response)
				continue
			}
			return response, status, err
		}
		return response, status, ErrNoServers
	}
}
