// go-tdx/benchmark.go — 服务器测速与自动选优
package tdx

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 已知通达信行情服务器列表
//
// 注意：公共行情主站（101.227.73.20 / 114.80.149.92 / 202.108.253.131 / 180.153.18.170 等）
// 近年已被大面积封锁——TCP 能连上但行情请求一律返回空体，无法取到数据。
// 这里改为使用券商（方正证券）专属行情主站，与桌面版小方客户端 connect.cfg 一致。
// 已在本机 pytdx 实测可取到真实行情的服务器排在前面。
// 也可用环境变量 TDX_HQ_SERVERS（格式 host:port,host:port,...）覆盖本清单。
var KnownServers = []ServerAddr{
	{"方正证券-长沙一", "124.232.133.136", 7709}, // 实测可用
	{"方正证券-长沙联通一", "110.53.177.135", 7709}, // 实测可用
	{"方正证券-长沙移动一", "111.23.148.7", 7709},   // 实测可用
	{"方正证券数据中心", "hq1.foundersc.com", 7709},
	{"方正证券-证通一", "101.226.9.232", 7709},
	{"方正证券-证通二", "175.6.5.151", 7709},
	{"方正证券-证通通信一", "123.125.108.55", 7709},
	{"方正证券-证通通信二", "182.118.47.184", 7709},
	{"方正证券-长沙二", "175.6.30.71", 7709},
	{"方正证券-长沙联通二", "110.53.162.71", 7709},
	{"方正证券-长沙移动二", "111.23.148.73", 7709},
}

// ServerAddr 服务器地址
type ServerAddr struct {
	Name string
	Host string
	Port int
}

// BenchmarkResult 测速结果
type BenchmarkResult struct {
	Addr    ServerAddr
	Latency time.Duration
	OK      bool
	Error   error
}

// loadServers 返回用于测速的服务器列表：默认使用 KnownServers，
// 若设置了环境变量 TDX_HQ_SERVERS（格式 "host:port,host:port,..."）则以其为准。
func loadServers() []ServerAddr {
	env := os.Getenv("TDX_HQ_SERVERS")
	if strings.TrimSpace(env) == "" {
		return KnownServers
	}
	var out []ServerAddr
	for i, item := range strings.Split(env, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		host := item
		port := 7709
		if h, p, ok := strings.Cut(item, ":"); ok && p != "" {
			if pv, err := strconv.Atoi(p); err == nil {
				host, port = h, pv
			}
		}
		out = append(out, ServerAddr{Name: fmt.Sprintf("ENV-%d", i), Host: host, Port: port})
	}
	if len(out) == 0 {
		return KnownServers
	}
	return out
}

// Benchmark 对已知服务器列表进行测速
// 返回按延迟排序的结果列表
func Benchmark(timeout time.Duration) []BenchmarkResult {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []BenchmarkResult

	for _, addr := range loadServers() {
		wg.Add(1)
		go func(a ServerAddr) {
			defer wg.Done()

			start := time.Now()
			client := NewClient(a.Host, a.Port)
			client.timeout = timeout

			err := client.Connect()
			if err != nil {
				mu.Lock()
				results = append(results, BenchmarkResult{
					Addr:  a,
					OK:    false,
					Error: err,
				})
				mu.Unlock()
				return
			}
			defer client.Close()

			// 发一个简单的 K线请求测试
			_, err = client.GetKline(CategoryKLineDay, MarketSZ, "000001", 0, 1)
			client.Close()

			latency := time.Since(start)

			mu.Lock()
			if err != nil {
				results = append(results, BenchmarkResult{
					Addr:  a,
					OK:    false,
					Error: fmt.Errorf("数据请求失败: %w", err),
				})
			} else {
				results = append(results, BenchmarkResult{
					Addr:    a,
					Latency: latency,
					OK:      true,
				})
			}
			mu.Unlock()
		}(addr)
	}

	wg.Wait()

	// 按延迟升序排列
	sort.Slice(results, func(i, j int) bool {
		// 成功的排前面
		if results[i].OK != results[j].OK {
			return results[i].OK
		}
		return results[i].Latency < results[j].Latency
	})

	return results
}

// SelectBestServer 选择最快的服务器
// 返回测速结果和最快的一组服务器地址
func SelectBestServer(timeout time.Duration) (*BenchmarkResult, []BenchmarkResult) {
	results := Benchmark(timeout)
	for i := range results {
		if results[i].OK {
			return &results[i], results
		}
	}
	return nil, results
}
