// go-tdx/benchmark.go — 服务器测速与自动选优
package tdx

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// 已知通达信行情服务器列表
var KnownServers = []ServerAddr{
	{"上海电信", "101.227.73.20", 7709},
	{"上海电信2", "101.226.10.92", 7709},
	{"深圳电信", "114.80.149.92", 7709},
	{"北京联通", "123.125.108.90", 7709},
	{"广州电信", "221.194.181.81", 7709},
	{"通达信主站", "202.108.253.131", 7709},
	{"深圳行情", "119.147.212.81", 7709},
	{"上海行情", "180.153.18.170", 7709},
	{"上海行情2", "180.153.18.171", 7709},
	{"上海行情3", "103.2.234.199", 7709},
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

// Benchmark 对已知服务器列表进行测速
// 返回按延迟排序的结果列表
func Benchmark(timeout time.Duration) []BenchmarkResult {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []BenchmarkResult

	for _, addr := range KnownServers {
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
