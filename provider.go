// go-tdx/provider.go — 高层 Provider 接口
//
// 封装 Client 的常用操作，提供：
//   - 自动连接/重连
//   - 批量行情获取 (自动分批)
//   - K线日/分钟数据
//   - 服务器自动选优
package tdx

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// Provider 通达信行情 Provider (高层封装)
type Provider struct {
	mu        sync.Mutex
	client    *Client
	connected bool
	debug     bool
}

// NewProvider 创建 Provider (暂不连接)
func NewProvider() *Provider {
	return &Provider{}
}

// ConnectBest 自动测速并连接最优服务器
func (p *Provider) ConnectBest() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.debug {
		log.Println("[TDX] 开始测速，选择最优服务器...")
	}

	best, all := SelectBestServer(5 * time.Second)
	if best == nil {
		// 输出失败信息
		var msgs []string
		for _, r := range all {
			msgs = append(msgs, fmt.Sprintf("%s: %v", r.Addr.Name, r.Error))
		}
		return fmt.Errorf("所有通达信服务器不可用: %s", strings.Join(msgs, "; "))
	}

	if p.debug {
		log.Printf("[TDX] 最优服务器: %s (%s) 延迟 %v", best.Addr.Name, best.Addr.Host, best.Latency)
	}

	p.client = NewClient(best.Addr.Host, best.Addr.Port)
	p.client.timeout = 8 * time.Second

	err := p.client.Connect()
	if err != nil {
		return fmt.Errorf("连接最优服务器失败: %w", err)
	}

	p.connected = true
	return nil
}

// ConnectTo 连接到指定服务器
func (p *Provider) ConnectTo(host string, port int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.client = NewClient(host, port)
	p.client.timeout = 8 * time.Second

	err := p.client.Connect()
	if err != nil {
		return err
	}

	p.connected = true
	return nil
}

// Close 关闭连接
func (p *Provider) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		p.client.Close()
	}
	p.connected = false
}

// SetDebug 设置调试模式
func (p *Provider) SetDebug(v bool) {
	p.debug = v
}

// IsConnected 检查连接状态
func (p *Provider) IsConnected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connected && p.client.IsConnected()
}

// ── 快捷方法 ────────────────────────────────────────────────────────────────

// TdxKline 获取通达信K线数据 (快捷方法)
type TdxKline struct {
	Symbol string
	Name   string
	Bars   []KlineBar
	Market int
	Code   string
}

// TdxPrice 从K线中提取的实时价格摘要
type TdxPrice struct {
	Symbol    string
	Name      string
	Price     float64    // 最新价 (最后一根K线的收盘价)
	Open      float64    // 今开
	High      float64
	Low       float64
	LastClose float64    // 昨收
	Volume    float64
	Amount    float64
	ChangePct float64    // 涨跌幅 %
	Provider  string
}

// GetPrices 批量获取实时价格 (通过日K线最后一条数据)
//
//	如果 K 线数据足够新 (当日)，价格即为实时价
func (p *Provider) GetPrices(symbols []string) ([]TdxPrice, error) {
	p.mu.Lock()
	if !p.connected || p.client == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("TDX Provider 未连接")
	}
	client := p.client
	p.mu.Unlock()

	var results []TdxPrice

	for _, sym := range symbols {
		market := ResolveMarket(sym)
		bars, err := client.GetKline(CategoryKLineDay, uint16(market), sym, 0, 2)
		if err != nil {
			if p.debug {
				log.Printf("[TDX] 获取 %s K线失败: %v", sym, err)
			}
			continue
		}

		if len(bars) == 0 {
			continue
		}

		// 最后一根是最新的，倒数第二根作为参考
		latest := bars[len(bars)-1]
		var prev KlineBar
		if len(bars) >= 2 {
			prev = bars[len(bars)-2]
		}

		changePct := 0.0
		if prev.Close > 0 {
			changePct = (latest.Close - prev.Close) / prev.Close * 100
		}

		price := TdxPrice{
			Symbol:    sym,
			Price:     latest.Close,
			Open:      latest.Open,
			High:      latest.High,
			Low:       latest.Low,
			LastClose: prev.Close,
			Volume:    latest.Volume,
			Amount:    latest.Amount,
			ChangePct: changePct,
			Provider:  "通达信TCP",
		}

		results = append(results, price)
	}

	return results, nil
}

// GetKline 获取单只股票的K线数据（委托给底层 Client）
func (p *Provider) GetKline(category uint16, market uint16, code string, start uint16, count uint16) ([]KlineBar, error) {
	p.mu.Lock()
	if !p.connected || p.client == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("TDX Provider 未连接")
	}
	client := p.client
	p.mu.Unlock()

	return client.GetKline(category, market, code, start, count)
}

// GetKlines 获取多只股票的K线数据
func (p *Provider) GetKlines(symbols []string, category uint16, count uint16) []TdxKline {
	var results []TdxKline

	p.mu.Lock()
	if !p.connected || p.client == nil {
		p.mu.Unlock()
		return results
	}
	client := p.client
	p.mu.Unlock()

	for _, sym := range symbols {
		market := ResolveMarket(sym)
		bars, err := client.GetKline(category, uint16(market), sym, 0, count)
		if err != nil {
			if p.debug {
				log.Printf("[TDX] 获取 %s K线(cat=%d)失败: %v", sym, category, err)
			}
			continue
		}

		results = append(results, TdxKline{
			Symbol: sym,
			Code:   sym,
			Market: market,
			Bars:   bars,
		})
	}

	return results
}
