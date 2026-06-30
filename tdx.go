// go-tdx/tdx.go — 通达信 TCP 协议客户端
//
// 基于 pytdx/mootdx 的协议分析实现
// 直连通达信行情服务器 7709 端口
// 支持：K线数据 (日/周/月/分钟)、实时行情 (五档)
package tdx

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// ── 常量 ────────────────────────────────────────────────────────────────────

const (
	DefaultPort    = 7709
	DefaultTimeout = 8 * time.Second
	headSize       = 16 // 协议头大小
)

// 市场代码
const (
	MarketSZ = 0 // 深圳
	MarketSH = 1 // 上海
)

// K线种类 (Category — 与 pytdx 兼容)
const (
	CategoryKLine5Min  = 0  // 5分钟K线
	CategoryKLine15Min = 1  // 15分钟K线
	CategoryKLine30Min = 2  // 30分钟K线
	CategoryKLine1Hour = 3  // 1小时K线
	CategoryKLineDay   = 4  // 日K线 (pytdx 主用)
	CategoryKLineWeek  = 5  // 周K线
	CategoryKLineMonth = 6  // 月K线
	CategoryKLine1Min  = 7  // 1分钟K线
	CategoryKLineDay9  = 9  // 日K线 (pytdx 备选/高版本)
	CategoryKLineQtr   = 10 // 季K线
	CategoryKLineYear  = 11 // 年K线
)

// 命令 ID
const (
	cmdSecurityBars   = 1 // 获取K线数据
	cmdSecurityCount  = 2 // 获取K线数量
	cmdSecurityQuotes = 4 // 获取实时行情 (命令字与 pytdx 对齐)
	cmdSecurityList   = 5 // 获取股票列表
)

// ── 数据结构 ────────────────────────────────────────────────────────────────

// KlineBar 单根K线
type KlineBar struct {
	Date   string  // 日期，格式 YYYYMMDD (或 YYYYMMDDHHMM)
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Amount float64 // 成交额 (元)
	Volume float64 // 成交量 (股)
}

// QuoteData 实时行情 (五档)
type QuoteData struct {
	Market     int
	Code       string
	Name       string  // 股票名称 (需从其他接口获取)
	Price      float64 // 最新价
	LastClose  float64 // 昨收
	Open       float64 // 今开
	High       float64 // 最高
	Low        float64 // 最低
	Volume     float64 // 成交量
	Amount     float64 // 成交额
	BuyPrices  [5]float64
	BuyVolumes [5]float64
	SellPrices [5]float64
	SellVolumes[5]float64
}

// ServerInfo 服务器测速信息
type ServerInfo struct {
	Host    string
	Port    int
	Latency time.Duration
	OK      bool
}

// ── 客户端 ──────────────────────────────────────────────────────────────────

// Client 通达信 TCP 客户端
type Client struct {
	mu       sync.Mutex
	conn     net.Conn
	host     string
	port     int
	timeout  time.Duration
	seqID    uint16
	bestHost string
	bestPort int
}

// NewClient 创建客户端
func NewClient(host string, port int) *Client {
	return &Client{
		host:     host,
		port:     port,
		timeout:  DefaultTimeout,
		bestHost: host,
		bestPort: port,
	}
}

// Connect 建立连接
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	conn, err := net.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return fmt.Errorf("连接通达信服务器 %s 失败: %w", addr, err)
	}
	c.conn = conn
	c.seqID = 0
	return nil
}

// Close 关闭连接
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// IsConnected 检查连接状态
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

// ── 核心协议 ────────────────────────────────────────────────────────────────

// sendPacket 发送请求包并接收响应
//
// 通达信协议帧格式 (发送):
//
//	[0-1]  uint16 magic (0x0c01 = body不压缩; 0x0c02 = body用zlib压缩)
//	[2-3]  uint16 zipped_length   (压缩后的 body 长度)
//	[4-5]  uint16 unzipped_length (压缩前的 body 长度)
//	[6-7]  uint16 seq_id
//	[8-9]  uint16 cmd_group
//	[10-11]uint16 cmd
//	[12-13]uint16 sub_cmd / market
//	[14-15]uint16 reserved
//
//	然后可选跟 body (可能zlib压缩)
func (c *Client) sendPacket(cmdGroup uint16, cmd uint16, subCmd uint16, body []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, fmt.Errorf("未连接到服务器")
	}

	c.seqID++

	// 构建 header (16 字节)
	// 如果 body 非空则尝试压缩
	var zipLen, unzipLen uint16
	var bodyData []byte

	if len(body) > 0 {
		unzipLen = uint16(len(body))

		// zlib 压缩
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		if _, err := w.Write(body); err != nil {
			return nil, fmt.Errorf("压缩body失败: %w", err)
		}
		w.Close()
		bodyData = buf.Bytes()
		zipLen = uint16(len(bodyData))
	} else {
		bodyData = nil
		zipLen = 0
		unzipLen = 0
	}

	// 使用 0x0c01 (不压缩) 或 0x0c02 (zlib压缩)?
	// 如果 body < 256 字节则不压缩，否则压缩
	magic := uint16(0x0c01)
	if len(bodyData) < len(body) {
		magic = 0x0c02
	} else {
		// 不压缩，使用原始 body
		bodyData = body
		zipLen = uint16(len(body))
		unzipLen = uint16(len(body))
	}

	header := make([]byte, headSize)
	binary.LittleEndian.PutUint16(header[0:2], magic)
	binary.LittleEndian.PutUint16(header[2:4], zipLen)
	binary.LittleEndian.PutUint16(header[4:6], unzipLen)
	binary.LittleEndian.PutUint16(header[6:8], c.seqID)
	binary.LittleEndian.PutUint16(header[8:10], cmdGroup)
	binary.LittleEndian.PutUint16(header[10:12], cmd)
	binary.LittleEndian.PutUint16(header[12:14], subCmd)
	binary.LittleEndian.PutUint16(header[14:16], 0)

	// 发送 header + body
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := c.conn.Write(header); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("发送header失败: %w", err)
	}
	if len(bodyData) > 0 {
		if _, err := c.conn.Write(bodyData); err != nil {
			c.conn.Close()
			c.conn = nil
			return nil, fmt.Errorf("发送body失败: %w", err)
		}
	}

	// 接收响应 header
	respHeader := make([]byte, headSize)
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("接收响应header失败: %w", err)
	}

	respZipLen := binary.LittleEndian.Uint16(respHeader[2:4])
	respUnzipLen := binary.LittleEndian.Uint16(respHeader[4:6])

	if respUnzipLen == 0 {
		return nil, nil
	}

	// 接收响应 body
	respBody := make([]byte, respZipLen)
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := io.ReadFull(c.conn, respBody); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("接收响应body失败: %w", err)
	}

	// 判断是否需要解压
	respMagic := binary.LittleEndian.Uint16(respHeader[0:2])
	if respMagic == 0x0c02 && respUnzipLen != respZipLen {
		// zlib 解压
		r, err := zlib.NewReader(bytes.NewReader(respBody))
		if err != nil {
			return nil, fmt.Errorf("创建解压器失败: %w", err)
		}
		defer r.Close()
		uncompressed := make([]byte, respUnzipLen)
		if _, err := io.ReadFull(r, uncompressed); err != nil {
			return nil, fmt.Errorf("解压响应失败: %w", err)
		}
		return uncompressed, nil
	}

	return respBody, nil
}

// ── K线数据 ──────────────────────────────────────────────────────────────────

// GetKline 获取K线数据
//
//	category: K线种类 (CategoryKLineDay 等)
//	market:   市场 (MarketSZ:0 深圳, MarketSH:1 上海)
//	code:     6位股票代码
//	start:    起始位置 (0=最新)
//	count:    获取数量 (最大 800)
func (c *Client) GetKline(category uint16, market uint16, code string, start uint16, count uint16) ([]KlineBar, error) {
	// 构建 body: category(2) + market(2) + code(6) + start(2) + count(2) = 14 字节
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, category)
	binary.Write(buf, binary.LittleEndian, market)

	// code: 固定 6 字节，不足补 0
	codeBytes := []byte(code)
	codePad := make([]byte, 6)
	copy(codePad, codeBytes)
	binary.Write(buf, binary.LittleEndian, codePad)

	binary.Write(buf, binary.LittleEndian, start)
	binary.Write(buf, binary.LittleEndian, count)

	// command: cmdGroup=0x01, cmd=0x01, subCmd=market
	resp, err := c.sendPacket(0x01, cmdSecurityBars, market, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("获取K线失败 [%s]: %w", code, err)
	}

	return parseKlineBars(resp, count)
}

// parseKlineBars 解析K线数据
//
// 通达信每根K线 32 字节:
//
//	[0-3]  uint32  date (YYYYMMDD)
//	[4-7]  uint32  open   (乘以100后的价格，单位：分)
//	[8-11] uint32  high
//	[12-15] uint32 low
//	[16-19] uint32 close
//	[20-23] float32 amount (成交额)
//	[24-27] uint32  volume (成交量)
//	[28-31] uint32  reserved
func parseKlineBars(data []byte, expectedCount uint16) ([]KlineBar, error) {
	const barSize = 32

	if len(data) == 0 {
		return nil, fmt.Errorf("K线响应数据为空")
	}

	n := len(data) / barSize
	if n == 0 {
		return nil, fmt.Errorf("K线数据长度不足以解析")
	}

	bars := make([]KlineBar, 0, n)

	for i := 0; i < n; i++ {
		off := i * barSize
		if off+barSize > len(data) {
			break
		}
		raw := data[off : off+barSize]

		dateVal := binary.LittleEndian.Uint32(raw[0:4])
		openVal := binary.LittleEndian.Uint32(raw[4:8])
		highVal := binary.LittleEndian.Uint32(raw[8:12])
		lowVal := binary.LittleEndian.Uint32(raw[12:16])
		closeVal := binary.LittleEndian.Uint32(raw[16:20])
		amountVal := float64(binary.LittleEndian.Uint32(raw[20:24])) // float32 as uint32 bits
		volumeVal := binary.LittleEndian.Uint32(raw[24:28])

		// 转换价格：分 → 元
		bars = append(bars, KlineBar{
			Date:   fmt.Sprintf("%08d", dateVal),
			Open:   float64(openVal) / 100.0,
			High:   float64(highVal) / 100.0,
			Low:    float64(lowVal) / 100.0,
			Close:  float64(closeVal) / 100.0,
			Amount: amountVal,
			Volume: float64(volumeVal),
		})
	}

	return bars, nil
}

// ── 实时行情 ──────────────────────────────────────────────────────────────────

// GetQuotes 获取实时行情
//
//	market: 市场
//	codes:  股票代码列表
func (c *Client) GetQuotes(market uint16, codes []string) ([]QuoteData, error) {
	if len(codes) == 0 {
		return nil, nil
	}

	// 构建 body: count(2) + for each: market(1) + code(6)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint16(len(codes)))

	codeBytes := make([]byte, 6)
	for _, code := range codes {
		binary.Write(buf, binary.LittleEndian, uint8(market))
		copy(codeBytes, []byte(code))
		binary.Write(buf, binary.LittleEndian, codeBytes)
	}

	resp, err := c.sendPacket(0x01, cmdSecurityQuotes, market, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("获取行情失败: %w", err)
	}

	return parseQuotes(resp, market, codes)
}

// parseQuotes 解析实时行情响应
//
// 每条行情约 400 字节，包含:
//
//	market, code, 名称, 昨收, 今开, 最高, 最低,
//	最新价, 成交量, 成交额, 买1-5价/量, 卖1-5价/量
func parseQuotes(data []byte, market uint16, codes []string) ([]QuoteData, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// 通达信行情每只股票固定长度 (pytdx 中每个股票约 420 字节)
	// 安全检查
	if len(data) < 100 {
		return nil, fmt.Errorf("行情数据太短: %d 字节", len(data))
	}

	// pytdx 中 stock_quotes 每条记录的大小约为 400-420 字节
	const quoteSize = 420

	var quotes []QuoteData
	_ = codes // 实际解析时会用到

	for i := 0; i < len(data)/quoteSize && i < len(codes); i++ {
		off := i * quoteSize
		if off+quoteSize > len(data) {
			break
		}
		raw := data[off : off+quoteSize]

		q := QuoteData{
			Market: int(market),
			Code:   codes[i],
		}

		// 跳过前 20 字节（市场/代码等）
		// 价格字段从偏移 20 开始 (pytdx 格式)

		// 字段映射 (与 pytdx 对齐):
		// 偏移 4: 昨收 (int32, 分)
		// 偏移 8: 今开
		// 偏移 12: 最高
		// 偏移 16: 最低
		// 偏移 20: 最新价
		// 偏移 28: 成交量
		// ...

		if len(raw) >= 48 {
			q.LastClose = float64(int32(binary.LittleEndian.Uint32(raw[4:8]))) / 100.0
			q.Open = float64(int32(binary.LittleEndian.Uint32(raw[8:12]))) / 100.0
			q.High = float64(int32(binary.LittleEndian.Uint32(raw[12:16]))) / 100.0
			q.Low = float64(int32(binary.LittleEndian.Uint32(raw[16:20]))) / 100.0
			q.Price = float64(int32(binary.LittleEndian.Uint32(raw[20:24]))) / 100.0

			// 成交量/成交额
			if len(raw) >= 36 {
				q.Volume = float64(binary.LittleEndian.Uint32(raw[28:32]))
				q.Amount = float64(float32(binary.LittleEndian.Uint32(raw[32:36])))
			}

			// 五档买卖盘口 (从偏移 80 附近开始)
			if len(raw) >= 220 {
				for j := 0; j < 5; j++ {
					base := 80 + j*20
					if base+16 <= len(raw) {
						q.BuyPrices[j] = float64(int32(binary.LittleEndian.Uint32(raw[base:base+4]))) / 100.0
						q.BuyVolumes[j] = float64(binary.LittleEndian.Uint32(raw[base+4 : base+8]))
						q.SellPrices[j] = float64(int32(binary.LittleEndian.Uint32(raw[base+8 : base+12]))) / 100.0
						q.SellVolumes[j] = float64(binary.LittleEndian.Uint32(raw[base+12 : base+16]))
					}
				}
			}
		}

		quotes = append(quotes, q)
	}

	return quotes, nil
}

// ── 工具函数 ──────────────────────────────────────────────────────────────────

// ResolveMarket 根据股票代码判断市场
func ResolveMarket(code string) int {
	if len(code) >= 1 {
		first := code[0]
		if first == '6' || first == '5' || first == '9' {
			return MarketSH
		}
	}
	return MarketSZ
}

// CategoryForPeriod 根据周期字符串返回 category
//
//	"1min" → CategoryKLine1Min, "5min" → CategoryKLine5Min,
//	"15min" → CategoryKLine15Min, "30min" → CategoryKLine30Min,
//	"60min" → CategoryKLine1Hour, "day" → CategoryKLineDay,
//	"week" → CategoryKLineWeek, "month" → CategoryKLineMonth
func CategoryForPeriod(period string) uint16 {
	switch period {
	case "1min":
		return CategoryKLine1Min
	case "5min":
		return CategoryKLine5Min
	case "15min":
		return CategoryKLine15Min
	case "30min":
		return CategoryKLine30Min
	case "60min":
		return CategoryKLine1Hour
	case "day":
		return CategoryKLineDay
	case "week":
		return CategoryKLineWeek
	case "month":
		return CategoryKLineMonth
	default:
		return CategoryKLineDay
	}
}

// DialTimeout 测试 TCP 连接到指定地址的延迟（不进行协议握手）
func DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}
