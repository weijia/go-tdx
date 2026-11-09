// go-tdx/tdx.go — 通达信 TCP 协议客户端
//
// 协议严格对齐 pytdx（实测抓包 + 源码核对）：
//   - 登录握手：连接后依次发送 3 个 setup 包，消费掉服务器回包，否则首个行情请求会被断开。
//   - 请求帧：单帧 struct.pack("<HIHHHH6sHHHHIIH" / "<HIHHIIHH+<B6s>...")，无额外 16 字节封装。
//   - 响应帧：16 字节头 + 体；体长度取 header[12:14](uint16 zipsize)，
//     header[14:16] 为 unzipsize；zipsize != unzipsize 时体为 zlib 压缩，需解压。
//   - 行情/分时均以"差异增量"编码（get_price 变长整数），价格按 /1000（K线、基金五档）
//     或 /100（股票五档）还原；基金/ETF 用 3 位小数，与股票的 2 位小数不同。
//
// 直连通达信行情服务器 7709 端口，支持 K线（日/周/月/分钟）与实时五档行情。
package tdx

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
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
	CategoryKLineDay   = 4  // 日K线 (pytdx/mootdx 主用, 方正证券主站实测可用)
	CategoryKLineWeek  = 5  // 周K线
	CategoryKLineMonth = 6  // 月K线
	CategoryKLine1Min  = 7  // 1分钟K线
	CategoryKLineDay9  = 9  // 日K线 (部分服务端/国际版)
	CategoryKLineQtr   = 10 // 季K线
	CategoryKLineYear  = 11 // 年K线
)

// ── 数据结构 ────────────────────────────────────────────────────────────────

// KlineBar 单根K线
type KlineBar struct {
	Date   string  // 日期，日线 YYYYMMDD；分钟线 YYYYMMDDHHMM
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Amount float64 // 成交额 (元)
	Volume float64 // 成交量 (股/手)
}

// QuoteData 实时行情 (五档)
type QuoteData struct {
	Market     int
	Code       string
	Name       string  // 股票名称 (本协议不含名称)
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
	SellVolumes [5]float64
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

// Connect 建立连接（含 TDX 登录握手）
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

	// TDX 登录握手：部分券商行情服务器要求在发送任何行情请求前先完成握手，
	// 否则会在首个数据请求时直接断开连接（表现为 EOF）。包内容来自 pytdx 实测抓包。
	if err := c.setupHandshake(); err != nil {
		c.conn.Close()
		c.conn = nil
		return fmt.Errorf("TDX 握手失败: %w", err)
	}
	return nil
}

// tdxHandshakePackets TDX 登录握手包（来自 pytdx 实测抓包，连接后必须依次发送）
var tdxHandshakePackets = [][]byte{
	{0x0c, 0x02, 0x18, 0x93, 0x00, 0x01, 0x03, 0x00, 0x03, 0x00, 0x0d, 0x00, 0x01},
	{0x0c, 0x02, 0x18, 0x94, 0x00, 0x01, 0x03, 0x00, 0x03, 0x00, 0x0d, 0x00, 0x02},
	{
		0x0c, 0x03, 0x18, 0x99, 0x00, 0x01, 0x20, 0x00, 0x20, 0x00,
		0xdb, 0x0f, 0xd5, 0xd0, 0xc9, 0xcc, 0xd6, 0xa4, 0xa8, 0xaf,
		0x00, 0x00, 0x00, 0x8f, 0xc2, 0x25, 0x40, 0x13, 0x00, 0x00,
		0xd5, 0x00, 0xc9, 0xcc, 0xbd, 0xf0, 0xd7, 0xea, 0x00, 0x00, 0x00, 0x02,
	},
}

// setupHandshake 发送 TDX 登录握手包并消费服务器响应，保持 TCP 流同步。
func (c *Client) setupHandshake() error {
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	for _, pkt := range tdxHandshakePackets {
		if _, err := c.conn.Write(pkt); err != nil {
			return fmt.Errorf("发送握手包失败: %w", err)
		}
		// 读取并丢弃服务器响应（16 字节头 + 体），避免流错位
		if _, err := c.readResponse(); err != nil {
			return fmt.Errorf("接收握手响应失败: %w", err)
		}
	}
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

// ── 底层收发 ─────────────────────────────────────────────────────────────────

// sendRaw 发送整帧请求并读取响应（16 字节头 + 体，按 pytdx 协议解压）。
func (c *Client) sendRaw(pkg []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, fmt.Errorf("未连接到服务器")
	}

	c.conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := c.conn.Write(pkg); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("发送请求失败: %w", err)
	}
	return c.readResponse()
}

// readResponse 读取一次 TDX 响应：16 字节头 + 体。
// header 布局（小端）：<IIIHH → [0:4]魔法 [4:8] [8:12] [12:14]zipsize [14:16]unzipsize。
// 当 zipsize != unzipsize 时，体为 zlib 压缩，需解压。
func (c *Client) readResponse() ([]byte, error) {
	respHeader := make([]byte, headSize)
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("接收响应header失败: %w", err)
	}

	zipsize := binary.LittleEndian.Uint16(respHeader[12:14])
	unzipsize := binary.LittleEndian.Uint16(respHeader[14:16])

	if zipsize == 0 {
		return nil, nil
	}

	body := make([]byte, zipsize)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("接收响应body失败: %w", err)
	}

	if zipsize != unzipsize {
		r, err := zlib.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("创建解压器失败: %w", err)
		}
		defer r.Close()
		out, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("解压响应失败: %w", err)
		}
		return out, nil
	}
	return body, nil
}

// ── 协议解码辅助（对齐 pytdx/helper.py）────────────────────────────────────

// getPrice 解析变长带符号整数（1~n 字节，对齐 pytdx helper.get_price）。
// 每个字节：低 6 位为数据；0x40 为符号位；0x80 为续字节标志（后续每字节低 7 位累加）。
func getPrice(data []byte, pos int) (int, int) {
	if pos >= len(data) {
		return 0, pos
	}
	posByte := 6
	bdata := int(data[pos])
	intdata := bdata & 0x3f
	sign := (bdata & 0x40) != 0

	if bdata&0x80 != 0 {
		for {
			pos++
			if pos >= len(data) {
				break
			}
			bdata = int(data[pos])
			intdata += (bdata & 0x7f) << posByte
			posByte += 7
			if bdata&0x80 == 0 {
				break
			}
		}
	}
	pos++
	if sign {
		intdata = -intdata
	}
	return intdata, pos
}

// getPriceAt 读取一个变长整数并就地前移位置指针（避免调用处用 := 意外遮蔽外层 pos）。
func getPriceAt(data []byte, pos *int) int {
	v, p := getPrice(data, *pos)
	*pos = p
	return v
}

// getDatetime 解析 K 线时间戳（对齐 pytdx helper.get_datetime）。
// category<4 或 ==7 或 ==8 为分钟线，日期+时间打包在 4 字节；其余为日线，4 字节即 YYYYMMDD。
func getDatetime(category uint16, data []byte, pos int) (int, int, int, int, int, int) {
	year, month, day, hour, minute := 0, 0, 0, 15, 0
	if category < 4 || category == 7 || category == 8 {
		zipday := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		tminutes := int(binary.LittleEndian.Uint16(data[pos+2 : pos+4]))
		year = (zipday >> 11) + 2004
		month = (zipday % 2048) / 100
		day = (zipday % 2048) % 100
		hour = tminutes / 60
		minute = tminutes % 60
	} else {
		zipday := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		year = zipday / 10000
		month = (zipday % 10000) / 100
		day = zipday % 100
	}
	pos += 4
	return year, month, day, hour, minute, pos
}

// getVolume 还原成交量/成交额（对齐 pytdx helper.get_volume 的自定义浮点解码）。
// 注意：并非标准 IEEE-754；例如原始值 0 会得到约 5.877e-39。必须逐位还原以保证与 pytdx 一致。
func getVolume(ivol uint32) float64 {
	logpoint := ivol >> 24
	hleax := (ivol >> 16) & 0xff
	lheax := (ivol >> 8) & 0xff
	lleax := ivol & 0xff

	dwEcx := int(logpoint)*2 - 0x7f
	dwEdx := int(logpoint)*2 - 0x86
	dwEsi := int(logpoint)*2 - 0x8e
	dwEax := int(logpoint)*2 - 0x96

	tmpEax := dwEcx
	if dwEcx < 0 {
		tmpEax = -dwEcx
	}

	dblXmm6 := math.Pow(2.0, float64(tmpEax))
	if dwEcx < 0 {
		dblXmm6 = 1.0 / dblXmm6
	}

	var dblXmm4 float64
	if hleax > 0x80 {
		dwtmpeax := dwEdx + 1
		tmpdblXmm3 := math.Pow(2.0, float64(dwtmpeax))
		dblXmm0 := math.Pow(2.0, float64(dwEdx))*128.0 + float64(hleax&0x7f)*tmpdblXmm3
		dblXmm4 = dblXmm0
	} else {
		var dblXmm0 float64
		if dwEdx >= 0 {
			dblXmm0 = math.Pow(2.0, float64(dwEdx)) * float64(hleax)
		} else {
			dblXmm0 = (1.0 / math.Pow(2.0, float64(dwEdx))) * float64(hleax)
		}
		dblXmm4 = dblXmm0
	}

	dblXmm3 := math.Pow(2.0, float64(dwEsi)) * float64(lheax)
	dblXmm1 := math.Pow(2.0, float64(dwEax)) * float64(lleax)
	if hleax&0x80 != 0 {
		dblXmm3 *= 2.0
		dblXmm1 *= 2.0
	}

	return dblXmm6 + dblXmm4 + dblXmm3 + dblXmm1
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
	codeBytes := make([]byte, 6)
	copy(codeBytes, []byte(code))

	// 对齐 pytdx GetSecurityBarsCmd: struct.pack("<HIHHHH6sHHHHIIH", ...)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint16(0x10c))
	binary.Write(buf, binary.LittleEndian, uint32(0x01016408))
	binary.Write(buf, binary.LittleEndian, uint16(0x1c))
	binary.Write(buf, binary.LittleEndian, uint16(0x1c))
	binary.Write(buf, binary.LittleEndian, uint16(0x052d))
	binary.Write(buf, binary.LittleEndian, uint16(market))
	buf.Write(codeBytes)
	binary.Write(buf, binary.LittleEndian, uint16(category))
	binary.Write(buf, binary.LittleEndian, uint16(1))
	binary.Write(buf, binary.LittleEndian, uint16(start))
	binary.Write(buf, binary.LittleEndian, uint16(count))
	binary.Write(buf, binary.LittleEndian, uint32(0))
	binary.Write(buf, binary.LittleEndian, uint32(0))
	binary.Write(buf, binary.LittleEndian, uint16(0))

	resp, err := c.sendRaw(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("获取K线失败 [%s]: %w", code, err)
	}
	return parseKlineBars(resp, category)
}

// parseKlineBars 解析 K 线数据（对齐 pytdx GetSecurityBarsCmd.parseResponse）
func parseKlineBars(data []byte, category uint16) ([]KlineBar, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("K线响应数据为空")
	}

	retCount := int(binary.LittleEndian.Uint16(data[0:2]))
	pos := 2

	bars := make([]KlineBar, 0, retCount)
	preDiffBase := 0

	for i := 0; i < retCount; i++ {
		if pos+4 > len(data) {
			break
		}
		year, month, day, hour, minute, np := getDatetime(category, data, pos)
		pos = np

		openDiff := getPriceAt(data, &pos)
		closeDiff := getPriceAt(data, &pos)
		highDiff := getPriceAt(data, &pos)
		lowDiff := getPriceAt(data, &pos)

		if pos+8 > len(data) {
			break
		}
		volRaw := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		dbVolRaw := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		vol := getVolume(volRaw)
		amount := getVolume(dbVolRaw)

		openP := float64(openDiff+preDiffBase) / 1000.0
		newOpenDiff := openDiff + preDiffBase
		closeP := float64(newOpenDiff+closeDiff) / 1000.0
		highP := float64(newOpenDiff+highDiff) / 1000.0
		lowP := float64(newOpenDiff+lowDiff) / 1000.0
		preDiffBase = newOpenDiff + closeDiff

		var dateStr string
		if category < 4 || category == 7 || category == 8 {
			dateStr = fmt.Sprintf("%04d%02d%02d%02d%02d", year, month, day, hour, minute)
		} else {
			dateStr = fmt.Sprintf("%04d%02d%02d", year, month, day)
		}

		bars = append(bars, KlineBar{
			Date:   dateStr,
			Open:   openP,
			Close:  closeP,
			High:   highP,
			Low:    lowP,
			Amount: amount,
			Volume: vol,
		})
	}

	return bars, nil
}

// ── 实时行情 ──────────────────────────────────────────────────────────────────

// GetQuotes 获取实时行情（五档）
//
//	market: 市场
//	codes:  股票代码列表（同市场）
func (c *Client) GetQuotes(market uint16, codes []string) ([]QuoteData, error) {
	if len(codes) == 0 {
		return nil, nil
	}

	// 对齐 pytdx GetSecurityQuotesCmd: struct.pack("<HIHHIIHH", ...) + 每支 struct.pack("<B6s", ...)
	// 注意格式是 H-I-H-H-I-I-H-H（中间两个 I 连写），共 22 字节头：
	//   0x10c, 0x02006320, pkgdatalen, pkgdatalen, 0x5053e, 0(I), 0(H), stock_len(H)
	// 早期写成 uint16(0) 导致第 6 个字段只有 2 字节，整包比正确值少 2 字节、服务器不回包。
	pkgdatalen := uint16(len(codes)*7 + 12)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint16(0x10c))
	binary.Write(buf, binary.LittleEndian, uint32(0x02006320))
	binary.Write(buf, binary.LittleEndian, pkgdatalen)
	binary.Write(buf, binary.LittleEndian, pkgdatalen)
	binary.Write(buf, binary.LittleEndian, uint32(0x5053e))
	binary.Write(buf, binary.LittleEndian, uint32(0))
	binary.Write(buf, binary.LittleEndian, uint16(0))
	binary.Write(buf, binary.LittleEndian, uint16(len(codes)))
	for _, code := range codes {
		codeBytes := make([]byte, 6)
		copy(codeBytes, []byte(code))
		buf.WriteByte(byte(market))
		buf.Write(codeBytes)
	}

	resp, err := c.sendRaw(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("获取行情失败: %w", err)
	}
	return parseQuotes(resp, market, codes)
}

// parseQuotes 解析实时行情响应（对齐 pytdx GetSecurityQuotesCmd.parseResponse）
func parseQuotes(data []byte, market uint16, codes []string) ([]QuoteData, error) {
	if len(data) < 4 {
		return nil, nil
	}
	// 跳过前 2 字节标记（b1 cb），随后 2 字节为股票数
	pos := 2
	if len(data) < pos+2 {
		return nil, nil
	}
	numStock := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
	pos += 2

	var quotes []QuoteData
	for i := 0; i < numStock; i++ {
		if pos+9 > len(data) {
			break
		}
		mkt := int(data[pos])
		codeRaw := string(data[pos+1 : pos+7])
		pos += 9 // <B6sH：market(1) + code(6) + active1(2)

		// 通达信五档行情：股票价格整数为「分」(/100)，基金/ETF 为「厘」(/1000)。
		// 统一按 /100 会把 ETF 价格放大 10 倍（如 159373: 1.184 → 11.84）。
		code := strings.TrimRight(codeRaw, "\x00")
		div := 100.0
		if isFundCode(code) {
			div = 1000.0
		}

		price := getPriceAt(data, &pos)
		lastCloseDiff := getPriceAt(data, &pos)
		openDiff := getPriceAt(data, &pos)
		highDiff := getPriceAt(data, &pos)
		lowDiff := getPriceAt(data, &pos)
		getPriceAt(data, &pos) // reversed_bytes0 (servertime)
		getPriceAt(data, &pos) // reversed_bytes1
		vol := getPriceAt(data, &pos)
		getPriceAt(data, &pos) // cur_vol
		if pos+4 > len(data) {
			break
		}
		amountRaw := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		amount := getVolume(amountRaw)
		getPriceAt(data, &pos) // s_vol
		getPriceAt(data, &pos) // b_vol
		getPriceAt(data, &pos) // reversed_bytes2
		getPriceAt(data, &pos) // reversed_bytes3

		bid1 := getPriceAt(data, &pos)
		ask1 := getPriceAt(data, &pos)
		bidVol1 := getPriceAt(data, &pos)
		askVol1 := getPriceAt(data, &pos)
		bid2 := getPriceAt(data, &pos)
		ask2 := getPriceAt(data, &pos)
		bidVol2 := getPriceAt(data, &pos)
		askVol2 := getPriceAt(data, &pos)
		bid3 := getPriceAt(data, &pos)
		ask3 := getPriceAt(data, &pos)
		bidVol3 := getPriceAt(data, &pos)
		askVol3 := getPriceAt(data, &pos)
		bid4 := getPriceAt(data, &pos)
		ask4 := getPriceAt(data, &pos)
		bidVol4 := getPriceAt(data, &pos)
		askVol4 := getPriceAt(data, &pos)
		bid5 := getPriceAt(data, &pos)
		ask5 := getPriceAt(data, &pos)
		bidVol5 := getPriceAt(data, &pos)
		askVol5 := getPriceAt(data, &pos)

		if pos+2 > len(data) {
			break
		}
		pos += 2 // reversed_bytes4 (uint16)
		getPriceAt(data, &pos)
		getPriceAt(data, &pos)
		getPriceAt(data, &pos)
		getPriceAt(data, &pos)
		if pos+4 > len(data) {
			break
		}
		pos += 4 // reversed_bytes9 (int16) + active2 (uint16)

		q := QuoteData{
			Market:    mkt,
			Code:      code,
			Price:     float64(price) / div,
			LastClose: float64(price+lastCloseDiff) / div,
			Open:      float64(price+openDiff) / div,
			High:      float64(price+highDiff) / div,
			Low:       float64(price+lowDiff) / div,
			Volume:    float64(vol),
			Amount:    amount,
			BuyPrices: [5]float64{
				float64(price+bid1) / div,
				float64(price+bid2) / div,
				float64(price+bid3) / div,
				float64(price+bid4) / div,
				float64(price+bid5) / div,
			},
			BuyVolumes: [5]float64{float64(bidVol1), float64(bidVol2), float64(bidVol3), float64(bidVol4), float64(bidVol5)},
			SellPrices: [5]float64{
				float64(price+ask1) / div,
				float64(price+ask2) / div,
				float64(price+ask3) / div,
				float64(price+ask4) / div,
				float64(price+ask5) / div,
			},
			SellVolumes: [5]float64{float64(askVol1), float64(askVol2), float64(askVol3), float64(askVol4), float64(askVol5)},
		}
		quotes = append(quotes, q)
	}

	return quotes, nil
}

// ── 工具函数 ──────────────────────────────────────────────────────────────────

// isFundCode 判断是否为基金类证券（ETF / LOF）。
// 通达信五档行情对基金使用 3 位小数（厘），与股票的 2 位小数（分）不同；
// 实测覆盖：深市 15/16、沪市 50/51/52/56/58。可转债 11/12/13 不在其列。
func isFundCode(code string) bool {
	if len(code) < 2 {
		return false
	}
	switch code[:2] {
	case "15", "16", "50", "51", "52", "56", "58":
		return true
	}
	return false
}

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
