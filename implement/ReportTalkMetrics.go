package implement

type ImsReportTalkMetrics struct {
	Rid          string  `json:"rid"`          //
	SspSlotId    int64   `json:"sspSlotId"`    // 媒体广告位Id
	DspSlotId    int64   `json:"dspSlotId"`    // 预算位广告位id
	DspSlotCode  string  `json:"dspSlotCode"`  // 预算广告位
	Ip           string  `json:"ip"`           // ip
	Ua           string  `json:"ua"`           // Ua
	AuctionPrice float64 `json:"auctionPrice"` // 成交价
	DspPayType   int     `json:"dspPayType"`   // 上游结算方式 1=分成，2=RTB
	BidPrice     float64 `json:"bidPrice"`     // 下游出价
	SspPayType   int     `json:"sspPayType"`   // 1=分成，2=RTB
	Caid         string  `json:"caid"`
	Idfa         string  `json:"idfa"`
	Oaid         string  `json:"oaid"`
	AndroidId    string  `json:"androidId"`
	Imei         string  `json:"imei"`
	Os           int     `json:"os"`
	AuctionTs    int64   `json:"auctionTs"` // 成交时间

	WinPrice string `json:"winPrice"` // 下游成交价通过宏替换上报

	UpX   string `json:"up_x"`
	UpY   string `json:"up_y"`
	DownX string `json:"down_x"`
	DownY string `json:"down_y"`
	Ts    int64  `json:"ts"`  //时间戳(毫秒)
	Tts   int64  `json:"tts"` // 时间戳(秒)

	AdvertisersId string `json:"advertisersId"`
	CreativeId    string `json:"creativeId"`
	Source        string `json:"source"`
	Pkg           string `json:"pkg"`
	EventTs       int64  `json:"eventTs"`

	ReportPrice float64 `json:"reportPrice"` //成交价：  RTB 的情况下媒体上报得到价格转换成int 类型 宏替换中解析的上报价格
	Profit      float64 `json:"profit"`      // 利润 ： 成交价 - 上报价格
}

type ClkReportTalkMetrics struct {
	Rid          string  `json:"rid"`
	SspSlotId    int64   `json:"sspSlotId"`    // 媒体广告位Id
	DspSlotId    int64   `json:"dspSlotId"`    // 预算位广告位id
	DspSlotCode  string  `json:"dspSlotCode"`  // 预算广告位
	Ip           string  `json:"ip"`           // ip
	AuctionPrice float64 `json:"auctionPrice"` // 成交价
	DspPayType   string  `json:"dspPayType"`   // 上游结算方式 1=分成，2=RTB
	Caid         string  `json:"caid"`
	Idfa         string  `json:"idfa"`
	Oaid         string  `json:"oaid"`
	AndroidId    string  `json:"androidId"`
	Imei         string  `json:"imei"`
	Os           int     `json:"os"`
	AuctionTs    int64   `json:"auctionTs"` // 成交时间

	Width    string `json:"width"`
	Height   string `json:"height"`
	WinPrice string `json:"winPrice"` // 下游成交价通过宏替换上报
	UpX      string `json:"up_x"`
	UpY      string `json:"up_y"`
	DownX    string `json:"down_x"`
	DownY    string `json:"down_y"`
	Ts       int64  `json:"ts"` //时间戳(毫秒)

	Pkg           string `json:"pkg"`
	Source        string `json:"source"`
	EventTs       int64  `json:"eventTs"`
	CreativeId    string `json:"creativeId"`
	AdvertisersId string `json:"advertisersId"`
}

type DspRequestMetrics struct {
	RequestId   string `json:"RequestId"`   // 唯一ID，用于链路追踪
	SspSlotId   int64  `json:"SspSlotId"`   // 媒体广告位ID
	DspSlotId   int64  `json:"DspSlotId"`   // 预算位广告位ID（可能为空，0表示空）
	DspSlotCode string `json:"DspSlotCode"` // 预算广告位编码（可能为空）

	// 统计字段
	SspReq        int   `json:"SspReq"`        // SSP请求次数（req_pv）
	SspRes        int   `json:"SspRes"`        // SSP返回次数（ret_pv）
	DspReq        int   `json:"DspReq"`        // DSP请求次数
	DspRes        int   `json:"DspRes"`        // DSP返回次数
	SspReqDiscard int   `json:"SspReqDiscard"` // SSP请求丢弃次数
	SspResDiscard int   `json:"SspResDiscard"` // SSP返回丢弃次数
	DspReqDiscard int   `json:"DspReqDiscard"` // DSP请求丢弃次数
	DspResDiscard int   `json:"DspResDiscard"` // DSP返回丢弃次数
	EventTs       int64 `json:"EventTs"`       // 事件时间戳：YYYYMMDDHHmm
}
