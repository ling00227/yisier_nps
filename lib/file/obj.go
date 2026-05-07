package file

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ehang.io/nps/lib/rate"
	"github.com/pkg/errors"
)

type Flow struct {
	ExportFlow int64
	InletFlow  int64
	FlowLimit  int64
	sync.RWMutex
}

func (s *Flow) Add(in, out int64) {
	s.Lock()
	defer s.Unlock()
	s.InletFlow += int64(in)
	s.ExportFlow += int64(out)
}

type Config struct {
	U        string
	P        string
	Compress bool
	Crypt    bool
}

type Client struct {
	Cnf             *Config
	Id              int        //id
	VerifyKey       string     //verify key
	Addr            string     //the ip of client
	Remark          string     //remark
	Status          bool       //is allow connect
	IsConnect       bool       //is the client connect
	RateLimit       int        //rate /kb
	Flow            *Flow      //flow setting
	Rate            *rate.Rate //rate limit
	NoStore         bool       //no store to file
	NoDisplay       bool       //no display on web
	MaxConn         int        //the max connection num of client allow
	NowConn         int32      //the connection num of now
	WebUserName     string     //the username of web login
	WebPassword     string     //the password of web login
	ConfigConnAllow bool       //is allow connected by config file
	MaxTunnelNum    int
	Version         string
	BlackIpList     []string
	CreateTime      string
	LastOnlineTime  string
	IpWhite         bool     // 是否启用ip白名单
	IpWhitePass     string   // ip授权密码
	IpWhiteList     []string // ip白名单
	sync.RWMutex
}

func NewClient(vKey string, noStore bool, noDisplay bool) *Client {
	return &Client{
		Cnf:       new(Config),
		Id:        0,
		VerifyKey: vKey,
		Addr:      "",
		Remark:    "",
		Status:    true,
		IsConnect: false,
		RateLimit: 0,
		Flow:      new(Flow),
		Rate:      nil,
		NoStore:   noStore,
		RWMutex:   sync.RWMutex{},
		NoDisplay: noDisplay,
	}
}

func (s *Client) CutConn() {
	atomic.AddInt32(&s.NowConn, 1)
}

func (s *Client) AddConn() {
	atomic.AddInt32(&s.NowConn, -1)
}

func (s *Client) GetConn() bool {
	if s.MaxConn == 0 || int(s.NowConn) < s.MaxConn {
		s.CutConn()
		return true
	}
	return false
}

// Tunnel 连接管理方法
func (s *Tunnel) CutConn() {
	atomic.AddInt32(&s.NowConn, 1)
}

func (s *Tunnel) AddConn() {
	atomic.AddInt32(&s.NowConn, -1)
}

func (s *Tunnel) GetConn() bool {
	if s.MaxConn == 0 || int(s.NowConn) < s.MaxConn {
		s.CutConn()
		return true
	}
	return false
}

// parseTimeRange 解析时间范围字符串
// 返回：开始星期(0表示每天), 开始小时, 开始分钟, 结束星期(0表示每天), 结束小时, 结束分钟, 跨天标志, 错误信息
func parseTimeRange(line string) (int, int, int, int, int, int, bool, string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return 0, 0, 0, 0, 0, 0, false, ""
	}

	var startWeekday, endWeekday int = 0, 0
	var startHour, startMin, endHour, endMin int
	var crossDay bool

	// 检查是否以星期开头，如 "1 08:00-12:30" 或 "1-5 08:00-12:30"
	weekdayPattern := regexp.MustCompile(`^((\d+)(?:-(\d+))?)\s+(.+)$`)
	weekdayMatch := weekdayPattern.FindStringSubmatch(line)

	if weekdayMatch != nil {
		// 有星期前缀
		startWeekday, _ = strconv.Atoi(weekdayMatch[2])
		if weekdayMatch[3] != "" {
			endWeekday, _ = strconv.Atoi(weekdayMatch[3])
		} else {
			endWeekday = startWeekday
		}
		line = weekdayMatch[4]
	}

	// 解析时间部分
	timePattern := regexp.MustCompile(`^(\d{1,2}):(\d{2})\s*-\s*((\d+)\s+)?(\d{1,2}):(\d{2})$`)
	timeMatch := timePattern.FindStringSubmatch(line)

	if timeMatch == nil {
		return 0, 0, 0, 0, 0, 0, false, "时间格式错误"
	}

	startHour, _ = strconv.Atoi(timeMatch[1])
	startMin, _ = strconv.Atoi(timeMatch[2])

	// timeMatch[3] 是最后一个时间前面的空格（如果有），timeMatch[4] 是结束星期，timeMatch[5] 是结束小时，timeMatch[6] 是结束分钟
	if timeMatch[4] != "" {
		endWeekday, _ = strconv.Atoi(timeMatch[4])
		crossDay = true
	}
	endHour, _ = strconv.Atoi(timeMatch[5])
	endMin, _ = strconv.Atoi(timeMatch[6])

	// 如果没有指定结束星期且跨天（结束时间小于开始时间），说明跨天
	startMinutes := startHour*60 + startMin
	endMinutes := endHour*60 + endMin
	if startWeekday > 0 && endWeekday == 0 && startMinutes > endMinutes {
		endWeekday = startWeekday + 1
		crossDay = true
	}
	if endWeekday == 0 {
		endWeekday = startWeekday
	}

	return startWeekday, startHour, startMin, endWeekday, endHour, endMin, crossDay, ""
}

// ValidateAllowTime 校验允许连接时间格式是否正确
// 格式支持：
//   - HH:MM-HH:MM（如 08:00-12:30，每天有效）
//   - 1 HH:MM-HH:MM（如 1 08:00-12:30，周一有效）
//   - 1-5 HH:MM-HH:MM（如 1-5 08:00-12:30，周一到周五有效）
//   - 1 HH:MM-2 HH:MM（如 1 08:00-2 18:00，周一08:00到周二18:00）
// 返回错误信息，如果格式正确返回空字符串
func ValidateAllowTime(allowTime string) string {
	if allowTime == "" {
		return ""
	}

	lines := strings.Split(allowTime, "\n")
	for lineNum, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		startWeekday, startHour, startMin, endWeekday, endHour, endMin, _, errMsg := parseTimeRange(line)
		if errMsg != "" {
			return fmt.Sprintf("第 %d 行格式错误，支持格式：08:00-12:30、1 08:00-12:30、1-5 08:00-12:30、1 08:00-2 18:00", lineNum+1)
		}

		// 校验星期范围
		if startWeekday < 0 || startWeekday > 7 {
			return fmt.Sprintf("第 %d 行开始星期应在 1-7 之间", lineNum+1)
		}
		if endWeekday < 0 || endWeekday > 7 {
			return fmt.Sprintf("第 %d 行结束星期应在 1-7 之间", lineNum+1)
		}

		// 校验时间范围
		if startHour < 0 || startHour > 23 {
			return fmt.Sprintf("第 %d 行开始时间小时应在 0-23 之间", lineNum+1)
		}
		if startMin < 0 || startMin > 59 {
			return fmt.Sprintf("第 %d 行开始时间分钟应在 0-59 之间", lineNum+1)
		}
		if endHour < 0 || endHour > 23 {
			return fmt.Sprintf("第 %d 行结束时间小时应在 0-23 之间", lineNum+1)
		}
		if endMin < 0 || endMin > 59 {
			return fmt.Sprintf("第 %d 行结束时间分钟应在 0-59 之间", lineNum+1)
		}

		// 校验星期范围
		if startWeekday > 0 && endWeekday > 0 && startWeekday > endWeekday && !strings.Contains(line, fmt.Sprintf("%d %d", startWeekday, endWeekday)) {
			// 只有明确指定了结束星期的情况下才检查
			timePattern := regexp.MustCompile(`^(\d+)\s+\d+:\d+\s+-\s+(\d+)\s+\d+:\d+$`)
			if timePattern.MatchString(line) {
				return fmt.Sprintf("第 %d 行开始星期不能大于结束星期", lineNum+1)
			}
		}
	}

	return ""
}

// IsAllowTime 检查当前时间是否在允许的时间范围内
// 格式支持：
//   - HH:MM-HH:MM（如 08:00-12:30，每天有效）
//   - 1 HH:MM-HH:MM（如 1 08:00-12:30，周一有效）
//   - 1-5 HH:MM-HH:MM（如 1-5 08:00-12:30，周一到周五有效）
//   - 1 HH:MM-2 HH:MM（如 1 08:00-2 18:00，周一08:00到周二18:00）
// 空字符串表示不限制，任何时间都允许
func (s *Tunnel) IsAllowTime() bool {
	if s.AllowTime == "" {
		return true
	}

	now := time.Now()
	// 将周日的0转为7，便于计算
	currentWeekday := int(now.Weekday())
	if currentWeekday == 0 {
		currentWeekday = 7
	}
	currentMinutes := now.Hour()*60 + now.Minute()

	lines := strings.Split(s.AllowTime, "\n")
	for _, line := range lines {
		startWeekday, startHour, startMin, endWeekday, endHour, endMin, crossDay, _ := parseTimeRange(line)

		if startWeekday == 0 {
			// 没有星期限制（每天有效）
			startWeekday = 1
			endWeekday = 7
		}
		if endWeekday == 0 {
			endWeekday = startWeekday
		}

		// 检查当前星期是否在范围内
		if currentWeekday < startWeekday || currentWeekday > endWeekday {
			continue
		}

		if crossDay {
			// 跨天情况：需要特殊处理
			if currentWeekday == startWeekday {
				// 如果是开始那天，检查时间是否 >= 开始时间
				if currentMinutes >= startHour*60+startMin {
					return true
				}
			} else if currentWeekday == endWeekday {
				// 如果是结束那天，检查时间是否 <= 结束时间
				if currentMinutes <= endHour*60+endMin {
					return true
				}
			} else {
				// 在中间的天数，全天有效
				return true
			}
		} else {
			// 普通情况：同一天
			startTotal := startHour*60 + startMin
			endTotal := endHour*60 + endMin
			if startWeekday == endWeekday {
				// 同一天的情况
				if currentWeekday == startWeekday && currentMinutes >= startTotal && currentMinutes <= endTotal {
					return true
				}
			} else {
				// 多天范围（不含跨天），检查时间
				if currentMinutes >= startTotal && currentMinutes <= endTotal {
					return true
				}
			}
		}
	}

	return false
}

func (s *Client) HasTunnel(t *Tunnel) (exist bool) {
	GetDb().JsonDb.Tasks.Range(func(key, value interface{}) bool {
		v := value.(*Tunnel)
		if v.Client.Id == s.Id && v.Port == t.Port && t.Port != 0 {
			exist = true
			return false
		}
		return true
	})
	return
}

func (s *Client) GetTunnelNum() (num int) {
	GetDb().JsonDb.Tasks.Range(func(key, value interface{}) bool {
		v := value.(*Tunnel)
		if v.Client.Id == s.Id {
			num++
		}
		return true
	})

	GetDb().JsonDb.Hosts.Range(func(key, value interface{}) bool {
		v := value.(*Host)
		if v.Client.Id == s.Id {
			num++
		}
		return true
	})
	return
}

func (s *Client) HasHost(h *Host) bool {
	var has bool
	GetDb().JsonDb.Hosts.Range(func(key, value interface{}) bool {
		v := value.(*Host)
		if v.Client.Id == s.Id && v.Host == h.Host && h.Location == v.Location {
			has = true
			return false
		}
		return true
	})
	return has
}

type Tunnel struct {
	Id           int
	Port         int
	ServerIp     string
	Mode         string
	Status       bool
	RunStatus    bool
	Client       *Client
	Ports        string
	Flow         *Flow
	Password     string
	Remark       string
	TargetAddr   string
	NoStore      bool
	LocalPath    string
	StripPre     string
	ProtoVersion string
	Target       *Target
	MultiAccount *MultiAccount
	NowConn      int32      //当前连接数
	MaxConn      int        //最大连接数，0表示不限制
	AllowTime    string     //允许连接时间，多行时间段格式：08:00-12:30\n18:00-20:00
	Health
	sync.RWMutex
}

type Health struct {
	HealthCheckTimeout  int
	HealthMaxFail       int
	HealthCheckInterval int
	HealthNextTime      time.Time
	HealthMap           map[string]int
	HttpHealthUrl       string
	HealthRemoveArr     []string
	HealthCheckType     string
	HealthCheckTarget   string
	sync.RWMutex
}

type Host struct {
	Id           int
	Host         string //host
	HeaderChange string //header change
	HostChange   string //host change
	Location     string //url router
	Remark       string //remark
	Scheme       string //http https all
	CertFilePath string
	KeyFilePath  string
	NoStore      bool
	IsClose      bool
	AutoHttps    bool // 自动https
	Flow         *Flow
	Client       *Client
	Target       *Target //目标
	Health       `json:"-"`
	sync.RWMutex
}

type Target struct {
	nowIndex   int
	TargetStr  string
	TargetArr  []string
	LocalProxy bool
	sync.RWMutex
}

type MultiAccount struct {
	AccountMap map[string]string // multi account and pwd
}

func (s *Target) GetRandomTarget() (string, error) {
	if s.TargetArr == nil {
		s.TargetArr = strings.Split(s.TargetStr, "\n")
	}
	if len(s.TargetArr) == 1 {
		return s.TargetArr[0], nil
	}
	if len(s.TargetArr) == 0 {
		return "", errors.New("all inward-bending targets are offline")
	}
	s.Lock()
	defer s.Unlock()
	if s.nowIndex >= len(s.TargetArr)-1 {
		s.nowIndex = -1
	}
	s.nowIndex++
	return s.TargetArr[s.nowIndex], nil
}

type Glob struct {
	BlackIpList []string
	ServerUrl   string
	sync.RWMutex
}
