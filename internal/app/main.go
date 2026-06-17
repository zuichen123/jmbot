package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	"math/big"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
)

type Config struct {
	AdminID              int64  `yaml:"admin_id"`
	HTTPHost             string `yaml:"http_host"`
	HTTPPort             int    `yaml:"http_port"`
	WebsocketURL         string `yaml:"websocket_url"`
	WebsocketToken       string `yaml:"websocket_token"`

	FileDir           string `yaml:"file_dir"`
	MangaDir          string `yaml:"manga_dir"`
	CBZDir            string `yaml:"cbz_dir"`
	CBZChapterEnabled bool   `yaml:"cbz_chapter_enabled"`
	CBZSeriesEnabled  bool   `yaml:"cbz_series_enabled"`
	LogDir            string `yaml:"log_dir"`
	TmpDir            string `yaml:"tmp_dir"`
	JMOptionPath      string `yaml:"jm_option_path"`
	JMProxy           string `yaml:"jm_proxy"` // JM下载代理
	TransferMode      string `yaml:"transfer_mode"`
	RemoteUser        string `yaml:"remote_user"`
	RemoteHost        string `yaml:"remote_host"`
	RemoteTempDir     string `yaml:"remote_temp_dir"`
	LocalSSHKey       string `yaml:"local_ssh_key"`
	DockerPath        string `yaml:"docker_internal_path"`

	DownloadTimeout      int     `yaml:"download_timeout"`
	SearchTimeout        int     `yaml:"search_timeout"`
	MaxEpisodes          int     `yaml:"max_episodes"`
	DedupWindow          int     `yaml:"dedup_window_seconds"`
	RandomPasswordLength int     `yaml:"random_password_length"`
	SoutuTriggerWindow   int     `yaml:"soutu_trigger_window_seconds"`
	SoutuGlobalM         int64   `yaml:"soutu_global_m"`
	SoutuFactor          float64 `yaml:"soutu_factor"`
	SoutuURL             string  `yaml:"soutu_url"`
	SoutuAPI             string  `yaml:"soutu_api"`
	SoutuUserAgent       string  `yaml:"soutu_user_agent"`
	CFBypassAPIURL       string  `yaml:"cf_bypass_api_url"`
	CFBypassPollInterval float64 `yaml:"cf_bypass_poll_interval_sec"`
	CFBypassPollTimeout  float64 `yaml:"cf_bypass_poll_timeout_sec"`
	EmbeddedBypassEnable bool    `yaml:"embedded_bypass_enabled"`
	EmbeddedBypassHost   string  `yaml:"embedded_bypass_host"`
	EmbeddedBypassPort   int     `yaml:"embedded_bypass_port"`
	HTTPPortFallback     bool    `yaml:"http_port_fallback"`
	PortFallbackTries    int     `yaml:"port_fallback_tries"`

	SendModeGlobal     string            `yaml:"send_mode_global"`
	SendModeGroup      map[string]string `yaml:"send_mode_group"`
	SendNameModeGlobal string            `yaml:"send_name_mode_global"`
	SendNameModeGroup  map[string]string `yaml:"send_name_mode_group"`

	EncEnabledGlobal  bool              `yaml:"enc_enabled_global"`
	EncEnabledGroup   map[string]bool   `yaml:"enc_enabled_group"`
	EncPasswordGlobal string            `yaml:"enc_password_global"`
	EncPasswordGroup  map[string]string `yaml:"enc_password_group"`

	RandomPasswordEnabledGlobal bool            `yaml:"random_password_enabled_global"`
	RandomPasswordEnabledGroup  map[string]bool `yaml:"random_password_enabled_group"`
	RegexEnabledGlobal          bool            `yaml:"regex_enabled_global"`
	RegexEnabledGroup           map[string]bool `yaml:"regex_enabled_group"`
	StrictModeGlobal            bool            `yaml:"strict_mode_global"`
	StrictModeGroup             map[string]bool `yaml:"strict_mode_group"`

	BannedID    []string `yaml:"banned_id"`
	BannedUser  []string `yaml:"banned_user"`
	BannedGroup []string `yaml:"banned_group"`
	AllowedGroup []int64 `yaml:"allowed_group"` // 白名单群聊，为空时所有群生效

	ReplyAsCard  bool   `yaml:"reply_as_card"`
	CardNickname string `yaml:"card_nickname"`
	CardUserID   int64  `yaml:"card_user_id"`

	LocalTestMode              bool `yaml:"local_test_mode"`
	LocalTestExitAfterSelftest bool `yaml:"local_test_exit_after_selftest"`

	BikaEnabled bool   `yaml:"bika_enabled"`
	BikaBaseURL string `yaml:"bika_base_url"`
	BikaToken   string `yaml:"bika_token"`
	BikaQuality string `yaml:"bika_quality"`
	BikaProxy   string `yaml:"bika_proxy"`
	BikaEmail   string `yaml:"bika_email"`    // 登录邮箱（用于自动重登）
	BikaPasswd  string `yaml:"bika_passwd"`   // 登录密码（用于自动重登）
	BikaAutoRenew bool `yaml:"bika_auto_renew"` // token过期自动重登

	DailyRecommendEnabled bool    `yaml:"daily_recommend_enabled"`
	DailyRecommendHour    int     `yaml:"daily_recommend_hour"`
	DailyRecommendMinute  int     `yaml:"daily_recommend_minute"`
	DailyRecommendGroups  []int64 `yaml:"daily_recommend_groups"`

	MaxConcurrentDownloads int `yaml:"max_concurrent_downloads"`

	AIImageEnabled   bool   `yaml:"ai_image_enabled"`
	AIImageBaseURL   string `yaml:"ai_image_base_url"`
	AIImageAPIKey    string `yaml:"ai_image_api_key"`
	AIImageModel     string `yaml:"ai_image_model"`
	AIImageSize      string `yaml:"ai_image_size"`
	AIImageTimeout   int    `yaml:"ai_image_timeout_seconds"`
	AIImageMaxRetries  int    `yaml:"ai_image_max_retries"`
	AIImageWaitingImage string `yaml:"ai_image_waiting_image"`
}

type App struct {
	cfgPath string
	cfgMu   sync.RWMutex
	cfg     *Config

	bot *NapcatClient
	jm  *JMBridge

	bika *BikaClient

	queue chan DownloadTask

	recentMu sync.Mutex
	recent   map[string]map[string]time.Time

	searchMu sync.Mutex
	search   map[string]PendingSearch

	jmEnabledMu sync.RWMutex
	jmEnabled   bool

	soutuMu    sync.Mutex
	soutuArmed map[string]time.Time

	bulkMu     sync.Mutex
	bulkStates map[string]*bulkBatchState
}

type PendingSearch struct {
	AlbumID    string
	Title      string
	At         time.Time
	AggResults []SearchResultItem
	MessageID  string // 每日推荐的消息ID，用于判断是否回复了该消息
}

type SearchResultItem struct {
	Source string // "JM" 或 "Bika"
	ID     string
	Title  string
	Author string
	Tags   []string
}

type DownloadTask struct {
	Number      string
	MessageType string
	GroupID     int64
	UserID      int64
	Scope       string
	Uploader    string
	Bulk        bool
	BatchID     string
	BatchTotal  int
	BatchIndex  int
}

type bulkTaskResult struct {
	BatchIndex int
	Number     string
	Message    string
	CoverPath  string // 封面图片路径
	FilePath   string
	OrigPDF    string // 原始PDF路径，用于发送后删除
	Cleanup    []string
	FailMsg    string
}

type bulkBatchState struct {
	MessageType string
	GroupID     int64
	UserID      int64
	Total       int
	Results     []bulkTaskResult
}

type Album struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	Episodes    int      `json:"episodes"`
	Views       string   `json:"views"`
}

type bypassResponse struct {
	Message   string         `json:"message"`
	UserAgent string         `json:"user_agent"`
	Cookies   []bypassCookie `json:"cookies"`
}

type bypassCookie struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type bypassQuery struct {
	URL         string `json:"url"`
	UserAgent   string `json:"user_agent"`
	ProxyServer string `json:"proxy_server"`
}

type embeddedBypassService struct {
	mu      sync.Mutex
	cache   map[string]embeddedCacheItem
	running map[string]bool
}

type embeddedCacheItem struct {
	Data      bypassResponse
	ExpiresAt time.Time
}

var (
	soutuCFMu            sync.RWMutex
	soutuCFCookies       = map[string]string{}
	soutuCFCookieExpires time.Time
)

func Main() {
	var installService bool
	var uninstallService bool
	var serviceName string
	var serviceUser string
	var serviceGroup string
	flag.BoolVar(&installService, "install", false, "install and enable systemd service")
	flag.BoolVar(&uninstallService, "uninstall", false, "disable and remove systemd service")
	flag.StringVar(&serviceName, "service-name", "napcat-jm-go", "systemd service name")
	flag.StringVar(&serviceUser, "service-user", "", "systemd service user, default current login user")
	flag.StringVar(&serviceGroup, "service-group", "", "systemd service group, default primary group of service user")
	flag.Parse()

	if installService && uninstallService {
		log.Fatal("cannot use --install and --uninstall together")
	}
	if installService {
		if err := installSystemdService(serviceName, serviceUser, serviceGroup); err != nil {
			log.Fatalf("install service failed: %v", err)
		}
		log.Printf("service installed and started: %s", serviceName)
		return
	}
	if uninstallService {
		if err := uninstallSystemdService(serviceName); err != nil {
			log.Fatalf("uninstall service failed: %v", err)
		}
		log.Printf("service removed: %s", serviceName)
		return
	}

	app, err := NewApp("config.yml", "configs/config.example.yml")
	if err != nil {
		log.Fatalf("init failed: %v", err)
	}

	if app.cfg.LocalTestMode {
		log.Printf("local test mode enabled")
		if err := app.runLocalSelfTest(); err != nil {
			log.Printf("local selftest failed: %v", err)
		} else {
			log.Printf("local selftest succeeded")
		}
		if app.cfg.LocalTestExitAfterSelftest {
			return
		}
	}

	// 启动多个worker支持并发下载
	cfg := app.currentConfig()
	workerCount := cfg.MaxConcurrentDownloads
	if workerCount <= 0 {
		workerCount = 3
	}
	for i := 0; i < workerCount; i++ {
		go app.worker()
	}

	mainMux := http.NewServeMux()
	mainMux.HandleFunc("/", app.handleHTTPEvent)
	mainServer := &http.Server{Handler: mainMux}

	mainTries := 1
	if cfg.HTTPPortFallback {
		mainTries = cfg.PortFallbackTries
	}
	mainListener, mainPort, err := listenWithFallback(cfg.HTTPHost, cfg.HTTPPort, mainTries)
	if err != nil {
		log.Fatalf("listen main http failed: %v", err)
	}
	if mainPort != cfg.HTTPPort {
		log.Printf("main port %d unavailable, fallback to %d", cfg.HTTPPort, mainPort)
	}

	var bypassServer *http.Server
	if cfg.EmbeddedBypassEnable {
		bypassSvc := newEmbeddedBypassService()
		bypassMux := http.NewServeMux()
		bypassMux.HandleFunc("/api/v1/bypass", bypassSvc.handleBypassV1)
		bypassMux.HandleFunc("/cloudflare5s/bypass-v1", bypassSvc.handleBypassV1)
		bypassServer = &http.Server{Handler: bypassMux}

		bypassListener, bypassPort, listenErr := listenWithFallback(cfg.EmbeddedBypassHost, cfg.EmbeddedBypassPort, cfg.PortFallbackTries)
		if listenErr != nil {
			log.Fatalf("listen embedded bypass failed: %v", listenErr)
		}
		if bypassPort != cfg.EmbeddedBypassPort {
			log.Printf("bypass port %d unavailable, fallback to %d", cfg.EmbeddedBypassPort, bypassPort)
		}
		if shouldUseEmbeddedBypassURL(cfg.CFBypassAPIURL) {
			u := fmt.Sprintf("http://%s:%d/api/v1/bypass", clientHost(cfg.EmbeddedBypassHost), bypassPort)
			app.cfgMu.Lock()
			app.cfg.CFBypassAPIURL = u
			app.cfgMu.Unlock()
			log.Printf("using embedded bypass api: %s", u)
		}
		go serveHTTP("embedded bypass", bypassServer, bypassListener)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("go bot listening at %s", mainListener.Addr().String())
		if serveErr := mainServer.Serve(mainListener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("received shutdown signal")
	case serveErr := <-errCh:
		log.Printf("server error: %v", serveErr)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := mainServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("main server shutdown error: %v", err)
	}
	if bypassServer != nil {
		if err := bypassServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("bypass server shutdown error: %v", err)
		}
	}
	log.Printf("shutdown complete")
}

func NewApp(configPath, configExamplePath string) (*App, error) {
	// 初始化日志系统
	initLogger()

	if _, err := os.Stat(configPath); errors.Is(err, os.ErrNotExist) {
		if writeErr := writeMinimalConfigTemplate(configPath); writeErr != nil {
			// Fallback: if minimal template creation fails, try copying example.
			raw, readErr := os.ReadFile(configExamplePath)
			if readErr != nil {
				return nil, fmt.Errorf("missing config.yml and failed to create minimal template: %w", writeErr)
			}
			if writeErr2 := os.WriteFile(configPath, raw, 0o644); writeErr2 != nil {
				return nil, fmt.Errorf("write config.yml failed: %w", writeErr2)
			}
		}
		return nil, fmt.Errorf("首次启动：已生成最小配置文件 `%s`，请先填写 admin_id / websocket_url / websocket_token 后重新启动", configPath)
	}

	raw, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, err
	}
	fillDefaults(cfg)

	app := &App{
		cfgPath:    configPath,
		cfg:        cfg,
		bot:        NewNapcatClient(cfg.WebsocketURL, cfg.WebsocketToken, cfg.LocalTestMode),
		jm:         NewJMBridge(cfg.JMOptionPath, cfg.FileDir, cfg.MangaDir, cfg.CBZDir, cfg.TmpDir, cfg.DownloadTimeout, cfg.LocalTestMode, cfg.JMProxy),
		queue:      make(chan DownloadTask, 1024),
		recent:     map[string]map[string]time.Time{},
		search:     map[string]PendingSearch{},
		jmEnabled:  true,
		soutuArmed: map[string]time.Time{},
		bulkStates: map[string]*bulkBatchState{},
	}
	if cfg.BikaEnabled {
		app.bika = NewBikaClient(getBikaConfig(cfg))
	}
	app.jm.SetCBZOptions(cfg.CBZChapterEnabled, cfg.CBZSeriesEnabled)
	app.startDailyRecommend()
	return app, nil
}

func writeMinimalConfigTemplate(configPath string) error {
	const tpl = `# 首次启动自动生成的最小配置，请至少填写以下三项：
# 1) admin_id（可留空为0，随后首个私聊发送 /jm admin 自动认领）
# 2) websocket_url
# 3) websocket_token

admin_id: 0
http_host: "0.0.0.0"
http_port: 8071

# NapCat OneBot WebSocket 地址与鉴权
websocket_url: "ws://127.0.0.1:13001"
websocket_token: ""

# 基础目录
file_dir: "./pdf/"
manga_dir: "./manga/"
cbz_dir: "./cbz/"
log_dir: "./logs"

# jmcomic 选项文件
jm_option_path: "./configs/option.yml"

# 传输模式：建议容器内/同机先用 local，跨机用 scp
transfer_mode: "local"
remote_user: ""
remote_host: ""
remote_temp_dir: "/tmp/napcat-jm-go-${USER}/temp"
local_ssh_key: ""
docker_internal_path: "/app/.config/QQ/temp/"
`
	return os.WriteFile(configPath, []byte(tpl), 0o644)
}

func initLogger() {
	// 确保logs目录存在
	logDir := "./logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("创建日志目录失败: %v", err)
		return
	}

	// 生成日志文件名（按日期）
	logFile := filepath.Join(logDir, fmt.Sprintf("bot_%s.log", time.Now().Format("2006-01-02")))

	// 打开日志文件（追加模式）
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("打开日志文件失败: %v", err)
		return
	}

	// 同时输出到文件和stdout
	multiWriter := io.MultiWriter(os.Stdout, f)
	log.SetOutput(multiWriter)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Printf("日志系统初始化完成，日志文件: %s", logFile)
}

func fillDefaults(cfg *Config) {
	if cfg.HTTPHost == "" {
		cfg.HTTPHost = "0.0.0.0"
	}
	if cfg.HTTPPort == 0 {
		cfg.HTTPPort = 8071
	}
	if cfg.WebsocketURL == "" {
		cfg.WebsocketURL = "ws://127.0.0.1:13001"
	}
	if cfg.FileDir == "" {
		cfg.FileDir = "./pdf/"
	}
	if cfg.MangaDir == "" {
		cfg.MangaDir = "./manga/"
	}
	if cfg.CBZDir == "" {
		cfg.CBZDir = "./cbz/"
	}
	if cfg.LogDir == "" {
		cfg.LogDir = "./logs"
	}
	if strings.TrimSpace(cfg.JMOptionPath) == "" {
		if fileExists("./configs/option.yml") {
			cfg.JMOptionPath = "./configs/option.yml"
		} else {
			cfg.JMOptionPath = "./option.yml"
		}
	}
	if cfg.TransferMode == "" {
		cfg.TransferMode = "local"
	}
	if strings.TrimSpace(cfg.RemoteTempDir) == "" {
		cfg.RemoteTempDir = "/tmp/napcat-jm-go-${USER}/temp"
	}
	cfg.RemoteTempDir = expandUserVars(cfg.RemoteTempDir)
	if strings.TrimSpace(cfg.TmpDir) == "" {
		cfg.TmpDir = "./tmp"
	}
	if err := os.MkdirAll(cfg.TmpDir, 0o755); err != nil {
		log.Printf("创建临时目录失败: %v", err)
	}
	if cfg.DownloadTimeout == 0 {
		cfg.DownloadTimeout = 1800
	}
	if cfg.SearchTimeout == 0 {
		cfg.SearchTimeout = 600
	}
	if cfg.MaxEpisodes == 0 {
		cfg.MaxEpisodes = 20
	}
	if cfg.DedupWindow == 0 {
		cfg.DedupWindow = 12 * 60 * 60
	}
	if cfg.RandomPasswordLength <= 0 {
		cfg.RandomPasswordLength = 10
	}
	if cfg.SoutuTriggerWindow <= 0 {
		cfg.SoutuTriggerWindow = 120
	}
	if cfg.SoutuGlobalM <= 0 {
		cfg.SoutuGlobalM = 3331358690401
	}
	if cfg.SoutuFactor <= 0 {
		cfg.SoutuFactor = 1.2
	}
	if strings.TrimSpace(cfg.SoutuURL) == "" {
		cfg.SoutuURL = "https://soutubot.moe"
	}
	cfg.SoutuURL = strings.TrimRight(strings.TrimSpace(cfg.SoutuURL), "/")
	if strings.TrimSpace(cfg.SoutuAPI) == "" {
		cfg.SoutuAPI = cfg.SoutuURL + "/api/search"
	}
	if strings.TrimSpace(cfg.SoutuUserAgent) == "" {
		cfg.SoutuUserAgent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
	}
	if strings.TrimSpace(cfg.CFBypassAPIURL) == "" {
		cfg.CFBypassAPIURL = "http://127.0.0.1:8000/api/v1/bypass"
	}
	if cfg.CFBypassPollInterval <= 0 {
		cfg.CFBypassPollInterval = 2
	}
	if cfg.CFBypassPollTimeout <= 0 {
		cfg.CFBypassPollTimeout = 120
	}
	if cfg.EmbeddedBypassHost == "" {
		cfg.EmbeddedBypassHost = "127.0.0.1"
	}
	if cfg.EmbeddedBypassPort <= 0 {
		cfg.EmbeddedBypassPort = 18000
	}
	if cfg.PortFallbackTries <= 0 {
		cfg.PortFallbackTries = 20
	}
	if !cfg.EmbeddedBypassEnable && shouldUseEmbeddedBypassURL(cfg.CFBypassAPIURL) {
		cfg.EmbeddedBypassEnable = true
	}
	if cfg.SendModeGlobal == "" {
		cfg.SendModeGlobal = "pdf"
	}
	cfg.SendNameModeGlobal = normalizeSendNameMode(cfg.SendNameModeGlobal)
	if cfg.SendModeGroup == nil {
		cfg.SendModeGroup = map[string]string{}
	}
	if cfg.SendNameModeGroup == nil {
		cfg.SendNameModeGroup = map[string]string{}
	}
	for k, v := range cfg.SendNameModeGroup {
		cfg.SendNameModeGroup[k] = normalizeSendNameMode(v)
	}
	if cfg.EncEnabledGroup == nil {
		cfg.EncEnabledGroup = map[string]bool{}
	}
	if cfg.EncPasswordGroup == nil {
		cfg.EncPasswordGroup = map[string]string{}
	}
	if cfg.RandomPasswordEnabledGroup == nil {
		cfg.RandomPasswordEnabledGroup = map[string]bool{}
	}
	if cfg.RegexEnabledGroup == nil {
		cfg.RegexEnabledGroup = map[string]bool{}
	}
	if cfg.StrictModeGroup == nil {
		cfg.StrictModeGroup = map[string]bool{}
	}
	// 默认开启regex模式，避免未配置时频繁触发
	cfg.RegexEnabledGlobal = true
	if strings.TrimSpace(cfg.CardNickname) == "" {
		cfg.CardNickname = "文件助手"
	}
	if cfg.CardUserID == 0 {
		if cfg.AdminID > 0 {
			cfg.CardUserID = cfg.AdminID
		} else {
			cfg.CardUserID = 10000
		}
	}
	if cfg.AIImageAPIKey == "" {
		cfg.AIImageAPIKey = os.Getenv("AI_IMAGE_API_KEY")
	}
	if cfg.AIImageBaseURL == "" {
		cfg.AIImageBaseURL = "https://api.openai.com/v1"
	}
	if cfg.AIImageModel == "" {
		cfg.AIImageModel = "dall-e-3"
	}
	if cfg.AIImageSize == "" {
		cfg.AIImageSize = "1024x1024"
	}
	if cfg.AIImageTimeout <= 0 {
		cfg.AIImageTimeout = 300
	}
	if cfg.AIImageMaxRetries <= 0 {
		cfg.AIImageMaxRetries = 2
	}
	if cfg.AIImageWaitingImage == "" {
		cfg.AIImageWaitingImage = defaultWaitingImage
	}
}

func serveHTTP(name string, srv *http.Server, ln net.Listener) {
	log.Printf("%s listening at %s", name, ln.Addr().String())
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("%s serve error: %v", name, err)
	}
}

func listenWithFallback(host string, preferredPort, tries int) (net.Listener, int, error) {
	if tries <= 0 {
		tries = 1
	}
	lastErr := error(nil)
	for i := 0; i < tries; i++ {
		p := preferredPort + i
		addr := net.JoinHostPort(host, strconv.Itoa(p))
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, p, nil
		}
		lastErr = err
		if !isAddrInUseErr(err) {
			return nil, 0, err
		}
	}
	return nil, 0, fmt.Errorf("failed to bind %s:%d after %d tries: %w", host, preferredPort, tries, lastErr)
}

func isAddrInUseErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "address already in use")
}

func shouldUseEmbeddedBypassURL(current string) bool {
	v := strings.TrimSpace(current)
	if v == "" || strings.EqualFold(v, "auto") {
		return true
	}
	return v == "http://127.0.0.1:8000/api/v1/bypass"
}

func clientHost(host string) string {
	h := strings.TrimSpace(host)
	if h == "" || h == "0.0.0.0" || h == "::" {
		return "127.0.0.1"
	}
	return h
}

func pairedHostForDualStack(host string) string {
	h := strings.TrimSpace(host)
	switch h {
	case "::":
		return "0.0.0.0"
	case "0.0.0.0":
		return "::"
	default:
		return ""
	}
}

func installSystemdService(serviceName, serviceUser, serviceGroup string) error {
	if os.Geteuid() != 0 {
		return errors.New("install requires root, run with sudo")
	}
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" || strings.Contains(serviceName, " ") || strings.Contains(serviceName, "/") {
		return fmt.Errorf("invalid service name: %q", serviceName)
	}

	userName, groupName, err := resolveServiceUserGroup(serviceUser, serviceGroup)
	if err != nil {
		return err
	}
	workDir, err := os.Getwd()
	if err != nil {
		return err
	}
	execPath, err := detectServiceExecPath(workDir)
	if err != nil {
		return err
	}

	servicePath := filepath.Join("/etc/systemd/system", serviceName+".service")
	content := renderSystemdService(serviceName, userName, groupName, workDir, execPath)
	if err := os.WriteFile(servicePath, []byte(content), 0o644); err != nil {
		return err
	}
	if err := runSystemctl("daemon-reload"); err != nil {
		return err
	}
	if err := runSystemctl("enable", "--now", serviceName); err != nil {
		return err
	}
	return nil
}

func uninstallSystemdService(serviceName string) error {
	if os.Geteuid() != 0 {
		return errors.New("uninstall requires root, run with sudo")
	}
	serviceName = strings.TrimSpace(serviceName)
	if serviceName == "" {
		return errors.New("service name is required")
	}
	_ = runSystemctl("disable", "--now", serviceName)
	servicePath := filepath.Join("/etc/systemd/system", serviceName+".service")
	if err := os.Remove(servicePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := runSystemctl("daemon-reload"); err != nil {
		return err
	}
	return nil
}

func resolveServiceUserGroup(serviceUser, serviceGroup string) (string, string, error) {
	userName := strings.TrimSpace(serviceUser)
	if userName == "" {
		userName = strings.TrimSpace(os.Getenv("SUDO_USER"))
	}
	if userName == "" {
		cur, err := user.Current()
		if err != nil {
			return "", "", err
		}
		userName = cur.Username
	}

	u, err := user.Lookup(userName)
	if err != nil {
		return "", "", fmt.Errorf("lookup user %s: %w", userName, err)
	}
	groupName := strings.TrimSpace(serviceGroup)
	if groupName == "" {
		g, gErr := user.LookupGroupId(u.Gid)
		if gErr != nil {
			return "", "", fmt.Errorf("lookup group id %s: %w", u.Gid, gErr)
		}
		groupName = g.Name
	}
	return userName, groupName, nil
}

func detectServiceExecPath(workDir string) (string, error) {
	preferreds := []string{
		filepath.Join(workDir, "bin", "napcat-jm-go"),
		filepath.Join(workDir, "napcat-jm-go"),
	}
	for _, preferred := range preferreds {
		if st, err := os.Stat(preferred); err == nil && !st.IsDir() {
			return preferred, nil
		}
	}
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}
	if strings.Contains(execPath, "/go-build") {
		return "", errors.New("running via go run without ./napcat-jm-go binary; build first with: go build -o napcat-jm-go .")
	}
	return execPath, nil
}

func renderSystemdService(serviceName, userName, groupName, workDir, execPath string) string {
	return fmt.Sprintf(`[Unit]
Description=%s
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=%s
Group=%s
WorkingDirectory=%s
ExecStart=%s
Restart=always
RestartSec=3
KillSignal=SIGTERM
TimeoutStopSec=20
StandardOutput=journal
StandardError=journal
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
`, serviceName, userName, groupName, workDir, execPath)
}

func runSystemctl(args ...string) error {
	cmd := exec.Command("systemctl", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl %s failed: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func newEmbeddedBypassService() *embeddedBypassService {
	return &embeddedBypassService{
		cache:   map[string]embeddedCacheItem{},
		running: map[string]bool{},
	}
}

func (s *embeddedBypassService) handleBypassV1(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var q bypassQuery
	if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": err.Error()})
		return
	}
	q.URL = strings.TrimSpace(q.URL)
	if q.URL == "" {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "url is required"})
		return
	}
	parsedURL, err := url.Parse(q.URL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"message": "url is invalid"})
		return
	}

	cacheKey := fmt.Sprintf("%s|%s|%s", q.URL, q.UserAgent, q.ProxyServer)
	now := time.Now()

	s.mu.Lock()
	if cached, ok := s.cache[cacheKey]; ok && now.Before(cached.ExpiresAt) {
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, cached.Data)
		return
	}
	if !s.running[cacheKey] {
		s.running[cacheKey] = true
		polling := bypassResponse{Message: "正在处理 cloudflare 验证（自动等待/点击），请继续轮询"}
		s.cache[cacheKey] = embeddedCacheItem{Data: polling, ExpiresAt: now.Add(60 * time.Second)}
		go s.solve(cacheKey, q)
	}
	out := s.cache[cacheKey].Data
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, out)
}

func (s *embeddedBypassService) solve(cacheKey string, q bypassQuery) {
	result, err := runCloudflareBypass(q)

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.running, cacheKey)

	if err != nil {
		s.cache[cacheKey] = embeddedCacheItem{
			Data:      bypassResponse{Message: "查询失败，请1分钟后再次尝试"},
			ExpiresAt: time.Now().Add(60 * time.Second),
		}
		log.Printf("embedded bypass failed: %v", err)
		return
	}
	result.Message = "ok"
	s.cache[cacheKey] = embeddedCacheItem{
		Data:      result,
		ExpiresAt: time.Now().Add(30 * time.Minute),
	}
}

func runCloudflareBypass(q bypassQuery) (bypassResponse, error) {
	browserPath, err := resolveChromeExecPath()
	if err != nil {
		return bypassResponse{}, err
	}

	opts := []chromedp.ExecAllocatorOption{
		chromedp.ExecPath(browserPath),
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.Flag("force-color-profile", "srgb"),
		chromedp.Flag("metrics-recording-only", true),
		chromedp.Flag("password-store", "basic"),
		chromedp.Flag("use-mock-keychain", true),
		chromedp.Flag("export-tagged-pdf", true),
		chromedp.Flag("disable-background-mode", true),
		chromedp.Flag("enable-features", "NetworkService,NetworkServiceInProcess,LoadCryptoTokenExtension,PermuteTLSExtensions"),
		chromedp.Flag("disable-features", "FlashDeprecationWarning,EnablePasswordsAccountStorage"),
		chromedp.Flag("deny-permission-prompts", true),
		chromedp.DisableGPU,
		chromedp.Flag("accept-lang", "en-US"),
	}
	if runtime.GOOS == "linux" {
		opts = append(opts, chromedp.Headless)
		opts = append(opts, chromedp.Flag("no-sandbox", true))
	}
	if strings.TrimSpace(q.UserAgent) != "" {
		opts = append(opts, chromedp.UserAgent(strings.TrimSpace(q.UserAgent)))
	}
	if strings.TrimSpace(q.ProxyServer) != "" {
		opts = append(opts, chromedp.ProxyServer(strings.TrimSpace(q.ProxyServer)))
	}

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()
	ctx, cancel := chromedp.NewContext(allocCtx, chromedp.WithLogf(func(string, ...any) {}))
	defer cancel()
	ctx, timeoutCancel := context.WithTimeout(ctx, 180*time.Second)
	defer timeoutCancel()

	if err := chromedp.Run(ctx, chromedp.Navigate(q.URL)); err != nil {
		return bypassResponse{}, fmt.Errorf("navigate: %w", err)
	}
	// First try passive wait for the common 5s challenge.
	time.Sleep(6 * time.Second)

	var userAgent string
	if err := chromedp.Run(ctx, chromedp.Evaluate(`navigator.userAgent`, &userAgent)); err != nil {
		return bypassResponse{}, fmt.Errorf("get user agent: %w", err)
	}

	for i := 0; i < 75; i++ {
		// Poll cookies; default 5s shield may resolve automatically.
		if i > 0 {
			time.Sleep(2 * time.Second)
		}
		// Some pages require clicking challenge widgets instead of passive wait.
		if i >= 3 && (i == 3 || i%8 == 0) {
			clicked := tryCloudflareChallengeClick(ctx)
			if clicked > 0 {
				log.Printf("embedded bypass: challenge click attempts=%d", clicked)
			}
		}

		var cookies []*network.Cookie
		if err := chromedp.Run(ctx, chromedp.ActionFunc(func(ctx context.Context) error {
			var err error
			cookies, err = network.GetCookies().Do(ctx)
			return err
		})); err != nil {
			continue
		}

		found := false
		out := bypassResponse{UserAgent: userAgent, Cookies: make([]bypassCookie, 0, len(cookies))}
		for _, ck := range cookies {
			if ck.Name == "cf_clearance" {
				found = true
			}
			out.Cookies = append(out.Cookies, bypassCookie{Name: ck.Name, Value: ck.Value})
		}
		if found {
			return out, nil
		}
	}

	return bypassResponse{}, errors.New("no cf_clearance cookie acquired after passive+interactive challenge window (~150s)")
}

func tryCloudflareChallengeClick(ctx context.Context) int {
	clickedTotal := 0

	// Click visible challenge-like controls in document and open shadow roots.
	js := `(function () {
		let clicked = 0;
		const tryClick = (el) => {
			if (!el) return;
			const r = el.getBoundingClientRect();
			if (r.width < 2 || r.height < 2) return;
			el.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
			el.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
			if (typeof el.click === 'function') el.click();
			clicked++;
		};
		const selectors = [
			'#challenge-stage input[type="checkbox"]',
			'input[type="checkbox"]',
			'#challenge-stage button',
			'button',
			'[role="button"]',
			'[data-testid*="challenge"]',
			'[id*="challenge"]'
		];
		for (const sel of selectors) {
			for (const el of document.querySelectorAll(sel)) {
				tryClick(el);
			}
		}
		for (const host of document.querySelectorAll('*')) {
			if (!host.shadowRoot) continue;
			for (const el of host.shadowRoot.querySelectorAll('input[type="checkbox"], button, [role="button"]')) {
				tryClick(el);
			}
		}
		return clicked;
	})();`
	var clicked int64
	if err := chromedp.Run(ctx, chromedp.Evaluate(js, &clicked)); err == nil && clicked > 0 {
		clickedTotal += int(clicked)
	}

	// If Cloudflare challenge is inside iframe, click its center area.
	var iframePoint map[string]float64
	if err := chromedp.Run(ctx, chromedp.Evaluate(`(() => {
		const nodes = Array.from(document.querySelectorAll('iframe'));
		const target = nodes.find((f) => {
			const src = (f.getAttribute('src') || '').toLowerCase();
			const title = (f.getAttribute('title') || '').toLowerCase();
			return src.includes('challenges.cloudflare.com') || title.includes('challenge') || title.includes('security');
		});
		if (!target) return null;
		const r = target.getBoundingClientRect();
		if (r.width < 2 || r.height < 2) return null;
		return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
	})()`, &iframePoint)); err == nil {
		if iframePoint != nil {
			x, xok := iframePoint["x"]
			y, yok := iframePoint["y"]
			if xok && yok {
				if err := chromedp.Run(ctx, chromedp.MouseClickXY(x, y)); err == nil {
					clickedTotal++
				}
			}
		}
	}
	return clickedTotal
}

func resolveChromeExecPath() (string, error) {
	candidates := []string{
		"google-chrome",
		"google-chrome-stable",
		"chromium",
		"chromium-browser",
		"chrome",
	}
	for _, name := range candidates {
		if p, err := exec.LookPath(name); err == nil {
			return p, nil
		}
	}

	absoluteCandidates := []string{
		"/usr/bin/google-chrome",
		"/usr/bin/google-chrome-stable",
		"/usr/bin/chromium",
		"/usr/bin/chromium-browser",
		"/opt/google/chrome/chrome",
	}
	for _, p := range absoluteCandidates {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
	}
	return "", errors.New("no chrome/chromium executable found for embedded bypass; install chromium or set cf_bypass_api_url to an external bypass service")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *App) runLocalSelfTest() error {
	cfg := a.currentConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	testID := "123456"
	album, err := a.jm.GetAlbum(ctx, testID)
	if err != nil {
		return fmt.Errorf("get album failed: %w", err)
	}

	testOut := filepath.Join(cfg.FileDir, fmt.Sprintf("%s_selftest.pdf", album.ID))
	if err := a.jm.DownloadTo(ctx, testID, testOut, "1234"); err != nil {
		return fmt.Errorf("download test pdf failed: %w", err)
	}
	defer os.Remove(testOut)

	if !a.bot.SendPrivateMessage(cfg.AdminID, "本地自测：文本发送通过") {
		return errors.New("send text failed")
	}
	if !a.bot.SendPrivateFile(cfg, cfg.AdminID, testOut) {
		return errors.New("send file failed")
	}
	return nil
}

func (a *App) saveConfig() {
	a.cfgMu.RLock()
	defer a.cfgMu.RUnlock()
	raw, err := yaml.Marshal(a.cfg)
	if err != nil {
		log.Printf("marshal config failed: %v", err)
		return
	}
	if err := os.WriteFile(a.cfgPath, raw, 0o644); err != nil {
		log.Printf("write config failed: %v", err)
	}
}

func (a *App) handleHTTPEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	go a.handleMessageEvent(data)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"success"}`))
}

func (a *App) handleMessageEvent(data map[string]any) {
	if toString(data["post_type"]) != "message" {
		return
	}
	messageType := toString(data["message_type"])
	rawMessage := strings.TrimSpace(toString(data["raw_message"]))
	userID := toInt64(data["user_id"])
	groupID := toInt64(data["group_id"])
	log.Printf("[Message] type=%s group=%d user=%d raw=%q", messageType, groupID, userID, rawMessage)
	scope := requestScope(messageType, groupID, userID)
	soutuScopeKey := requestSoutuScope(messageType, groupID, userID)
	soutuCompatScopeKey := requestSoutuCompatScope(messageType, groupID, userID)
	if isSoutuRelatedEvent(rawMessage, data) {
	}

	if a.handleSoutuArmingCommand(rawMessage, messageType, groupID, userID, soutuScopeKey, soutuCompatScopeKey) {
		return
	}
	if a.tryHandleSoutuImage(data, messageType, groupID, userID, soutuScopeKey, soutuCompatScopeKey) {
		return
	}

	if rawMessage == "" {
		return
	}
	if a.handleAdminClaimCommand(rawMessage, messageType, groupID, userID) {
		return
	}

	if a.handleAIImageCommand(rawMessage, data, messageType, groupID, userID) {
		return
	}

	if matched(`^/jm\s+help$`, rawMessage) {
		a.sendMessage(messageType, groupID, userID, helpMessage())
		return
	}
	if a.handleCfgCommand(rawMessage, messageType, groupID, userID) {
		return
	}
	if a.handleDedupCommand(rawMessage, messageType, groupID, userID) {
		return
	}
	if m := mustMatch(`^/jm\s+mode\s+(pdf|zip)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可设置发送格式") {
			return
		}
		a.cfgMu.Lock()
		if messageType == "group" && groupID > 0 {
			a.cfg.SendModeGroup[strconv.FormatInt(groupID, 10)] = m[1]
		} else {
			a.cfg.SendModeGlobal = m[1]
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "发送格式已更新")
		return
	}
	if m := mustMatch(`^/jm\s+fname\s+(jm|full|current)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可设置发送文件命名方式") {
			return
		}
		mode := normalizeSendNameMode(m[1])
		a.cfgMu.Lock()
		if messageType == "group" && groupID > 0 {
			a.cfg.SendNameModeGroup[strconv.FormatInt(groupID, 10)] = mode
		} else {
			a.cfg.SendNameModeGlobal = mode
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "发送文件命名方式已更新："+mode)
		return
	}
	if m := mustMatch(`^/jm\s+enc\s+(on|off)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可设置加密开关") {
			return
		}
		enabled := m[1] == "on"
		a.cfgMu.Lock()
		if messageType == "group" && groupID > 0 {
			a.cfg.EncEnabledGroup[strconv.FormatInt(groupID, 10)] = enabled
		} else {
			a.cfg.EncEnabledGlobal = enabled
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "加密开关已更新")
		return
	}
	if m := mustMatch(`^/jm\s+passwd\s+(.+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可设置加密密码") {
			return
		}
		pw := strings.TrimSpace(m[1])
		if pw == "" {
			a.sendMessage(messageType, groupID, userID, "密码不能为空")
			return
		}
		a.cfgMu.Lock()
		if messageType == "group" && groupID > 0 {
			a.cfg.EncPasswordGroup[strconv.FormatInt(groupID, 10)] = pw
		} else {
			a.cfg.EncPasswordGlobal = pw
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "加密密码已设置")
		return
	}
	if m := mustMatch(`^/jm\s+randpwd\s+(on|off)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可设置随机密码开关") {
			return
		}
		enabled := m[1] == "on"
		a.cfgMu.Lock()
		if messageType == "group" && groupID > 0 {
			a.cfg.RandomPasswordEnabledGroup[strconv.FormatInt(groupID, 10)] = enabled
		} else {
			a.cfg.RandomPasswordEnabledGlobal = enabled
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "随机密码开关已更新")
		return
	}
	if m := mustMatch(`^/jm\s+regex\s+(on|off)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可设置正则模式") {
			return
		}
		enabled := m[1] == "on"
		a.cfgMu.Lock()
		if messageType == "group" && groupID > 0 {
			a.cfg.RegexEnabledGroup[strconv.FormatInt(groupID, 10)] = enabled
		} else {
			a.cfg.RegexEnabledGlobal = enabled
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "正则模式已更新")
		return
	}
	if m := mustMatch(`^/jm\s+strict\s+(on|off)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可设置严格模式") {
			return
		}
		enabled := m[1] == "on"
		a.cfgMu.Lock()
		if messageType == "group" && groupID > 0 {
			a.cfg.StrictModeGroup[strconv.FormatInt(groupID, 10)] = enabled
		} else {
			a.cfg.StrictModeGlobal = enabled
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("严格模式已%s", map[bool]string{true: "开启", false: "关闭"}[enabled]))
		return
	}
	if matched(`^/jm\s+goodluck$`, rawMessage) || matched(`^/goodluck$`, rawMessage) || rawMessage == "随机本子" {
		id, ok := a.randomExistingJMID()
		if !ok {
			a.sendMessage(messageType, groupID, userID, "随机本子失败：暂未找到可用本子，请稍后重试")
			return
		}
		a.sendMessage(messageType, groupID, userID, "随机本子ID：JM"+id)
		a.enqueueDownloads([]string{id}, messageType, groupID, userID, data)
		return
	}
	if matched(`^/jm\s+on$`, rawMessage) {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		a.cfgMu.Lock()
		if groupID > 0 {
			a.cfg.BannedGroup = removeStr(a.cfg.BannedGroup, strconv.FormatInt(groupID, 10))
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "禁漫功能已开启")
		return
	}
	if matched(`^/jm\s+off$`, rawMessage) {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		a.cfgMu.Lock()
		if groupID > 0 && !contains(a.cfg.BannedGroup, strconv.FormatInt(groupID, 10)) {
			a.cfg.BannedGroup = append(a.cfg.BannedGroup, strconv.FormatInt(groupID, 10))
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "禁漫功能已关闭")
		return
	}
	if m := mustMatch(`^/jm\s+addban\s+(\d+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		a.cfgMu.Lock()
		if !contains(a.cfg.BannedID, m[1]) {
			a.cfg.BannedID = append(a.cfg.BannedID, m[1])
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "已封禁本子ID："+m[1])
		return
	}
	if m := mustMatch(`^/jm\s+delban\s+(\d+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		a.cfgMu.Lock()
		a.cfg.BannedID = removeStr(a.cfg.BannedID, m[1])
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "已解封本子ID："+m[1])
		return
	}
	if m := mustMatch(`^/jm\s+setmax\s+(\d+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		n, _ := strconv.Atoi(m[1])
		a.cfgMu.Lock()
		a.cfg.MaxEpisodes = n
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("章节数阈值已设为 %d", n))
		return
	}
	if m := mustMatch(`^/jm\s+allow\s+add\s+(\d+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		gid, _ := strconv.ParseInt(m[1], 10, 64)
		a.cfgMu.Lock()
		if !containsInt64(a.cfg.AllowedGroup, gid) {
			a.cfg.AllowedGroup = append(a.cfg.AllowedGroup, gid)
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("已添加白名单群：%d", gid))
		return
	}
	if m := mustMatch(`^/jm\s+allow\s+del\s+(\d+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		gid, _ := strconv.ParseInt(m[1], 10, 64)
		a.cfgMu.Lock()
		a.cfg.AllowedGroup = removeInt64(a.cfg.AllowedGroup, gid)
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("已移除白名单群：%d", gid))
		return
	}
	if matched(`^/jm\s+allow\s+list$`, rawMessage) {
		a.cfgMu.RLock()
		groups := a.cfg.AllowedGroup
		a.cfgMu.RUnlock()
		if len(groups) == 0 {
			a.sendMessage(messageType, groupID, userID, "白名单为空，所有群均可使用")
		} else {
			var ids []string
			for _, g := range groups {
				ids = append(ids, strconv.FormatInt(g, 10))
			}
			a.sendMessage(messageType, groupID, userID, "白名单群："+strings.Join(ids, ", "))
		}
		return
	}
	if matched(`^/jm\s+daily\s+on$`, rawMessage) {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		a.cfgMu.Lock()
		a.cfg.DailyRecommendEnabled = true
		a.cfgMu.Unlock()
		a.saveConfig()
		a.startDailyRecommend()
		a.sendMessage(messageType, groupID, userID, "每日本子推荐已开启")
		return
	}
	if matched(`^/jm\s+daily\s+off$`, rawMessage) {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		a.cfgMu.Lock()
		a.cfg.DailyRecommendEnabled = false
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "每日本子推荐已关闭")
		return
	}
	if m := mustMatch(`^/jm\s+daily\s+add\s+(\d+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		groupIDToAdd, _ := strconv.ParseInt(m[1], 10, 64)
		a.cfgMu.Lock()
		if !containsInt64(a.cfg.DailyRecommendGroups, groupIDToAdd) {
			a.cfg.DailyRecommendGroups = append(a.cfg.DailyRecommendGroups, groupIDToAdd)
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("已添加每日推荐群：%d", groupIDToAdd))
		return
	}
	if m := mustMatch(`^/jm\s+daily\s+del\s+(\d+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		groupIDToDel, _ := strconv.ParseInt(m[1], 10, 64)
		a.cfgMu.Lock()
		a.cfg.DailyRecommendGroups = removeInt64(a.cfg.DailyRecommendGroups, groupIDToDel)
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("已删除每日推荐群：%d", groupIDToDel))
		return
	}
	if matched(`^/jm\s+daily\s+now$`, rawMessage) {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可操作") {
			return
		}
		a.sendMessage(messageType, groupID, userID, "正在发送每日推荐...")
		go a.sendDailyRecommend()
		return
	}
	if m := mustMatch(`^(?:/jm\s+(?:look|验车)\s+|验车\s+)(.+)$`, rawMessage); m != nil {
		if !a.isJMAllowed(messageType, groupID, userID) {
			return
		}
		input := strings.TrimSpace(m[1])
		log.Printf("[验车] 开始检索: input=%q", input)
		a.sendMessage(messageType, groupID, userID, "正在检索："+input)

		var al *Album
		var err error

		// 去除 JM/jm 前缀
		cleaned := strings.TrimPrefix(strings.TrimPrefix(input, "jm"), "JM")

		// 如果去除前缀后是纯数字（JM号），直接查询
		if re := regexp.MustCompile(`^\d+$`); re.MatchString(cleaned) {
			log.Printf("[验车] JM号查询: %s", cleaned)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			al, err = a.jm.GetAlbum(ctx, cleaned)
			cancel()
			if err != nil {
				log.Printf("[验车] 查询失败: %v", err)
				a.sendMessage(messageType, groupID, userID, "查询失败："+err.Error())
				return
			}
		} else {
			// 否则按关键词搜索，显示前10个结果
			keyword := normalizeSearchKeyword(input)
			log.Printf("[验车] 关键词搜索: %s (原始: %s)", keyword, input)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			results, searchErr := a.jm.SearchAlbums(ctx, keyword, 10)
			cancel()
			if searchErr != nil || len(results) == 0 {
				log.Printf("[验车] 搜索失败: err=%v", searchErr)
				a.sendMessage(messageType, groupID, userID, "未找到相关本子")
				return
			}

			// 缓存搜索结果供用户选择
			searchScope := requestScope(messageType, groupID, userID)
			a.searchMu.Lock()
			a.search[searchScope] = PendingSearch{
				At:         time.Now(),
				AggResults: results,
			}
			a.searchMu.Unlock()

			// 显示搜索结果列表
			lines := make([]string, 0, len(results))
			for i, r := range results {
				tags := strings.Join(r.Tags, ", ")
				if len(tags) > 40 {
					tags = tags[:40] + "..."
				}
				author := ""
				if r.Author != "" {
					author = " 作者：" + r.Author
				}
				lines = append(lines, fmt.Sprintf("%d. [JM] %s%s\n   标签：%s", i+1, r.Title, author, tags))
			}
			msg := fmt.Sprintf("搜索结果（共%d条）：\n%s\n\n回复 序号 下载（可批量：1 2 3）", len(results), strings.Join(lines, "\n"))
			a.sendMessage(messageType, groupID, userID, msg)
			return
		}

		// 纯数字JM号查询，直接显示详情
		log.Printf("[验车] 获取到本子: ID=%s 标题=%s", al.ID, al.Title)

		// 构建详细信息
		tags := strings.Join(al.Tags, ", ")
		// 获取封面并转换为PDF
		coverPath := ""
		// 优先从本地manga目录获取
		if mangaPath, ok, _ := a.findMangaPageByID(al.ID, 1); ok && fileExists(mangaPath) {
			coverPath = mangaPath
			log.Printf("[验车] 使用本地封面: %s", coverPath)
		} else {
			// 本地没有，尝试从JM API下载第一章封面
			log.Printf("[验车] 本地无封面，尝试从API下载")
			coverPath = a.downloadJMCover(al.ID)
			if coverPath != "" {
				log.Printf("[验车] API封面下载成功: %s", coverPath)
			} else {
				log.Printf("[验车] API封面下载失败，无封面")
			}
		}

		// 把封面图片转换为PDF
		pdfPath := ""
		if coverPath != "" && fileExists(coverPath) {
			pdfPath = filepath.Join(a.tmpDir(), fmt.Sprintf("cover_%s_%d.pdf", al.ID, time.Now().UnixNano()))
			if err := imageToPDF(coverPath, pdfPath); err != nil {
				log.Printf("[验车] 封面转PDF失败: %v", err)
				pdfPath = ""
			}
		}

		// 使用转发消息发送（信息+封面PDF）
		cfg := a.currentConfig()
		infoMsg := fmt.Sprintf("【本子详情】\nID：%s\n标题：%s\n描述：%s\n标签：%s\n章节：%d\n浏览：%s", al.ID, al.Title, al.Description, tags, al.Episodes, al.Views)
		log.Printf("[验车] 发送转发消息: pdfPath=%q", pdfPath)
		a.sendComicForwardMessage(messageType, groupID, userID, infoMsg, "", pdfPath, cfg)
		// 删除临时封面PDF
		if pdfPath != "" && fileExists(pdfPath) {
			_ = os.Remove(pdfPath)
		}
		return
	}
	if m := mustMatch(`^(?:/jm\s+search|搜索)\s+(.+)$`, rawMessage); m != nil {
		if !a.isJMAllowed(messageType, groupID, userID) {
			return
		}
		keyword := normalizeSearchKeyword(m[1])
		a.sendMessage(messageType, groupID, userID, "正在搜索："+keyword+" ...")

		// 聚合搜索：同时搜索哔咔和JM
		var allResults []SearchResultItem

		// 搜索哔咔
		if a.bika != nil {
			token := a.getBikaUserToken(userID)
			if token != "" {
				results, _, err := a.bika.Search(keyword, 1, token)
				if err != nil {
					log.Printf("[Search] 哔咔搜索失败: %v", err)
				} else if len(results) > 0 {
					for _, r := range results {
						allResults = append(allResults, SearchResultItem{
							Source: "Bika",
							ID:     r.ID,
							Title:  r.Title,
							Author: r.Author,
							Tags:   r.Tags,
						})
					}
				}
			} else {
				log.Printf("[Search] 用户未登录哔咔，跳过哔咔搜索")
			}
		}

		// 搜索JM（返回前10个结果）
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		jmResults, jmErr := a.jm.SearchAlbums(ctx, keyword, 10)
		if jmErr != nil {
			log.Printf("[Search] JM搜索失败: %v", jmErr)
		} else {
			allResults = append(allResults, jmResults...)
		}

		if len(allResults) == 0 {
			a.sendMessage(messageType, groupID, userID, "未找到相关本子")
			return
		}

		// 缓存搜索结果
		a.searchMu.Lock()
		a.search[scope] = PendingSearch{
			AlbumID:    "",
			Title:      "",
			At:         time.Now(),
			AggResults: allResults,
		}
		a.searchMu.Unlock()

		// 显示结果
		var lines []string
		for i, r := range allResults {
			if i >= 15 {
				break
			}
			tags := strings.Join(r.Tags, ", ")
			if len(tags) > 40 {
				tags = tags[:40] + "..."
			}
			author := ""
			if r.Author != "" {
				author = " 作者：" + r.Author
			}
			lines = append(lines, fmt.Sprintf("%d. [%s] %s%s\n   标签：%s", i+1, r.Source, r.Title, author, tags))
		}
		msg := fmt.Sprintf("搜索结果（共%d条）：\n%s\n\n回复 确认 <序号> 下载（可批量：确认 1 2 3）", len(allResults), strings.Join(lines, "\n"))
		a.sendRecordMessage(messageType, groupID, userID, msg)
		return
	}

	// Bika 命令处理
	if a.handleBikaCommand(rawMessage, messageType, groupID, userID, scope, data) {
		return
	}

	// 处理确认命令（支持聚合搜索、哔咔搜索和每日推荐）
	// 支持格式：确认 1 2 3、确认1、1、1 2 3
	// 去除CQ码后再匹配（支持回复消息时带数字）
	strippedMsg := stripCQCodes(rawMessage)
	strippedMsg = strings.TrimSpace(strippedMsg)
	if m := mustMatch(`^(?:确认\s*)?(\d+(?:\s+\d+)*)$`, strippedMsg); m != nil {
		log.Printf("[Confirm] 匹配到数字命令: rawMessage=%q, strippedMsg=%q, m[1]=%q", rawMessage, strippedMsg, m[1])
		// 解析序号
		parts := strings.Fields(m[1])
		var indices []int
		for _, p := range parts {
			idx, err := strconv.Atoi(p)
			if err != nil || idx <= 0 {
				continue
			}
			indices = append(indices, idx)
		}
		if len(indices) == 0 {
			return
		}

		// 检查是否回复了每日推荐的消息（通过检查被回复消息的特征文本）
		isDailyReply := false
		replyID := extractReplyID(rawMessage)
		if replyID != "" {
			// 通过检查原始消息中的特征文本来判断是否是每日推荐
			// 从 data 中获取被回复消息的内容
			if msgArray, ok := data["message"].([]any); ok {
				for _, seg := range msgArray {
					if segMap, ok := seg.(map[string]any); ok {
						if segMap["type"] == "reply" {
							// 这是回复消息，但我们无法直接获取被回复消息的内容
							// 使用一个简单的方法：检查用户是否明确回复了每日推荐
							// 由于无法获取被回复消息内容，我们假设回复消息时使用数字就是回复每日推荐
							isDailyReply = true
							break
						}
					}
				}
			}
		}

		// 只有回复消息时才检查每日推荐缓存
		dailyKey := fmt.Sprintf("daily:%d", groupID)
		a.searchMu.Lock()
		dailyPending, dailyOk := a.search[dailyKey]
		if dailyOk && time.Since(dailyPending.At) > 24*time.Hour {
			delete(a.search, dailyKey)
			dailyOk = false
		}
		a.searchMu.Unlock()
		log.Printf("[Confirm] 检查每日推荐缓存: dailyKey=%q, dailyOk=%v, results=%d, isDailyReply=%v", dailyKey, dailyOk, len(dailyPending.AggResults), isDailyReply)

		if isDailyReply && dailyOk && len(dailyPending.AggResults) > 0 {
			// 处理每日推荐结果
			var validItems []SearchResultItem
			for _, idx := range indices {
				if idx > len(dailyPending.AggResults) {
					a.sendMessage(messageType, groupID, userID, fmt.Sprintf("序号 %d 超出范围，最大为 %d", idx, len(dailyPending.AggResults)))
					continue
				}
				validItems = append(validItems, dailyPending.AggResults[idx-1])
			}

			if len(validItems) == 0 {
				return
			}

			// 逐个下载
			if len(validItems) == 1 {
				item := validItems[0]
				a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载：%s [%s]", item.Title, item.Source))
				if item.Source == "Bika" {
					go a.bikaDownloadAndSend(item.ID, "", messageType, groupID, userID)
				} else {
					a.enqueueDownloads([]string{item.ID}, messageType, groupID, userID, data)
				}
			} else {
				names := make([]string, len(validItems))
				for i, item := range validItems {
					names[i] = fmt.Sprintf("%s [%s]", item.Title, item.Source)
				}
				a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载 %d 个本子：\n%s", len(validItems), strings.Join(names, "\n")))
				for _, item := range validItems {
					if item.Source == "Bika" {
						go a.bikaDownloadAndSend(item.ID, "", messageType, groupID, userID)
					} else {
						a.enqueueDownloads([]string{item.ID}, messageType, groupID, userID, data)
					}
				}
			}
			return
		}

		// 先检查聚合搜索结果
		a.searchMu.Lock()
		aggPending, aggOk := a.search[scope]
		if aggOk && time.Since(aggPending.At) > time.Duration(a.cfg.SearchTimeout)*time.Second {
			delete(a.search, scope)
			aggOk = false
		}
		a.searchMu.Unlock()

		if aggOk && len(aggPending.AggResults) > 0 {
			// 处理聚合搜索结果
			var validItems []SearchResultItem
			for _, idx := range indices {
				if idx > len(aggPending.AggResults) {
					a.sendMessage(messageType, groupID, userID, fmt.Sprintf("序号 %d 超出范围，最大为 %d", idx, len(aggPending.AggResults)))
					continue
				}
				validItems = append(validItems, aggPending.AggResults[idx-1])
			}

			if len(validItems) == 0 {
				return
			}

			// 清除搜索缓存
			a.searchMu.Lock()
			delete(a.search, scope)
			a.searchMu.Unlock()

			// 逐个下载
			if len(validItems) == 1 {
				item := validItems[0]
				a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载：%s [%s]", item.Title, item.Source))
				if item.Source == "Bika" {
					go a.bikaDownloadAndSend(item.ID, "", messageType, groupID, userID)
				} else {
					a.enqueueDownloads([]string{item.ID}, messageType, groupID, userID, data)
				}
			} else {
				names := make([]string, len(validItems))
				for i, item := range validItems {
					names[i] = fmt.Sprintf("%s [%s]", item.Title, item.Source)
				}
				a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载 %d 个本子：\n%s", len(validItems), strings.Join(names, "\n")))
				for _, item := range validItems {
					if item.Source == "Bika" {
						go a.bikaDownloadAndSend(item.ID, "", messageType, groupID, userID)
					} else {
						a.enqueueDownloads([]string{item.ID}, messageType, groupID, userID, data)
					}
				}
			}
			return
		}

		// 检查哔咔搜索结果
		bikaSearchCacheMu.Lock()
		bikaPending, bikaOk := bikaSearchCache[scope]
		if bikaOk && time.Since(bikaPending.At) > 10*time.Minute {
			delete(bikaSearchCache, scope)
			bikaOk = false
		}
		bikaSearchCacheMu.Unlock()

		if bikaOk && len(bikaPending.Results) > 0 {
			// 处理哔咔搜索结果
			var validComics []BikaSearchResult
			for _, idx := range indices {
				if idx > len(bikaPending.Results) {
					a.sendMessage(messageType, groupID, userID, fmt.Sprintf("序号 %d 超出范围，最大为 %d", idx, len(bikaPending.Results)))
					continue
				}
				validComics = append(validComics, bikaPending.Results[idx-1])
			}

			if len(validComics) == 0 {
				return
			}

			// 清除搜索缓存
			bikaSearchCacheMu.Lock()
			delete(bikaSearchCache, scope)
			bikaSearchCacheMu.Unlock()

			// 逐个下载
			if len(validComics) == 1 {
				comic := validComics[0]
				a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载哔咔漫画：%s", comic.Title))
				go a.bikaDownloadAndSend(comic.ID, "", messageType, groupID, userID)
			} else {
				names := make([]string, len(validComics))
				for i, c := range validComics {
					names[i] = c.Title
				}
				a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载 %d 个哔咔漫画：\n%s", len(validComics), strings.Join(names, "\n")))
				for _, comic := range validComics {
					go a.bikaDownloadAndSend(comic.ID, "", messageType, groupID, userID)
				}
			}
			return
		}

		// 处理普通JM搜索结果（单个）
		a.searchMu.Lock()
		jmPending, jmOk := a.search[scope]
		if jmOk && time.Since(jmPending.At) > time.Duration(a.cfg.SearchTimeout)*time.Second {
			delete(a.search, scope)
			jmOk = false
		}
		a.searchMu.Unlock()

		if jmOk && jmPending.AlbumID != "" && len(indices) == 1 && indices[0] == 1 {
			a.searchMu.Lock()
			delete(a.search, scope)
			a.searchMu.Unlock()
			a.sendMessage(messageType, groupID, userID, "已确认，开始处理本子："+jmPending.Title)
			a.enqueueDownloads([]string{jmPending.AlbumID}, messageType, groupID, userID, data)
			return
		}

		return
	}

	// 处理"确认"（不带数字）默认为"确认 1"
	if rawMessage == "确认" {
		// 检查聚合搜索结果
		a.searchMu.Lock()
		aggPending, aggOk := a.search[scope]
		if aggOk && time.Since(aggPending.At) > time.Duration(a.cfg.SearchTimeout)*time.Second {
			delete(a.search, scope)
			aggOk = false
		}
		a.searchMu.Unlock()

		if aggOk && len(aggPending.AggResults) > 0 {
			// 聚合搜索结果，下载第一个
			item := aggPending.AggResults[0]
			a.searchMu.Lock()
			delete(a.search, scope)
			a.searchMu.Unlock()
			a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载：%s [%s]", item.Title, item.Source))
			if item.Source == "Bika" {
				go a.bikaDownloadAndSend(item.ID, "", messageType, groupID, userID)
			} else {
				a.enqueueDownloads([]string{item.ID}, messageType, groupID, userID, data)
			}
			return
		}

		// 检查哔咔搜索结果
		bikaSearchCacheMu.Lock()
		bikaPending, bikaOk := bikaSearchCache[scope]
		if bikaOk && time.Since(bikaPending.At) > 10*time.Minute {
			delete(bikaSearchCache, scope)
			bikaOk = false
		}
		bikaSearchCacheMu.Unlock()

		if bikaOk && len(bikaPending.Results) > 0 {
			// 哔咔搜索结果，下载第一个
			comic := bikaPending.Results[0]
			bikaSearchCacheMu.Lock()
			delete(bikaSearchCache, scope)
			bikaSearchCacheMu.Unlock()
			a.sendMessage(messageType, groupID, userID, fmt.Sprintf("开始下载哔咔漫画：%s", comic.Title))
			go a.bikaDownloadAndSend(comic.ID, "", messageType, groupID, userID)
			return
		}

		// 检查普通JM搜索结果
		a.searchMu.Lock()
		jmPending, jmOk := a.search[scope]
		if jmOk && time.Since(jmPending.At) > time.Duration(a.cfg.SearchTimeout)*time.Second {
			delete(a.search, scope)
			jmOk = false
		}
		a.searchMu.Unlock()

		if jmOk && jmPending.AlbumID != "" {
			a.searchMu.Lock()
			delete(a.search, scope)
			a.searchMu.Unlock()
			a.sendMessage(messageType, groupID, userID, "已确认，开始处理本子："+jmPending.Title)
			a.enqueueDownloads([]string{jmPending.AlbumID}, messageType, groupID, userID, data)
			return
		}

		return
	}

	// 严格模式检查：只处理以/jm开头的消息
	strictMode := a.getStrictMode(messageType, groupID)
	if strictMode && !strings.HasPrefix(strings.TrimSpace(rawMessage), "/jm") {
		return
	}

	regexEnabled := a.getRegexEnabled(messageType, groupID)
	numbers := extractJMNumbersFromEvent(data, regexEnabled)
	log.Printf("[JM] regex=%v numbers=%v group=%d", regexEnabled, numbers, groupID)
	if len(numbers) > 0 {
		a.enqueueDownloads(numbers, messageType, groupID, userID, data)
	}
}

func (a *App) requireAdmin(messageType string, groupID, userID int64, deny string) bool {
	a.cfgMu.RLock()
	admin := a.cfg.AdminID
	a.cfgMu.RUnlock()
	if userID != admin {
		a.sendMessage(messageType, groupID, userID, deny)
		return false
	}
	return true
}

func (a *App) handleAdminClaimCommand(rawMessage, messageType string, groupID, userID int64) bool {
	if !matched(`^/jm\s+admin$`, rawMessage) {
		return false
	}
	if messageType != "private" || userID <= 0 {
		a.sendMessage(messageType, groupID, userID, "请私聊机器人发送 /jm admin 认领管理员")
		return true
	}

	a.cfgMu.Lock()
	current := a.cfg.AdminID
	if current == 0 {
		a.cfg.AdminID = userID
		if a.cfg.CardUserID == 0 || a.cfg.CardUserID == 10000 {
			a.cfg.CardUserID = userID
		}
		a.cfgMu.Unlock()
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, "管理员认领成功")
		return true
	}
	a.cfgMu.Unlock()
	if current == userID {
		a.sendMessage(messageType, groupID, userID, "你已经是管理员")
	} else {
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("管理员已设置为：%d", current))
	}
	return true
}

func expandUserVars(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	u := os.Getenv("USER")
	if u == "" {
		if cu, err := user.Current(); err == nil {
			u = cu.Username
		}
	}
	if u != "" {
		s = strings.ReplaceAll(s, "${USER}", u)
		s = strings.ReplaceAll(s, "$USER", u)
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		s = strings.ReplaceAll(s, "${HOME}", home)
		s = strings.ReplaceAll(s, "$HOME", home)
	}
	return s
}

func (a *App) handleCfgCommand(rawMessage, messageType string, groupID, userID int64) bool {
	if matched(`^/jm\s+cfg\s+list$`, rawMessage) {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可配置开关") {
			return true
		}
		msg := "可配置开关：\n" +
			"1) reply_as_card\n" +
			"2) cbz_chapter_enabled\n" +
			"3) cbz_series_enabled\n" +
			"4) embedded_bypass_enabled\n" +
			"5) http_port_fallback\n" +
			"6) local_test_mode\n" +
			"7) local_test_exit_after_selftest\n" +
			"8) enc_enabled (支持global/group)\n" +
			"9) randpwd_enabled (支持global/group)\n" +
			"10) regex_enabled (支持global/group)\n\n" +
			"示例：\n" +
			"/jm cfg show reply_as_card\n" +
			"/jm cfg set reply_as_card on\n" +
			"/jm cfg set enc_enabled on group\n" +
			"/jm cfg set enc_enabled off global"
		a.sendMessage(messageType, groupID, userID, msg)
		return true
	}
	if m := mustMatch(`^/jm\s+cfg\s+show\s+([a-z_]+)$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可配置开关") {
			return true
		}
		key := strings.ToLower(strings.TrimSpace(m[1]))
		a.cfgMu.RLock()
		cfg := a.cfg
		var msg string
		switch key {
		case "reply_as_card":
			msg = fmt.Sprintf("%s = %t", key, cfg.ReplyAsCard)
		case "cbz_chapter_enabled":
			msg = fmt.Sprintf("%s = %t", key, cfg.CBZChapterEnabled)
		case "cbz_series_enabled":
			msg = fmt.Sprintf("%s = %t", key, cfg.CBZSeriesEnabled)
		case "embedded_bypass_enabled":
			msg = fmt.Sprintf("%s = %t", key, cfg.EmbeddedBypassEnable)
		case "http_port_fallback":
			msg = fmt.Sprintf("%s = %t", key, cfg.HTTPPortFallback)
		case "local_test_mode":
			msg = fmt.Sprintf("%s = %t", key, cfg.LocalTestMode)
		case "local_test_exit_after_selftest":
			msg = fmt.Sprintf("%s = %t", key, cfg.LocalTestExitAfterSelftest)
		case "enc_enabled":
			groupVal := cfg.EncEnabledGroup[strconv.FormatInt(groupID, 10)]
			msg = fmt.Sprintf("%s: global=%t, group(%d)=%t", key, cfg.EncEnabledGlobal, groupID, groupVal)
		case "randpwd_enabled":
			groupVal := cfg.RandomPasswordEnabledGroup[strconv.FormatInt(groupID, 10)]
			msg = fmt.Sprintf("%s: global=%t, group(%d)=%t", key, cfg.RandomPasswordEnabledGlobal, groupID, groupVal)
		case "regex_enabled":
			groupVal := cfg.RegexEnabledGroup[strconv.FormatInt(groupID, 10)]
			msg = fmt.Sprintf("%s: global=%t, group(%d)=%t", key, cfg.RegexEnabledGlobal, groupID, groupVal)
		default:
			msg = "不支持的开关，使用 /jm cfg list 查看可用项"
		}
		a.cfgMu.RUnlock()
		a.sendMessage(messageType, groupID, userID, msg)
		return true
	}
	if m := mustMatch(`^/jm\s+cfg\s+set\s+([a-z_]+)\s+(\S+)(?:\s+(global|group))?$`, rawMessage); m != nil {
		if !a.requireAdmin(messageType, groupID, userID, "仅管理员可配置开关") {
			return true
		}
		key := strings.ToLower(strings.TrimSpace(m[1]))
		v, ok := parseSwitchBool(m[2])
		if !ok {
			a.sendMessage(messageType, groupID, userID, "值仅支持 on/off(true/false/1/0)")
			return true
		}
		scope := strings.ToLower(strings.TrimSpace(m[3]))
		if scope == "" {
			scope = "group"
			if messageType != "group" || groupID <= 0 {
				scope = "global"
			}
		}

		a.cfgMu.Lock()
		changed := true
		switch key {
		case "reply_as_card":
			a.cfg.ReplyAsCard = v
		case "cbz_chapter_enabled":
			a.cfg.CBZChapterEnabled = v
		case "cbz_series_enabled":
			a.cfg.CBZSeriesEnabled = v
		case "embedded_bypass_enabled":
			a.cfg.EmbeddedBypassEnable = v
		case "http_port_fallback":
			a.cfg.HTTPPortFallback = v
		case "local_test_mode":
			a.cfg.LocalTestMode = v
		case "local_test_exit_after_selftest":
			a.cfg.LocalTestExitAfterSelftest = v
		case "enc_enabled":
			if scope == "group" && messageType == "group" && groupID > 0 {
				a.cfg.EncEnabledGroup[strconv.FormatInt(groupID, 10)] = v
			} else {
				a.cfg.EncEnabledGlobal = v
			}
		case "randpwd_enabled":
			if scope == "group" && messageType == "group" && groupID > 0 {
				a.cfg.RandomPasswordEnabledGroup[strconv.FormatInt(groupID, 10)] = v
			} else {
				a.cfg.RandomPasswordEnabledGlobal = v
			}
		case "regex_enabled":
			if scope == "group" && messageType == "group" && groupID > 0 {
				a.cfg.RegexEnabledGroup[strconv.FormatInt(groupID, 10)] = v
			} else {
				a.cfg.RegexEnabledGlobal = v
			}
		default:
			changed = false
		}
		a.cfgMu.Unlock()
		if !changed {
			a.sendMessage(messageType, groupID, userID, "不支持的开关，使用 /jm cfg list 查看可用项")
			return true
		}
		a.saveConfig()
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("配置已更新：%s=%t (%s)", key, v, scope))
		return true
	}
	return false
}

func parseSwitchBool(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "on", "true", "1", "yes":
		return true, true
	case "off", "false", "0", "no":
		return false, true
	default:
		return false, false
	}
}

func (a *App) handleDedupCommand(rawMessage, messageType string, groupID, userID int64) bool {
	if !matched(`^/jm\s+dedup(?:\s+.*)?$`, rawMessage) {
		return false
	}
	if !a.requireAdmin(messageType, groupID, userID, "仅管理员可管理重复请求冷却") {
		return true
	}

	usage := "用法：\n" +
		"/jm dedup show\n" +
		"/jm dedup set <秒数|Go时长(如 30m/12h)>\n" +
		"/jm dedup clear <本子ID>"

	if matched(`^/jm\s+dedup\s+show$`, rawMessage) {
		a.cfgMu.RLock()
		seconds := a.cfg.DedupWindow
		a.cfgMu.RUnlock()
		if seconds <= 0 {
			a.sendMessage(messageType, groupID, userID, "当前重复请求冷却：已关闭（0秒）")
			return true
		}
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("当前重复请求冷却：%d秒（%s）", seconds, formatDedupWindow(seconds)))
		return true
	}
	if m := mustMatch(`^/jm\s+dedup\s+set\s+(\S+)$`, rawMessage); m != nil {
		seconds, err := parseDedupWindowSeconds(m[1])
		if err != nil {
			a.sendMessage(messageType, groupID, userID, "冷却时长无效，请使用正整数秒或 Go 时长（例如 1800 / 30m / 12h）")
			return true
		}
		a.cfgMu.Lock()
		a.cfg.DedupWindow = seconds
		a.cfgMu.Unlock()
		a.saveConfig()
		if seconds <= 0 {
			a.sendMessage(messageType, groupID, userID, "重复请求冷却已关闭")
			return true
		}
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("重复请求冷却已设为：%d秒（%s）", seconds, formatDedupWindow(seconds)))
		return true
	}
	if m := mustMatch(`^/jm\s+dedup\s+clear\s+(\d+)$`, rawMessage); m != nil {
		number := strings.TrimSpace(m[1])
		cleared := a.clearRecentRequest(number)
		if cleared == 0 {
			a.sendMessage(messageType, groupID, userID, "该本子当前没有冷却记录：JM"+number)
			return true
		}
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("已清除本子冷却：JM%s（影响作用域 %d 个）", number, cleared))
		return true
	}

	a.sendMessage(messageType, groupID, userID, usage)
	return true
}

func parseDedupWindowSeconds(raw string) (int, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("empty value")
	}
	if n, err := strconv.Atoi(s); err == nil {
		if n < 0 {
			return 0, errors.New("negative seconds")
		}
		return n, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	sec := int(d.Seconds())
	if d > 0 && sec == 0 {
		sec = 1
	}
	if sec < 0 {
		return 0, errors.New("negative duration")
	}
	return sec, nil
}

func formatDedupWindow(seconds int) string {
	if seconds <= 0 {
		return "0秒"
	}
	day := seconds / 86400
	seconds %= 86400
	hour := seconds / 3600
	seconds %= 3600
	min := seconds / 60
	sec := seconds % 60

	parts := make([]string, 0, 4)
	if day > 0 {
		parts = append(parts, fmt.Sprintf("%d天", day))
	}
	if hour > 0 {
		parts = append(parts, fmt.Sprintf("%d小时", hour))
	}
	if min > 0 {
		parts = append(parts, fmt.Sprintf("%d分钟", min))
	}
	if sec > 0 {
		parts = append(parts, fmt.Sprintf("%d秒", sec))
	}
	if len(parts) == 0 {
		return "0秒"
	}
	return strings.Join(parts, "")
}

func (a *App) clearRecentRequest(number string) int {
	number = strings.TrimSpace(number)
	if number == "" {
		return 0
	}
	a.recentMu.Lock()
	defer a.recentMu.Unlock()
	clearedScopes := 0
	for scope, m := range a.recent {
		if m == nil {
			continue
		}
		if _, ok := m[number]; ok {
			delete(m, number)
			clearedScopes++
		}
		if len(m) == 0 {
			delete(a.recent, scope)
		}
	}
	return clearedScopes
}

func (a *App) isJMAllowed(messageType string, groupID, userID int64) bool {
	a.cfgMu.RLock()
	defer a.cfgMu.RUnlock()

	// 私聊始终允许
	if messageType != "group" {
		return true
	}

	// 检查黑名单群
	if contains(a.cfg.BannedGroup, strconv.FormatInt(groupID, 10)) {
		a.sendMessage(messageType, groupID, userID, "该群已被禁止使用")
		return false
	}

	// 检查白名单（如果设置了白名单，只允许白名单中的群）
	if len(a.cfg.AllowedGroup) > 0 && !containsInt64(a.cfg.AllowedGroup, groupID) {
		a.sendMessage(messageType, groupID, userID, "该群未在白名单中")
		return false
	}

	// 检查黑名单用户
	if contains(a.cfg.BannedUser, strconv.FormatInt(userID, 10)) {
		a.sendMessage(messageType, groupID, userID, "禁止下载或用户被封禁")
		return false
	}

	return true
}

func (a *App) getRegexEnabled(messageType string, groupID int64) bool {
	a.cfgMu.RLock()
	defer a.cfgMu.RUnlock()
	if messageType == "group" {
		if v, ok := a.cfg.RegexEnabledGroup[strconv.FormatInt(groupID, 10)]; ok {
			return v
		}
	}
	return a.cfg.RegexEnabledGlobal
}

func (a *App) getStrictMode(messageType string, groupID int64) bool {
	a.cfgMu.RLock()
	defer a.cfgMu.RUnlock()
	if messageType == "group" {
		if v, ok := a.cfg.StrictModeGroup[strconv.FormatInt(groupID, 10)]; ok {
			return v
		}
	}
	return a.cfg.StrictModeGlobal
}

func (a *App) enqueueDownloads(numbers []string, messageType string, groupID, userID int64, data map[string]any) {
	if !a.isJMAllowed(messageType, groupID, userID) {
		return
	}
	cfg := a.currentConfig()
	scope := requestScope(messageType, groupID, userID)
	accepted := make([]string, 0, len(numbers))
	for _, n := range numbers {
		if contains(cfg.BannedID, n) {
			a.sendMessage(messageType, groupID, userID, "禁止下载或用户被封禁")
			continue
		}
		if cfg.DedupWindow > 0 && a.isRecentRequest(scope, n, time.Duration(cfg.DedupWindow)*time.Second) {
			if len(n) >= 4 {
				a.sendMessage(messageType, groupID, userID, fmt.Sprintf("本子 %s 在过去%s内已请求过，已跳过", n, formatDedupWindow(cfg.DedupWindow)))
			}
			continue
		}
		a.markRequest(scope, n)
		accepted = append(accepted, n)
	}
	if len(accepted) == 0 {
		return
	}

	bulkRequested := len(accepted) > 1
	batchID := ""
	if bulkRequested {
		batchID = fmt.Sprintf("bulk_%d_%d_%d", time.Now().UnixNano(), groupID, userID)
	}
	nickname := toString(mapGet(mapGet(data, "sender"), "nickname"))
	queued := 0
	for idx, n := range accepted {
		task := DownloadTask{
			Number:      n,
			MessageType: messageType,
			GroupID:     groupID,
			UserID:      userID,
			Scope:       scope,
			Uploader:    nickname,
			Bulk:        bulkRequested,
			BatchID:     batchID,
			BatchTotal:  len(accepted),
			BatchIndex:  idx,
		}
		select {
		case a.queue <- task:
			queued++
		default:
			a.sendMessage(messageType, groupID, userID, "下载队列已满，请稍后重试")
			return
		}
	}
	if queued > 0 {
		a.sendMessage(messageType, groupID, userID, fmt.Sprintf("已加入队列，正在下载 %d 个本子，当前队列：%d", queued, len(a.queue)))
	}
}

func (a *App) worker() {
	for task := range a.queue {
		a.processTask(task)
	}
}

func (a *App) processTask(task DownloadTask) {
	result := bulkTaskResult{
		BatchIndex: task.BatchIndex,
		Number:     task.Number,
	}
	defer func() {
		if !task.Bulk || strings.TrimSpace(task.BatchID) == "" || task.BatchTotal <= 1 {
			return
		}
		a.finishBulkTask(task, result)
	}()

	notify := func(message string) {
		if task.Bulk && strings.TrimSpace(task.BatchID) != "" && task.BatchTotal > 1 {
			if result.FailMsg == "" {
				result.FailMsg = message
			}
			return
		}
		a.sendMessage(task.MessageType, task.GroupID, task.UserID, message)
	}

	cfg := a.currentConfig()
	sendMode := cfg.SendModeGlobal
	if task.MessageType == "group" {
		if v, ok := cfg.SendModeGroup[strconv.FormatInt(task.GroupID, 10)]; ok {
			sendMode = v
		}
	}
	nameMode := cfg.SendNameModeGlobal
	if task.MessageType == "group" {
		if v, ok := cfg.SendNameModeGroup[strconv.FormatInt(task.GroupID, 10)]; ok {
			nameMode = v
		}
	}
	nameMode = normalizeSendNameMode(nameMode)

	encEnabled := cfg.EncEnabledGlobal
	if task.MessageType == "group" {
		if v, ok := cfg.EncEnabledGroup[strconv.FormatInt(task.GroupID, 10)]; ok {
			encEnabled = v
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.DownloadTimeout)*time.Second)
	defer cancel()

	path := ""
	name := ""
	albumTitle := ""
	needDownload := true
	downloadSource := "JM" // 标记下载来源：JM 或 Bika

	// Reuse locally downloaded files whenever possible.
	if !encEnabled {
		if sendMode == "zip" {
			if book, found, _ := a.findBookByID(task.Number); found && strings.TrimSpace(book.Path) != "" {
				path = book.Path
				name = filepath.Base(book.Path)
				albumTitle = strings.TrimSpace(book.Title)
				needDownload = false
				log.Printf("reuse local cbz for JM%s path=%s", task.Number, path)
			} else {
				// ZIP mode can still reuse local PDF when CBZ is unavailable.
				path, name = findPDF(cfg.FileDir, task.Number, "")
				if path != "" {
					albumTitle = strings.TrimSpace(strings.TrimSuffix(name, filepath.Ext(name)))
					needDownload = false
					log.Printf("reuse local pdf for JM%s path=%s (zip fallback)", task.Number, path)
				}
			}
		} else {
			path, name = findPDF(cfg.FileDir, task.Number, "")
			if path != "" {
				albumTitle = strings.TrimSpace(strings.TrimSuffix(name, filepath.Ext(name)))
				needDownload = false
				log.Printf("reuse local pdf for JM%s path=%s", task.Number, path)
			}
		}
	}
	if encEnabled && path == "" {
		// Encryption mode still prefers existing local PDF to avoid redownloading.
		path, name = findPDF(cfg.FileDir, task.Number, "")
		if path != "" {
			albumTitle = strings.TrimSpace(strings.TrimSuffix(name, filepath.Ext(name)))
			needDownload = false
			log.Printf("reuse local pdf for encrypted JM%s path=%s", task.Number, path)
		}
	}

	// 哔咔升级策略：即使本地有文件，也尝试从哔咔下载原画版
	log.Printf("[Bika] 检查哔咔升级条件: bika=%v, albumTitle='%s'", a.bika != nil, albumTitle)
	if a.bika != nil {
		token := a.getBikaUserToken(task.UserID)
		log.Printf("[Bika] 用户 %d token状态: %v", task.UserID, token != "")
		if token != "" {
			// 如果没有标题，先获取
			if albumTitle == "" || albumTitle == task.Number {
				album2, err2 := a.jm.GetAlbum(ctx, task.Number)
				if err2 == nil && album2 != nil {
					albumTitle = strings.TrimSpace(album2.Title)
					log.Printf("[Bika] 获取到本子标题: %s", albumTitle)
				}
			}

			if albumTitle != "" && albumTitle != task.Number {
				// 构建搜索关键词列表：原始标题、去除[]、去除()、都去除
				searchKeywords := []string{albumTitle}
				cleaned1 := regexp.MustCompile(`\[.*?\]`).ReplaceAllString(albumTitle, "")
				cleaned1 = strings.TrimSpace(cleaned1)
				if cleaned1 != "" && cleaned1 != albumTitle {
					searchKeywords = append(searchKeywords, cleaned1)
				}
				cleaned2 := regexp.MustCompile(`\(.*?\)`).ReplaceAllString(albumTitle, "")
				cleaned2 = strings.TrimSpace(cleaned2)
				if cleaned2 != "" && cleaned2 != albumTitle && cleaned2 != cleaned1 {
					searchKeywords = append(searchKeywords, cleaned2)
				}
				cleaned3 := regexp.MustCompile(`[\[\(].*?[\]\)]`).ReplaceAllString(albumTitle, "")
				cleaned3 = strings.TrimSpace(cleaned3)
				if cleaned3 != "" && cleaned3 != albumTitle && cleaned3 != cleaned1 && cleaned3 != cleaned2 {
					searchKeywords = append(searchKeywords, cleaned3)
				}

				var bestMatch *BikaSearchResult
				for _, keyword := range searchKeywords {
					if keyword == "" {
						continue
					}
					log.Printf("[Bika] 搜索关键词: '%s'", keyword)
					results, _, searchErr := a.bika.Search(keyword, 1, token)
					if searchErr != nil {
						log.Printf("[Bika] 搜索失败: %v", searchErr)
						continue
					}
					log.Printf("[Bika] 搜索结果: %d 条", len(results))
					if len(results) > 0 {
						bestMatch = &results[0]
						log.Printf("[Bika] 找到匹配: '%s' (ID: %s)", bestMatch.Title, bestMatch.ID)
						break
					}
				}

				if bestMatch != nil {
					// 找到匹配的哔咔漫画，使用哔咔下载
					log.Printf("[Bika] 开始从哔咔下载: %s", bestMatch.ID)
					if !(task.Bulk && strings.TrimSpace(task.BatchID) != "" && task.BatchTotal > 1) {
						a.sendMessage(task.MessageType, task.GroupID, task.UserID, fmt.Sprintf("哔咔升级：找到原画版 %s，正在从哔咔下载...", bestMatch.Title))
					}
					bikaCBZ, bikaErr := a.bikaDownloadComic(ctx, bestMatch.ID, "", task.MessageType, task.GroupID, task.UserID, token, cfg.BikaQuality)
					if bikaErr != nil {
						log.Printf("[Bika] 下载失败: %v", bikaErr)
					}
					if bikaErr == nil && bikaCBZ != "" {
						// 哔咔下载成功，使用哔咔的文件
						path = bikaCBZ
						name = filepath.Base(bikaCBZ)
						albumTitle = bestMatch.Title
						needDownload = false
						downloadSource = "Bika"
						log.Printf("[Bika] 下载成功: %s", bikaCBZ)
					} else {
						log.Printf("[Bika] 下载失败，回退到JM: %v", bikaErr)
					}
				} else {
					log.Printf("[Bika] 未找到匹配，使用JM下载")
				}
			} else {
				log.Printf("[Bika] 标题为空或等于JM号，跳过哔咔升级")
			}
		} else {
			log.Printf("[Bika] 用户未登录，跳过哔咔升级")
		}
	} else {
		log.Printf("[Bika] 哔咔未启用，跳过哔咔升级")
	}

	if needDownload {
		path, name = findPDF(cfg.FileDir, task.Number, "")
		if path == "" {
			// 需要获取album信息
			album, err := a.jm.GetAlbum(ctx, task.Number)
			if err != nil || album == nil {
				if len(task.Number) < 4 {
					return
				}
				reason := "未能成功下载（可能ID错误或网络失败）"
				if err != nil {
					reason = fmt.Sprintf("获取本子信息失败: %v", err)
				}
				notify(reason)
				a.notifyAdminDownloadFailure(task.GroupID, task.Number, reason)
				return
			}
			if album.Episodes > cfg.MaxEpisodes {
				notify(fmt.Sprintf("本子章节过多(>%d)", cfg.MaxEpisodes))
				return
			}
			if albumTitle == "" {
				albumTitle = strings.TrimSpace(album.Title)
			}
			if !(task.Bulk && strings.TrimSpace(task.BatchID) != "" && task.BatchTotal > 1) {
				a.sendMessage(task.MessageType, task.GroupID, task.UserID, "正在下载本子 "+task.Number)
			}
			if err := a.jm.Download(ctx, task.Number); err != nil {
				reason := fmt.Sprintf("下载失败或超时: %v", err)
				notify("下载失败或超时")
				a.notifyAdminDownloadFailure(task.GroupID, task.Number, reason)
				return
			}
			path, name = findPDF(cfg.FileDir, task.Number, album.Title)
			if path == "" {
				reason := "下载完成但未找到PDF文件"
				notify(reason)
				a.notifyAdminDownloadFailure(task.GroupID, task.Number, reason)
				return
			}
		}
	}
	if strings.TrimSpace(albumTitle) == "" {
		albumTitle = "JM" + task.Number
	}

	sendPath := path
	cleanup := []string{}

	password := ""
	randomPasswordEnabled := cfg.RandomPasswordEnabledGlobal
	if task.MessageType == "group" {
		if v, ok := cfg.RandomPasswordEnabledGroup[strconv.FormatInt(task.GroupID, 10)]; ok {
			randomPasswordEnabled = v
		}
	}
	if randomPasswordEnabled {
		password = randomPassword(cfg.RandomPasswordLength)
	} else {
		password = strings.TrimSpace(cfg.EncPasswordGlobal)
		if task.MessageType == "group" {
			if v, ok := cfg.EncPasswordGroup[strconv.FormatInt(task.GroupID, 10)]; ok {
				password = strings.TrimSpace(v)
			}
		}
	}
	if encEnabled {
		if password == "" {
			notify("未设置加密密码，请先使用 /jm passwd <密码> 设置")
			return
		}
		encOut := filepath.Join(a.tmpDir(), fmt.Sprintf("enc_%s_%d.pdf", task.Number, time.Now().UnixNano()))
		encrypted := false
		if strings.EqualFold(filepath.Ext(sendPath), ".pdf") && fileExists(sendPath) {
			if err := encryptPDFWithQPDF(sendPath, encOut, password); err == nil {
				encrypted = true
			} else {
				log.Printf("local qpdf encryption failed for JM%s, fallback to remote: %v", task.Number, err)
				if rebuilt, rebuildErr := a.buildEncryptedPDFFromLocalManga(task.Number, encOut, password); rebuildErr == nil && rebuilt {
					encrypted = true
					log.Printf("local manga encryption fallback success for JM%s path=%s", task.Number, encOut)
				} else if rebuildErr != nil {
					log.Printf("local manga encryption fallback failed for JM%s: %v", task.Number, rebuildErr)
				}
			}
		}
		if !encrypted {
			if err := a.jm.DownloadTo(ctx, task.Number, encOut, password); err != nil {
				reason := fmt.Sprintf("文件加密失败: %v", err)
				notify("文件加密失败")
				a.notifyAdminDownloadFailure(task.GroupID, task.Number, reason)
				_ = os.Remove(encOut)
				return
			}
		}
		cleanup = append(cleanup, encOut)
		sendPath = encOut
	}

	if sendMode == "zip" && !strings.EqualFold(filepath.Ext(sendPath), ".zip") && !strings.EqualFold(filepath.Ext(sendPath), ".cbz") {
		zipPath, err := buildZip(sendPath, a.tmpDir())
		if err != nil {
			reason := fmt.Sprintf("文件压缩失败: %v", err)
			notify("文件压缩失败")
			a.notifyAdminDownloadFailure(task.GroupID, task.Number, reason)
			_ = os.Remove(zipPath)
			return
		}
		cleanup = append(cleanup, zipPath)
		sendPath = zipPath
	}

	baseName := sanitizeFileName(albumTitle)
	// JM下载的文件加 jmxxxx_ 前缀，Bika下载的用本子名
	if downloadSource == "JM" {
		baseName = fmt.Sprintf("JM%s_%s", task.Number, baseName)
	}
	if encEnabled && strings.TrimSpace(password) != "" {
		baseName = fmt.Sprintf("%s_%s", baseName, sanitizeFileName(password))
	}
	if nameMode == "jm" || nameMode == "full" || downloadSource == "JM" {
		renamed, renamedCleanup, err := cloneWithName(sendPath, baseName, a.tmpDir())
		if err == nil && renamed != sendPath {
			sendPath = renamed
			if renamedCleanup {
				cleanup = append(cleanup, renamed)
			}
		}
	}
	if nameMode == "current" && downloadSource != "JM" {
		ext := strings.ToLower(filepath.Ext(sendPath))
		if ext == ".zip" || ext == ".cbz" {
			hashPath, hashCleanup, err := randomizeHash(sendPath, a.tmpDir())
			if err == nil && hashCleanup {
				sendPath = hashPath
				cleanup = append(cleanup, hashPath)
			}
		}
	}

	sizeMB := fileSizeMB(sendPath)
	label := "PDF"
	if strings.HasSuffix(strings.ToLower(sendPath), ".zip") || strings.HasSuffix(strings.ToLower(sendPath), ".cbz") {
		label = "ZIP"
	}

	// 基本信息消息
	infoMsg := fmt.Sprintf("车牌号：%s\n本子名：%s\n来源：%s\n文件类型：%s\n文件大小：(%.2fMB)", task.Number, albumTitle, downloadSource, label, sizeMB)
	if encEnabled {
		infoMsg += "\n密码：" + password
	}

	// 获取封面路径
	coverPath := ""
	if mangaPath, ok, _ := a.findMangaPageByID(normalizeJMID(task.Number), 1); ok && fileExists(mangaPath) {
		coverPath = mangaPath
	}

	// 发送函数
	sendFunc := func() bool {
		return a.sendComicForwardMessage(task.MessageType, task.GroupID, task.UserID, infoMsg, coverPath, sendPath, cfg)
	}

	if task.Bulk && strings.TrimSpace(task.BatchID) != "" && task.BatchTotal > 1 {
		result.Message = infoMsg
		result.CoverPath = coverPath
		result.FilePath = sendPath
		result.OrigPDF = path
		result.Cleanup = append(result.Cleanup, cleanup...)
	} else {
		ok := sendFunc()
		if !ok {
			failMsg := "文件发送失败"
			a.sendMessage(task.MessageType, task.GroupID, task.UserID, failMsg)
			// 通知管理员
			a.notifyAdminSendFailure(task.GroupID, task.Number, albumTitle, sendPath)
		}
		// 发送完成后立即删除所有临时文件
		for _, c := range cleanup {
			_ = os.Remove(c)
		}
		if ok {
			// 发送成功，删除原始PDF和发送文件
			if path != "" && fileExists(path) {
				_ = os.Remove(path)
				log.Printf("deleted original file: %s", path)
			}
			if sendPath != "" && fileExists(sendPath) && sendPath != path {
				_ = os.Remove(sendPath)
				log.Printf("deleted sent file: %s", sendPath)
			}
			// 删除manga目录
			a.deleteMangaDirByID(normalizeJMID(task.Number))
		}
	}
	_ = name
}

func encryptPDFWithQPDF(inFile, outFile, password string) error {
	if strings.TrimSpace(inFile) == "" || strings.TrimSpace(outFile) == "" || strings.TrimSpace(password) == "" {
		return errors.New("invalid encrypt args")
	}
	if !fileExists(inFile) {
		return fmt.Errorf("input file not found: %s", inFile)
	}
	if _, err := exec.LookPath("qpdf"); err != nil {
		return err
	}
	cmd := exec.Command("qpdf", "--encrypt", password, password, "256", "--", inFile, outFile)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("qpdf encrypt failed: %v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (a *App) buildEncryptedPDFFromLocalManga(number, outFile, password string) (bool, error) {
	id := normalizeJMID(number)
	if id == "" || strings.TrimSpace(outFile) == "" || strings.TrimSpace(password) == "" {
		return false, errors.New("invalid args")
	}
	pages, ok, err := a.listMangaPagesByID(id)
	if err != nil {
		return false, err
	}
	if !ok || len(pages) == 0 {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil {
		return false, err
	}
	if err := buildPDF(outFile, pages, password); err != nil {
		return false, err
	}
	return true, nil
}

func (a *App) deleteMangaDirByID(id string) {
	cfg := a.currentConfig()
	root := strings.TrimSpace(cfg.MangaDir)
	if root == "" {
		root = "./manga/"
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		dirID := extractIDFromName(name)
		if dirID == id {
			dirPath := filepath.Join(root, name)
			if err := os.RemoveAll(dirPath); err != nil {
				log.Printf("failed to delete manga dir %s: %v", dirPath, err)
			} else {
				log.Printf("deleted manga dir: %s", dirPath)
			}
		}
	}
}

// downloadJMCover 从JM API下载第一章封面图片到临时文件
func (a *App) downloadJMCover(albumID string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 获取第一章信息
	data, err := a.jm.reqAPI(ctx, "/chapter", map[string]string{"id": albumID})
	if err != nil {
		log.Printf("[Cover] 获取章节信息失败: %v", err)
		return ""
	}

	images := anyToStringSlice(data["images"])
	if len(images) == 0 {
		return ""
	}

	// 下载第一张图片
	imgBytes, err := a.jm.downloadImage(ctx, albumID, images[0])
	if err != nil {
		log.Printf("[Cover] 下载封面图片失败: %v", err)
		return ""
	}

	// 获取章节ID和scrambleID，用于解密图片
	photoID := toJMID(anyToString(data["id"]))
	if photoID == "" {
		photoID = toJMID(albumID)
	}
	scrambleID, _ := a.jm.fetchScrambleID(ctx, photoID)
	if scrambleID == "" {
		scrambleID = strconv.Itoa(jmFallbackScramble)
	}
	num := calcSegmentationNum(scrambleID, photoID, trimExt(images[0]))
	if num > 0 {
		// 解码图片
		decoded, format, decErr := image.Decode(bytesReader(imgBytes))
		if decErr != nil {
			log.Printf("[Cover] 图片解码失败: %v", decErr)
		} else {
			// 解密图片
			unscrambled := unscrambleImage(decoded, num)
			// 重新编码
			var buf bytes.Buffer
			var encErr error
			if format == "png" {
				encErr = png.Encode(&buf, unscrambled)
			} else {
				encErr = jpeg.Encode(&buf, unscrambled, &jpeg.Options{Quality: 85})
			}
			if encErr != nil {
				log.Printf("[Cover] 图片编码失败: %v", encErr)
			} else {
				imgBytes = buf.Bytes()
			}
		}
	}

	// 保存到临时文件
	ext := ".jpg"
	if strings.HasSuffix(strings.ToLower(images[0]), ".png") {
		ext = ".png"
	}
	tmpPath := filepath.Join(a.tmpDir(), fmt.Sprintf("jm_cover_%s%s", albumID, ext))
	if err := os.WriteFile(tmpPath, imgBytes, 0644); err != nil {
		return ""
	}
	return tmpPath
}

func (a *App) notifyAdminDownloadFailure(groupID int64, jmNumber, reason string) {
	cfg := a.currentConfig()
	adminID := cfg.AdminID
	if adminID == 0 {
		return
	}
	now := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf("【下载失败通知】\n群号: %d\nJM号: %s\n申请时间: %s\n失败原因: %s", groupID, jmNumber, now, reason)
	_ = a.bot.SendPrivateMessage(adminID, msg)
}

func (a *App) notifyAdminSendFailure(groupID int64, jmNumber, title, filePath string) {
	cfg := a.currentConfig()
	adminID := cfg.AdminID
	if adminID == 0 {
		return
	}
	now := time.Now().Format("2006-01-02 15:04:05")
	fileName := filepath.Base(filePath)
	fileSize := fileSizeMB(filePath)
	msg := fmt.Sprintf("【发送失败通知】\n群号: %d\n本子号: %s\n本子名: %s\n文件名: %s\n文件大小: %.2fMB\n时间: %s",
		groupID, jmNumber, title, fileName, fileSize, now)
	_ = a.bot.SendPrivateMessage(adminID, msg)
}

// sendComicForwardMessage keeps the visible chat compact by sending metadata,
// cover and preview link as a merged forward card, then uploads the comic as a
// real QQ file. Comic files inside merged forwards can point to transient NapCat
// paths and become unopenable in chat history.
func (a *App) sendComicForwardMessage(messageType string, groupID, userID int64, infoMsg, coverPath, filePath string, cfg Config) bool {
	// 转发卡片失败时回退到普通文本消息+文件
	infoOK := a.sendComicInfoForwardMessage(messageType, groupID, userID, infoMsg, coverPath, cfg)
	if !infoOK {
		a.sendMessage(messageType, groupID, userID, infoMsg)
		// 转发失败时单独发送封面PDF
		if coverPath != "" && fileExists(coverPath) {
			pdfPath := filepath.Join(a.tmpDir(), fmt.Sprintf("cover_%d.pdf", time.Now().UnixNano()))
			if err := imageToPDF(coverPath, pdfPath); err == nil {
				if messageType == "group" && groupID > 0 {
					a.bot.SendGroupFile(cfg, groupID, pdfPath)
				} else if messageType == "private" && userID > 0 {
					a.bot.SendPrivateFile(cfg, userID, pdfPath)
				}
				// 删除临时封面PDF
				_ = os.Remove(pdfPath)
			}
		}
	}

	// 如果有文件则发送文件
	if filePath != "" && fileExists(filePath) {
		if messageType == "group" && groupID > 0 {
			return a.bot.SendGroupFile(cfg, groupID, filePath)
		}
		if messageType == "private" && userID > 0 {
			return a.bot.SendPrivateFile(cfg, userID, filePath)
		}
	}
	return infoOK
}

func (a *App) sendComicInfoForwardMessage(messageType string, groupID, userID int64, infoMsg, coverPath string, cfg Config) bool {
	senderID := cfg.CardUserID
	nickname := cfg.CardNickname
	if senderID <= 0 {
		senderID = 10000
	}
	if strings.TrimSpace(nickname) == "" {
		nickname = "文件助手"
	}

	text := strings.TrimSpace(infoMsg)
	if text == "" {
		text = "文件信息"
	}

	nodes := []map[string]any{{
		"type": "node",
		"data": map[string]any{
			"user_id":  senderID,
			"nickname": nickname,
			"content":  []map[string]any{{"type": "text", "data": map[string]any{"text": text}}},
		},
	}}

	if coverPath != "" && fileExists(coverPath) {
		// 把封面图片转换为PDF
		pdfPath := filepath.Join(a.tmpDir(), fmt.Sprintf("cover_%d.pdf", time.Now().UnixNano()))
		if err := imageToPDF(coverPath, pdfPath); err == nil {
			if pf, err := a.bot.prepareForwardFile(cfg, pdfPath); err == nil && len(pf.candidates) > 0 {
				if pf.cleanup != nil {
					defer pf.cleanup()
				}
				nodes = append(nodes, map[string]any{
					"type": "node",
					"data": map[string]any{
						"user_id":  senderID,
						"nickname": nickname,
						"content":  []map[string]any{{"type": "file", "data": map[string]any{"file": pf.candidates[0]}}},
					},
				})
			}
			// 延迟删除临时封面PDF
			defer func() {
				if fileExists(pdfPath) {
					_ = os.Remove(pdfPath)
				}
			}()
		}
	}

	var action string
	var baseParams map[string]any
	if messageType == "group" && groupID > 0 {
		action = "send_group_forward_msg"
		baseParams = map[string]any{"group_id": groupID}
	} else if messageType == "private" && userID > 0 {
		action = "send_private_forward_msg"
		baseParams = map[string]any{"user_id": userID}
	} else {
		return false
	}

	sent := false
	for retry := 0; retry < 3; retry++ {
		params := copyMap(baseParams)
		params["message"] = nodes
		_, err := a.bot.send(action, params, echo("forward_comic_info", groupID), 60*time.Second)
		if err == nil {
			sent = true
			break
		}
		log.Printf("[ForwardInfo] 发送失败 (重试%d/3): %v", retry+1, err)
		time.Sleep(2 * time.Second)
	}

	return sent
}

func (a *App) sendMessage(messageType string, groupID, userID int64, message string) {
	if messageType == "group" && groupID > 0 {
		_ = a.bot.SendGroupMessage(groupID, message)
		return
	}
	if messageType == "private" && userID > 0 {
		_ = a.bot.SendPrivateMessage(userID, message)
	}
}

func (a *App) sendRecordMessage(messageType string, groupID, userID int64, message string) {
	a.sendMessage(messageType, groupID, userID, message)
}

func (a *App) sendBulkRecordMessage(messageType string, groupID, userID int64, message string) {
	cfg := a.currentConfig()
	if !cfg.ReplyAsCard {
		a.sendMessage(messageType, groupID, userID, message)
		return
	}
	if messageType == "group" && groupID > 0 {
		if a.bot.SendGroupForwardCardMessage(groupID, message, cfg.CardUserID, cfg.CardNickname) {
			return
		}
	}
	if messageType == "private" && userID > 0 {
		if a.bot.SendPrivateForwardCardMessage(userID, message, cfg.CardUserID, cfg.CardNickname) {
			return
		}
	}
	a.sendMessage(messageType, groupID, userID, message)
}

func (a *App) finishBulkTask(task DownloadTask, result bulkTaskResult) {
	batchID := strings.TrimSpace(task.BatchID)
	if batchID == "" || task.BatchTotal <= 1 {
		return
	}

	a.bulkMu.Lock()
	st := a.bulkStates[batchID]
	if st == nil {
		st = &bulkBatchState{
			MessageType: task.MessageType,
			GroupID:     task.GroupID,
			UserID:      task.UserID,
			Total:       task.BatchTotal,
			Results:     make([]bulkTaskResult, 0, task.BatchTotal),
		}
		a.bulkStates[batchID] = st
	}
	st.Results = append(st.Results, result)
	done := len(st.Results) >= st.Total
	if done {
		delete(a.bulkStates, batchID)
	}
	a.bulkMu.Unlock()

	if !done {
		return
	}
	a.flushBulkBatch(st)
}

func (a *App) flushBulkBatch(st *bulkBatchState) {
	if st == nil || len(st.Results) == 0 {
		return
	}
	sort.Slice(st.Results, func(i, j int) bool {
		return st.Results[i].BatchIndex < st.Results[j].BatchIndex
	})

	cfg := a.currentConfig()

	// 收集成功和失败的结果
	okResults := make([]bulkTaskResult, 0)
	failMessages := make([]string, 0)
	for _, r := range st.Results {
		if r.FilePath == "" {
			if strings.TrimSpace(r.FailMsg) != "" {
				failMessages = append(failMessages, fmt.Sprintf("JM%s：%s", r.Number, r.FailMsg))
			}
			continue
		}
		okResults = append(okResults, r)
	}

	// 将所有成功的漫画合并为一个转发消息发送
	if len(okResults) > 0 {
		sendOK := a.sendBulkComicForwardMessage(st.MessageType, st.GroupID, st.UserID, okResults, cfg)
		if !sendOK {
			// 合并发送失败，逐个发送
			log.Printf("[BulkFlush] 合并转发失败，回退到逐个发送")
			for _, r := range okResults {
				singleOK := a.sendComicForwardMessage(st.MessageType, st.GroupID, st.UserID, r.Message, r.CoverPath, r.FilePath, cfg)
				if !singleOK {
					failMessages = append(failMessages, fmt.Sprintf("JM%s：文件发送失败", r.Number))
					a.notifyAdminSendFailure(st.GroupID, r.Number, "", r.FilePath)
				}
			}
		}
	}

	// 发送批量结果摘要（仅在有失败时）
	if len(failMessages) > 0 {
		summary := fmt.Sprintf("批量发送结果：成功 %d/%d\n\n%s", len(okResults), len(st.Results), strings.Join(failMessages, "\n"))
		a.sendMessage(st.MessageType, st.GroupID, st.UserID, summary)
	}

	// 清理临时文件
	cleanup := make([]string, 0, len(st.Results)*2)
	for _, r := range st.Results {
		cleanup = append(cleanup, r.Cleanup...)
	}
	for _, c := range cleanup {
		_ = os.Remove(c)
	}

	// 批量发送完成后立即删除所有文件
	for _, r := range st.Results {
		if r.FilePath != "" {
			if r.OrigPDF != "" && fileExists(r.OrigPDF) {
				_ = os.Remove(r.OrigPDF)
				log.Printf("deleted original file: %s", r.OrigPDF)
			}
			if fileExists(r.FilePath) && r.FilePath != r.OrigPDF {
				_ = os.Remove(r.FilePath)
				log.Printf("deleted sent file: %s", r.FilePath)
			}
			a.deleteMangaDirByID(normalizeJMID(r.Number))
		}
	}
}

func (a *App) sendBulkComicForwardMessage(messageType string, groupID, userID int64, results []bulkTaskResult, cfg Config) bool {
	senderID := cfg.CardUserID
	nickname := cfg.CardNickname
	if senderID <= 0 {
		senderID = 10000
	}
	if strings.TrimSpace(nickname) == "" {
		nickname = "文件助手"
	}

	// 准备所有文件（封面和漫画文件）
	type preparedItem struct {
		result   bulkTaskResult
		coverPF  preparedForwardFile
		filePF   preparedForwardFile
	}
	items := make([]preparedItem, 0, len(results))
	defer func() {
		for _, it := range items {
			if it.coverPF.cleanup != nil {
				it.coverPF.cleanup()
			}
			if it.filePF.cleanup != nil {
				it.filePF.cleanup()
			}
		}
	}()

	for _, r := range results {
		it := preparedItem{result: r}

		// 准备封面
		if r.CoverPath != "" && fileExists(r.CoverPath) {
			resizedPath := a.resizeImageTo210p(r.CoverPath)
			coverToSend := r.CoverPath
			if resizedPath != "" && fileExists(resizedPath) && resizedPath != r.CoverPath {
				coverToSend = resizedPath
			}
			if pf, err := a.bot.prepareForwardFile(cfg, coverToSend); err == nil && len(pf.candidates) > 0 {
				it.coverPF = pf
			}
		}

		// 准备漫画文件
		if r.FilePath != "" && fileExists(r.FilePath) {
			if pf, err := a.bot.prepareForwardFile(cfg, r.FilePath); err == nil && len(pf.candidates) > 0 {
				it.filePF = pf
			}
		}

		items = append(items, it)
	}

	// 构建转发消息节点：总览 + 每个漫画的(文本+封面+文件)
	// 计算候选路径最大数量以支持重试
	maxCandidates := 1
	for _, it := range items {
		if len(it.coverPF.candidates) > maxCandidates {
			maxCandidates = len(it.coverPF.candidates)
		}
		if len(it.filePF.candidates) > maxCandidates {
			maxCandidates = len(it.filePF.candidates)
		}
	}

	var action string
	var baseParams map[string]any
	if messageType == "group" && groupID > 0 {
		action = "send_group_forward_msg"
		baseParams = map[string]any{"group_id": groupID}
	} else if messageType == "private" && userID > 0 {
		action = "send_private_forward_msg"
		baseParams = map[string]any{"user_id": userID}
	} else {
		return false
	}

	for idx := 0; idx < maxCandidates; idx++ {
		nodes := make([]map[string]any, 0, len(items)*3+1)

		// 总览节点
		summaryText := fmt.Sprintf("批量下载完成，共 %d 个本子", len(results))
		nodes = append(nodes, map[string]any{
			"type": "node",
			"data": map[string]any{
				"user_id":  senderID,
				"nickname": nickname,
				"content":  []map[string]any{{"type": "text", "data": map[string]any{"text": summaryText}}},
			},
		})

		for _, it := range items {
			// 文本信息节点
			nodes = append(nodes, map[string]any{
				"type": "node",
				"data": map[string]any{
					"user_id":  senderID,
					"nickname": nickname,
					"content":  []map[string]any{{"type": "text", "data": map[string]any{"text": it.result.Message}}},
				},
			})

			// 封面图节点
			if len(it.coverPF.candidates) > 0 {
				coverRef := it.coverPF.candidates[len(it.coverPF.candidates)-1]
				if idx < len(it.coverPF.candidates) {
					coverRef = it.coverPF.candidates[idx]
				}
				nodes = append(nodes, map[string]any{
					"type": "node",
					"data": map[string]any{
						"user_id":  senderID,
						"nickname": nickname,
						"content":  []map[string]any{{"type": "image", "data": map[string]any{"file": coverRef}}},
					},
				})
			}

			// 文件节点
			if len(it.filePF.candidates) > 0 {
				fileRef := it.filePF.candidates[len(it.filePF.candidates)-1]
				if idx < len(it.filePF.candidates) {
					fileRef = it.filePF.candidates[idx]
				}
				nodes = append(nodes, map[string]any{
					"type": "node",
					"data": map[string]any{
						"user_id":  senderID,
						"nickname": nickname,
						"content":  []map[string]any{{"type": "file", "data": map[string]any{"file": fileRef}}},
					},
				})
			}
		}

		params := copyMap(baseParams)
		params["message"] = nodes
		if _, err := a.bot.send(action, params, echo("forward_bulk_comics", groupID), 600*time.Second); err == nil {
			return true
		}
		log.Printf("[BulkForward] 发送失败 (候选%d/%d)", idx+1, maxCandidates)
	}

	return false
}

func (a *App) currentConfig() Config {
	a.cfgMu.RLock()
	defer a.cfgMu.RUnlock()
	cp := *a.cfg
	return cp
}

func (a *App) tmpDir() string {
	cfg := a.currentConfig()
	if d := strings.TrimSpace(cfg.TmpDir); d != "" {
		return d
	}
	return "./tmp"
}

func (a *App) isRecentRequest(scope, number string, window time.Duration) bool {
	a.recentMu.Lock()
	defer a.recentMu.Unlock()
	now := time.Now()
	m := a.recent[scope]
	if m == nil {
		return false
	}
	for k, t := range m {
		if now.Sub(t) > window {
			delete(m, k)
		}
	}
	t, ok := m[number]
	if !ok {
		return false
	}
	return now.Sub(t) <= window
}

func (a *App) markRequest(scope, number string) {
	a.recentMu.Lock()
	defer a.recentMu.Unlock()
	if a.recent[scope] == nil {
		a.recent[scope] = map[string]time.Time{}
	}
	a.recent[scope][number] = time.Now()
}

func (a *App) handleSoutuArmingCommand(rawMessage, messageType string, groupID, userID int64, scope string, compatScope string) bool {
	if !(matched(`^/jm\s+search$`, rawMessage) || matched(`^识图$`, rawMessage) || matched(`^/jm识图$`, rawMessage) || matched(`^/jm\s+识图$`, rawMessage)) {
		return false
	}
	cfg := a.currentConfig()
	a.soutuMu.Lock()
	a.soutuArmed[scope] = time.Now().Add(time.Duration(cfg.SoutuTriggerWindow) * time.Second)
	if compatScope != "" && compatScope != scope {
		a.soutuArmed[compatScope] = a.soutuArmed[scope]
	}
	a.soutuMu.Unlock()
	a.sendMessage(messageType, groupID, userID, fmt.Sprintf("已进入识图模式，请在 %d 秒内发送图片", cfg.SoutuTriggerWindow))
	return true
}

func (a *App) tryHandleSoutuImage(data map[string]any, messageType string, groupID, userID int64, scope string, compatScope string) bool {
	cfg := a.currentConfig()
	now := time.Now()
	a.soutuMu.Lock()
	deadline, ok := a.soutuArmed[scope]
	if !ok && compatScope != "" && compatScope != scope {
		deadline, ok = a.soutuArmed[compatScope]
	}
	if ok && now.After(deadline) {
		delete(a.soutuArmed, scope)
		if compatScope != "" && compatScope != scope {
			delete(a.soutuArmed, compatScope)
		}
		ok = false
	}
	if !ok {
		a.soutuMu.Unlock()
		if isSoutuRelatedEvent("", data) {
		}
		return false
	}

	sources := extractSoutuImageSourcesFromEvent(data)
	if len(sources) == 0 {
		refs := extractSoutuImageFileRefsFromEvent(data)
		for _, ref := range refs {
			u, err := a.bot.GetImageURL(ref)
			if err != nil {
				log.Printf("get_image failed for %s: %v", ref, err)
				continue
			}
			u = strings.TrimSpace(htmlUnescape(u))
			if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
				sources = append(sources, SoutuImageSource{ImageURL: u})
			}
		}
	}
	if len(sources) == 0 {
		a.soutuMu.Unlock()
		if hasUnsupportedSoutuImageRef(data) {
			a.sendMessage(messageType, groupID, userID, "检测到本地图片路径(file://)或HTML图片标签，机器人无法直接读取。请直接发送QQ图片（不要粘贴本地路径/HTML）。")
			return true
		}
		return false
	}
	delete(a.soutuArmed, scope)
	if compatScope != "" && compatScope != scope {
		delete(a.soutuArmed, compatScope)
	}
	a.soutuMu.Unlock()

	a.sendMessage(messageType, groupID, userID, "正在识图，请稍候...")
	var lastErr error
	for _, src := range sources {
		result, err := searchSoutuBySource(src, cfg)
		if err != nil {
			lastErr = err
			log.Printf("soutu failed for source=%s: %v", src.Desc(), err)
			continue
		}
		a.sendMessage(messageType, groupID, userID, formatSoutuResult(result))
		searchScope := requestScope(messageType, groupID, userID)
		a.tryAutoSearchFromSoutuResult(result, searchScope, messageType, groupID, userID)
		return true
	}
	if lastErr != nil {
		a.sendMessage(messageType, groupID, userID, "识图失败："+briefError(lastErr))
	}
	return true
}

func soutuSourceDescs(sources []SoutuImageSource) []string {
	out := make([]string, 0, len(sources))
	for _, s := range sources {
		out = append(out, s.Desc())
	}
	return out
}

func summarizeMessageForDebug(data map[string]any) string {
	msg, ok := data["message"].([]any)
	if !ok {
		raw := strings.TrimSpace(toString(data["raw_message"]))
		if len(raw) > 80 {
			raw = raw[:80] + "..."
		}
		return "raw:" + raw
	}
	parts := make([]string, 0, len(msg))
	for _, seg := range msg {
		m, ok := seg.(map[string]any)
		if !ok {
			continue
		}
		typ := toString(m["type"])
		dataMap := mapGet(m, "data")
		if typ == "image" {
			u := strings.TrimSpace(toString(dataMap["url"]))
			f := strings.TrimSpace(toString(dataMap["file"]))
			b64 := strings.TrimSpace(toString(dataMap["base64"]))
			parts = append(parts, fmt.Sprintf("image(url=%t,file=%q,base64=%t)", u != "", f, b64 != ""))
			continue
		}
		if typ == "text" {
			t := strings.TrimSpace(toString(dataMap["text"]))
			if len(t) > 30 {
				t = t[:30] + "..."
			}
			parts = append(parts, fmt.Sprintf("text(%q)", t))
			continue
		}
		parts = append(parts, typ)
	}
	return strings.Join(parts, "; ")
}

func isSoutuRelatedEvent(rawMessage string, data map[string]any) bool {
	raw := strings.TrimSpace(strings.ToLower(rawMessage))
	if raw == "/jm search" || raw == "识图" || raw == "/jm识图" || raw == "/jm 识图" {
		return true
	}
	if msg, ok := data["message"].([]any); ok {
		for _, seg := range msg {
			m, ok := seg.(map[string]any)
			if ok && toString(m["type"]) == "image" {
				return true
			}
		}
	}
	r := strings.ToLower(toString(data["raw_message"]))
	return strings.Contains(r, "[cq:image") || strings.Contains(r, "<img") || strings.Contains(r, "file://")
}

func (a *App) tryAutoSearchFromSoutuResult(result map[string]any, scope, messageType string, groupID, userID int64) {
	title := soutuTopTitle(result)
	if title == "" {
		return
	}
	keywords := deriveSoutuSearchKeywords(title)
	for _, keyword := range keywords {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		al, err := a.jm.SearchBestAlbum(ctx, keyword)
		cancel()
		if err != nil || al == nil {
			continue
		}
		a.searchMu.Lock()
		a.search[scope] = PendingSearch{AlbumID: al.ID, Title: al.Title, At: time.Now()}
		a.searchMu.Unlock()
		a.sendRecordMessage(
			messageType,
			groupID,
			userID,
			fmt.Sprintf("识图联动检索：\n搜图标题：%s\n检索关键词：%s\n命中：JM%s\n标题：%s\n是否下载？请在10分钟内回复“确认”", title, keyword, al.ID, al.Title),
		)
		return
	}
}

func soutuTopTitle(result map[string]any) string {
	data, ok := result["data"].([]any)
	if !ok || len(data) == 0 {
		return ""
	}
	top, ok := data[0].(map[string]any)
	if !ok {
		return ""
	}
	return strings.TrimSpace(toString(top["title"]))
}

func deriveSoutuSearchKeywords(title string) []string {
	raw := strings.TrimSpace(htmlUnescape(title))
	if raw == "" {
		return nil
	}

	out := make([]string, 0, 4)
	add := func(s string) {
		s = strings.TrimSpace(s)
		if len([]rune(s)) < 2 {
			return
		}
		out = append(out, s)
	}

	bracketRe := regexp.MustCompile(`\[[^\]]*\]|\([^\)]*\)`)
	clean := strings.Join(strings.Fields(bracketRe.ReplaceAllString(raw, " ")), " ")
	clean = normalizeSearchKeyword(clean)
	add(clean)

	langTagRe := regexp.MustCompile(`(?i)\b(chinese|english|japanese|korean|翻訳|翻译|汉化|中文版|中国語)\b`)
	clean2 := strings.Join(strings.Fields(langTagRe.ReplaceAllString(clean, " ")), " ")
	add(clean2)

	cjkRe := regexp.MustCompile(`[\p{Han}\p{Hiragana}\p{Katakana}ー・々〆ヵヶ]{2,}`)
	cjkParts := cjkRe.FindAllString(raw, -1)
	if len(cjkParts) > 0 {
		longest := cjkParts[0]
		for _, p := range cjkParts[1:] {
			if len([]rune(p)) > len([]rune(longest)) {
				longest = p
			}
		}
		add(longest)
		joined := strings.Join(cjkParts, " ")
		add(strings.Join(strings.Fields(joined), " "))
	}

	return unique(out)
}

func requestScope(messageType string, groupID, userID int64) string {
	if messageType == "group" && groupID > 0 {
		return fmt.Sprintf("group:%d:user:%d", groupID, userID)
	}
	return fmt.Sprintf("private:%d", userID)
}

func requestSoutuScope(messageType string, groupID, userID int64) string {
	if messageType == "group" && groupID > 0 {
		return fmt.Sprintf("group:%d:user:%d", groupID, userID)
	}
	return fmt.Sprintf("private:%d", userID)
}

func requestSoutuCompatScope(messageType string, groupID, userID int64) string {
	if messageType == "group" && groupID > 0 {
		return fmt.Sprintf("group:%d", groupID)
	}
	return fmt.Sprintf("private:%d", userID)
}

func randomJMID() string {
	l := 3 + randIntN(7)
	var b strings.Builder
	b.WriteString(strconv.Itoa(1 + randIntN(9)))
	for i := 1; i < l; i++ {
		b.WriteString(strconv.Itoa(randIntN(10)))
	}
	return b.String()
}

func (a *App) randomExistingJMID() (string, bool) {
	const maxTry = 80
	for i := 0; i < maxTry; i++ {
		id := randomJMID()
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		album, err := a.jm.GetAlbum(ctx, id)
		cancel()
		if err != nil || album == nil {
			continue
		}
		if strings.TrimSpace(album.ID) == "" && strings.TrimSpace(album.Title) == "" {
			continue
		}
		return id, true
	}
	return "", false
}

func randIntN(n int) int {
	if n <= 0 {
		return 0
	}
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return int(time.Now().UnixNano() % int64(n))
	}
	return int(v.Int64())
}

func randomPassword(length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if length < 4 {
		length = 4
	}
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteByte(alphabet[randIntN(len(alphabet))])
	}
	return b.String()
}

func normalizeSearchKeyword(k string) string {
	s := strings.TrimSpace(htmlUnescape(k))
	// 移除所有括号类内容：[]、()、【】、《》、〔〕、｛｝、『』、「」
	bracketRe := regexp.MustCompile(`\[[^\]]*\]|\([^\)]*\)|【[^】]*】|《[^》]*》|〔[^〕]*〕|｛[^｝]*｝|『[^』]*」|「[^」]*」`)
	s = bracketRe.ReplaceAllString(s, " ")
	// 合并多个空格并trim
	s = strings.Join(strings.Fields(s), " ")
	return s
}

func htmlUnescape(s string) string {
	replacer := strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`, "&#39;", "'", "&#91;", "[", "&#93;", "]")
	return replacer.Replace(s)
}

func findPDF(dir, number, title string) (string, string) {
	if number != "" {
		// 精确匹配：{number}.pdf 或 JM{number}.pdf
		for _, n := range []string{number + ".pdf", "JM" + number + ".pdf"} {
			p := filepath.Join(dir, n)
			if fileExists(p) {
				return p, n
			}
		}
	}
	if title != "" {
		n := sanitizeFileName(title) + ".pdf"
		p := filepath.Join(dir, n)
		if fileExists(p) {
			return p, n
		}
	}
	// 模糊匹配：只匹配以JM{number}_开头或完全包含{number}.pdf的文件
	if number != "" {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return "", ""
		}
		prefix := "JM" + number + "_"
		suffix := number + ".pdf"
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(strings.ToLower(name), ".pdf") &&
				(strings.HasPrefix(name, prefix) || strings.HasSuffix(name, suffix)) {
				return filepath.Join(dir, name), name
			}
		}
	}
	return "", ""
}

func sanitizeFileName(s string) string {
	r := regexp.MustCompile(`[\\/:*?"<>|]+`)
	s = r.ReplaceAllString(s, "_")
	s = strings.TrimSpace(strings.Trim(s, "."))
	if s == "" {
		return "JM"
	}
	// 按字节截断，避免ext4文件系统255字节限制
	for len(s) > 200 {
		runes := []rune(s)
		if len(runes) <= 1 {
			break
		}
		s = string(runes[:len(runes)-1])
	}
	return s
}

func buildZip(filePath, tmpDir string) (string, error) {
	tmp := filepath.Join(tmpDir, fmt.Sprintf("%d_%s.zip", time.Now().UnixNano(), sanitizeFileName(strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath)))))
	cmd := exec.Command("zip", "-j", "-q", tmp, filePath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("zip failed: %v: %s", err, string(out))
	}
	return tmp, nil
}

func cloneWithName(filePath, baseName, tmpDir string) (string, bool, error) {
	ext := filepath.Ext(filePath)
	safeBase := sanitizeFileName(baseName)
	newPath := filepath.Join(tmpDir, safeBase+ext)
	if fileExists(newPath) {
		newPath = filepath.Join(tmpDir, fmt.Sprintf("%s_%d%s", safeBase, time.Now().UnixNano(), ext))
	}
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return filePath, false, err
	}
	if err := os.WriteFile(newPath, raw, 0o644); err != nil {
		return filePath, false, err
	}
	return newPath, true, nil
}

func normalizeSendNameMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "jm":
		return "jm"
	case "full":
		return "full"
	case "current":
		return "current"
	default:
		return "full"
	}
}

func randomizeHash(filePath, tmpDir string) (string, bool, error) {
	newPath := filepath.Join(tmpDir, fmt.Sprintf("hash_%d_%s", time.Now().UnixNano(), filepath.Base(filePath)))
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return filePath, false, err
	}
	if err := os.WriteFile(newPath, raw, 0o644); err != nil {
		return filePath, false, err
	}
	f, err := os.OpenFile(newPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return filePath, false, err
	}
	defer f.Close()
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return filePath, false, err
	}
	if _, err := f.Write(append([]byte("\n"), buf...)); err != nil {
		return filePath, false, err
	}
	return newPath, true, nil
}

func fileSizeMB(path string) float64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return float64(st.Size()) / 1024.0 / 1024.0
}

// ensureValidImage 确保图片格式正确（处理扩展名与实际格式不匹配的情况）
func (a *App) ensureValidImage(srcPath string) string {
	ext := strings.ToLower(filepath.Ext(srcPath))
	
	// 检测图片实际格式
	actualFormat, err := detectImageFormat(srcPath)
	if err != nil {
		return srcPath
	}
	
	// 如果扩展名与实际格式匹配，直接返回
	if (ext == ".jpg" || ext == ".jpeg") && actualFormat == "jpeg" {
		return srcPath
	}
	if ext == ".png" && actualFormat == "png" {
		return srcPath
	}
	if ext == ".webp" && actualFormat == "webp" {
		return srcPath
	}
	
	// 格式不匹配，转换为JPEG
	return convertImageToJPEG(srcPath)
}

func detectImageFormat(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	
	header := make([]byte, 12)
	_, err = f.Read(header)
	if err != nil {
		return "", err
	}
	
	if header[0] == 0xFF && header[1] == 0xD8 {
		return "jpeg", nil
	}
	if header[0] == 0x89 && header[1] == 0x50 && header[2] == 0x4E && header[3] == 0x47 {
		return "png", nil
	}
	if string(header[0:4]) == "RIFF" && string(header[8:12]) == "WEBP" {
		return "webp", nil
	}
	return "", fmt.Errorf("unknown format")
}

func convertImageToJPEG(srcPath string) string {
	f, err := os.Open(srcPath)
	if err != nil {
		return srcPath
	}
	defer f.Close()
	
	img, _, err := image.Decode(f)
	if err != nil {
		return srcPath
	}
	
	tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("converted_%d.jpg", time.Now().UnixNano()))
	out, err := os.Create(tmpPath)
	if err != nil {
		return srcPath
	}
	defer out.Close()
	
	if err := jpeg.Encode(out, img, &jpeg.Options{Quality: 90}); err != nil {
		return srcPath
	}
	return tmpPath
}

// resizeImageTo210p 缩放图片到210p高度（用于缩略图），返回新文件路径
func (a *App) resizeImageTo210p(srcPath string) string {
	f, err := os.Open(srcPath)
	if err != nil {
		return ""
	}
	defer f.Close()

	img, _, err := image.Decode(f)
	if err != nil {
		return ""
	}

	bounds := img.Bounds()
	w, h := bounds.Dx(), bounds.Dy()

	// 如果已经是210p或更小，直接返回原路径
	if h <= 210 {
		return srcPath
	}

	// 计算缩放后的尺寸，保持比例
	ratio := 210.0 / float64(h)
	newW := int(float64(w) * ratio)
	newH := 210

	// 创建缩放后的图片（简单最近邻缩放）
	resized := image.NewRGBA(image.Rect(0, 0, newW, newH))
	for y := 0; y < newH; y++ {
		for x := 0; x < newW; x++ {
			srcX := int(float64(x) / ratio)
			srcY := int(float64(y) / ratio)
			if srcX >= w {
				srcX = w - 1
			}
			if srcY >= h {
				srcY = h - 1
			}
			resized.Set(x, y, img.At(srcX, srcY))
		}
	}

	// 保存为临时文件
	ext := strings.ToLower(filepath.Ext(srcPath))
	if ext != ".jpg" && ext != ".jpeg" && ext != ".png" {
		ext = ".jpg"
	}
	tmpPath := filepath.Join(a.tmpDir(), fmt.Sprintf("thumb_%d%s", time.Now().UnixNano(), ext))

	out, err := os.Create(tmpPath)
	if err != nil {
		return ""
	}
	defer out.Close()

	switch ext {
	case ".png":
		png.Encode(out, resized)
	default:
		jpeg.Encode(out, resized, &jpeg.Options{Quality: 85})
	}

	return tmpPath
}

func extractJMNumbersFromEvent(data map[string]any, regexEnabled bool) []string {
	text := extractTextFromEvent(data)
	if regexEnabled {
		re := regexp.MustCompile(`(?i)\bjm(\d+)\b`)
		m := re.FindAllStringSubmatch(text, -1)
		out := make([]string, 0, len(m))
		for _, g := range m {
			if len(g) > 1 {
				out = append(out, g[1])
			}
		}
		return unique(out)
	}
	re := regexp.MustCompile(`\d+`)
	return unique(re.FindAllString(stripCQCodes(text), -1))
}

func stripCQCodes(s string) string {
	re := regexp.MustCompile(`\[CQ:[^\]]*\]`)
	return re.ReplaceAllString(s, "")
}

func extractReplyID(s string) string {
	re := regexp.MustCompile(`\[CQ:reply,id=(\d+)\]`)
	m := re.FindStringSubmatch(s)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

func extractTextFromEvent(data map[string]any) string {
	if msg, ok := data["message"]; ok {
		switch t := msg.(type) {
		case string:
			return t
		case []any:
			var b strings.Builder
			for _, seg := range t {
				m, ok := seg.(map[string]any)
				if !ok {
					continue
				}
				if toString(m["type"]) != "text" {
					continue
				}
				dataMap := mapGet(m, "data")
				b.WriteString(toString(dataMap["text"]))
			}
			return b.String()
		}
	}
	return toString(data["raw_message"])
}

type SoutuImageSource struct {
	ImageBytes []byte
	ImageURL   string
}

func (s SoutuImageSource) Desc() string {
	if len(s.ImageBytes) > 0 {
		return "inline-bytes"
	}
	return s.ImageURL
}

func extractSoutuImageSourcesFromEvent(data map[string]any) []SoutuImageSource {
	out := make([]SoutuImageSource, 0, 2)
	appendCandidate := func(v string) {
		u := strings.TrimSpace(v)
		if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
			out = append(out, SoutuImageSource{ImageURL: u})
		}
	}
	appendBase64Candidate := func(v string) {
		s := strings.TrimSpace(v)
		if s == "" {
			return
		}
		// Compatible with data URLs: data:image/jpeg;base64,xxxx
		if idx := strings.Index(s, ","); idx > 0 && strings.Contains(strings.ToLower(s[:idx]), "base64") {
			s = s[idx+1:]
		}
		raw, err := base64.StdEncoding.DecodeString(s)
		if err != nil || len(raw) == 0 {
			return
		}
		out = append(out, SoutuImageSource{ImageBytes: raw})
	}
	parseSegments := func(msg []any) {
		for _, seg := range msg {
			m, ok := seg.(map[string]any)
			if !ok || toString(m["type"]) != "image" {
				continue
			}
			dataMap := mapGet(m, "data")
			appendBase64Candidate(toString(dataMap["base64"]))
			appendCandidate(toString(dataMap["url"]))
			appendCandidate(toString(dataMap["file"]))
		}
	}
	if msg, ok := data["message"].([]any); ok {
		parseSegments(msg)
	}

	// Fallback for CQ-style message payloads.
	cqRaw := toString(data["raw_message"])
	if cqRaw == "" {
		cqRaw = toString(data["message"])
	}
	if cqRaw != "" {
		re := regexp.MustCompile(`url=([^,\]]+)`)
		matches := re.FindAllStringSubmatch(cqRaw, -1)
		for _, m := range matches {
			if len(m) > 1 {
				appendCandidate(htmlUnescape(m[1]))
			}
		}
		// Fallback for HTML-like image messages.
		imgRe := regexp.MustCompile(`(?i)<img[^>]+src=['"]([^'"]+)['"]`)
		imgMatches := imgRe.FindAllStringSubmatch(cqRaw, -1)
		for _, m := range imgMatches {
			if len(m) > 1 {
				src := strings.TrimSpace(htmlUnescape(m[1]))
				if strings.HasPrefix(src, "//") {
					src = "https:" + src
				}
				appendCandidate(src)
			}
		}
	}
	seen := map[string]struct{}{}
	uniq := make([]SoutuImageSource, 0, len(out))
	for _, s := range out {
		key := s.ImageURL
		if len(s.ImageBytes) > 0 {
			key = fmt.Sprintf("bytes:%d", len(s.ImageBytes))
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		uniq = append(uniq, s)
	}
	return uniq
}

func extractSoutuImageFileRefsFromEvent(data map[string]any) []string {
	out := make([]string, 0, 2)
	add := func(v string) {
		s := strings.TrimSpace(htmlUnescape(v))
		if s == "" {
			return
		}
		if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
			return
		}
		if strings.HasPrefix(strings.ToLower(s), "file://") {
			return
		}
		out = append(out, s)
	}
	if msg, ok := data["message"].([]any); ok {
		for _, seg := range msg {
			m, ok := seg.(map[string]any)
			if !ok || toString(m["type"]) != "image" {
				continue
			}
			dataMap := mapGet(m, "data")
			add(toString(dataMap["file"]))
		}
	}
	raw := toString(data["raw_message"])
	if raw == "" {
		raw = toString(data["message"])
	}
	if raw != "" {
		re := regexp.MustCompile(`file=([^,\]]+)`)
		for _, m := range re.FindAllStringSubmatch(raw, -1) {
			if len(m) > 1 {
				add(m[1])
			}
		}
	}
	return unique(out)
}

func hasUnsupportedSoutuImageRef(data map[string]any) bool {
	if msg, ok := data["message"].([]any); ok {
		for _, seg := range msg {
			m, ok := seg.(map[string]any)
			if !ok || toString(m["type"]) != "image" {
				continue
			}
			dataMap := mapGet(m, "data")
			f := strings.TrimSpace(toString(dataMap["file"]))
			if strings.HasPrefix(strings.ToLower(f), "file://") {
				return true
			}
		}
	}
	raw := strings.ToLower(strings.TrimSpace(toString(data["raw_message"])))
	if raw == "" {
		raw = strings.ToLower(strings.TrimSpace(toString(data["message"])))
	}
	return strings.Contains(raw, "file://") || strings.Contains(raw, "<img")
}

func searchSoutuBySource(src SoutuImageSource, cfg Config) (map[string]any, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	if len(src.ImageBytes) > 0 {
		return searchSoutu(src.ImageBytes, cfg, client)
	}
	return searchSoutuByImageURL(src.ImageURL, cfg)
}

func searchSoutuByImageURL(imageURL string, cfg Config) (map[string]any, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("download image status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	imageBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return searchSoutu(imageBytes, cfg, client)
}

func searchSoutu(imageBytes []byte, cfg Config, client *http.Client) (map[string]any, error) {
	cookies := currentCFCookies()
	defaultKey := generateSoutuAPIKey(cfg.SoutuUserAgent, cfg.SoutuGlobalM)
	result, status, respBody, err := searchSoutuOnce(imageBytes, cfg, client, cookies, defaultKey)
	if err != nil {
		return nil, err
	}
	if status >= 200 && status < 300 {
		return result, nil
	}
	if shouldTryCloudflareBypass(status, respBody, len(cookies) > 0) {
		log.Printf("soutu detected potential CF block status=%d body=%q cookies_present=%t", status, strings.TrimSpace(string(respBody)), len(cookies) > 0)
		if err := ensureCfCookies(cfg, client); err == nil {
			result, status, respBody, err = searchSoutuWithAuthFallback(imageBytes, cfg, client, currentCFCookies())
			if err != nil {
				return nil, err
			}
			if status >= 200 && status < 300 {
				return result, nil
			}
			log.Printf("soutu retry after bypass still failed status=%d body=%q", status, strings.TrimSpace(string(respBody)))
			if status == http.StatusUnauthorized {
				log.Printf("soutu fallback to browser-context request after 401")
				if browserResult, browserErr := searchSoutuViaBrowser(imageBytes, cfg); browserErr == nil {
					return browserResult, nil
				} else {
					log.Printf("soutu browser-context fallback failed: %v", browserErr)
				}
			}
		} else {
			return nil, fmt.Errorf("cloudflare bypass failed: %w", err)
		}
	}
	return nil, fmt.Errorf("soutu status %d: %s", status, strings.TrimSpace(string(respBody)))
}

func searchSoutuWithAuthFallback(imageBytes []byte, cfg Config, client *http.Client, cookies map[string]string) (map[string]any, int, []byte, error) {
	type authTry struct {
		label string
		key   string
	}
	tries := []authTry{{
		label: "cfg_global_m",
		key:   generateSoutuAPIKey(cfg.SoutuUserAgent, cfg.SoutuGlobalM),
	}}
	if dynamicM, ok := fetchSoutuGlobalM(cfg, client, cookies); ok && dynamicM > 0 && dynamicM != cfg.SoutuGlobalM {
		tries = append(tries, authTry{
			label: "dynamic_global_m",
			key:   generateSoutuAPIKey(cfg.SoutuUserAgent, dynamicM),
		})
	}
	tries = append(tries, authTry{label: "no_api_key", key: ""})

	seenKey := map[string]struct{}{}
	var lastResult map[string]any
	var lastStatus int
	var lastBody []byte
	for _, tr := range tries {
		if _, ok := seenKey[tr.key]; ok {
			continue
		}
		seenKey[tr.key] = struct{}{}
		result, status, respBody, err := searchSoutuOnce(imageBytes, cfg, client, cookies, tr.key)
		if err != nil {
			return nil, 0, nil, err
		}
		if status >= 200 && status < 300 {
			if tr.label != "cfg_global_m" {
				log.Printf("soutu auth fallback success strategy=%s", tr.label)
			}
			return result, status, respBody, nil
		}
		lastResult, lastStatus, lastBody = result, status, respBody
		log.Printf("soutu auth strategy failed strategy=%s status=%d body=%q", tr.label, status, strings.TrimSpace(string(respBody)))
		if status != http.StatusUnauthorized {
			break
		}
	}
	return lastResult, lastStatus, lastBody, nil
}

func searchSoutuOnce(imageBytes []byte, cfg Config, client *http.Client, cookies map[string]string, apiKey string) (map[string]any, int, []byte, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("factor", fmt.Sprintf("%g", cfg.SoutuFactor))
	fw, err := mw.CreateFormFile("file", "image.jpg")
	if err != nil {
		return nil, 0, nil, err
	}
	if _, err := fw.Write(imageBytes); err != nil {
		return nil, 0, nil, err
	}
	if err := mw.Close(); err != nil {
		return nil, 0, nil, err
	}

	req, err := http.NewRequest(http.MethodPost, cfg.SoutuAPI, &body)
	if err != nil {
		return nil, 0, nil, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("User-Agent", cfg.SoutuUserAgent)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("X-API-KEY", apiKey)
	}
	req.Header.Set("Referer", cfg.SoutuURL)
	for k, v := range cookies {
		req.AddCookie(&http.Cookie{Name: k, Value: v})
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, resp.StatusCode, respBody, nil
	}

	out := map[string]any{}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, resp.StatusCode, nil, err
	}
	return out, resp.StatusCode, nil, nil
}

func fetchSoutuGlobalM(cfg Config, client *http.Client, cookies map[string]string) (int64, bool) {
	req, err := http.NewRequest(http.MethodGet, cfg.SoutuURL, nil)
	if err != nil {
		return 0, false
	}
	req.Header.Set("User-Agent", cfg.SoutuUserAgent)
	req.Header.Set("Referer", cfg.SoutuURL)
	for k, v := range cookies {
		req.AddCookie(&http.Cookie{Name: k, Value: v})
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, false
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return 0, false
	}
	html := string(raw)
	re := regexp.MustCompile(`(?i)global[_-]?m["']?\s*[:=]\s*["']?(\d{6,})`)
	m := re.FindStringSubmatch(html)
	if len(m) < 2 {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	log.Printf("soutu dynamic global_m extracted: %d", n)
	return n, true
}

func searchSoutuViaBrowser(imageBytes []byte, cfg Config) (map[string]any, error) {
	browserPath, err := resolveChromeExecPath()
	if err != nil {
		return nil, err
	}

	opts := []chromedp.ExecAllocatorOption{
		chromedp.ExecPath(browserPath),
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.Flag("force-color-profile", "srgb"),
		chromedp.Flag("metrics-recording-only", true),
		chromedp.Flag("password-store", "basic"),
		chromedp.Flag("use-mock-keychain", true),
		chromedp.Flag("export-tagged-pdf", true),
		chromedp.Flag("disable-background-mode", true),
		chromedp.Flag("enable-features", "NetworkService,NetworkServiceInProcess,LoadCryptoTokenExtension,PermuteTLSExtensions"),
		chromedp.Flag("disable-features", "FlashDeprecationWarning,EnablePasswordsAccountStorage"),
		chromedp.Flag("deny-permission-prompts", true),
		chromedp.DisableGPU,
		chromedp.Flag("accept-lang", "en-US"),
	}
	if runtime.GOOS == "linux" {
		opts = append(opts, chromedp.Headless)
		opts = append(opts, chromedp.Flag("no-sandbox", true))
	}
	if strings.TrimSpace(cfg.SoutuUserAgent) != "" {
		opts = append(opts, chromedp.UserAgent(strings.TrimSpace(cfg.SoutuUserAgent)))
	}

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	defer allocCancel()
	ctx, cancel := chromedp.NewContext(allocCtx, chromedp.WithLogf(func(string, ...any) {}))
	defer cancel()
	ctx, timeoutCancel := context.WithTimeout(ctx, 180*time.Second)
	defer timeoutCancel()

	if err := chromedp.Run(ctx, chromedp.Navigate(cfg.SoutuURL)); err != nil {
		return nil, fmt.Errorf("browser navigate: %w", err)
	}
	time.Sleep(6 * time.Second)
	for i := 0; i < 30; i++ {
		if i >= 2 && (i == 2 || i%7 == 0) {
			_ = tryCloudflareChallengeClick(ctx)
		}
		time.Sleep(300 * time.Millisecond)
	}

	type authTry struct {
		label string
		key   string
	}
	tries := []authTry{{label: "cfg_global_m", key: generateSoutuAPIKey(cfg.SoutuUserAgent, cfg.SoutuGlobalM)}}
	tries = append(tries, authTry{label: "no_api_key", key: ""})

	b64 := base64.StdEncoding.EncodeToString(imageBytes)
	factor := fmt.Sprintf("%g", cfg.SoutuFactor)
	for _, tr := range tries {
		status, body, err := runBrowserSoutuFetch(ctx, cfg.SoutuAPI, cfg.SoutuURL, factor, b64, tr.key)
		if err != nil {
			log.Printf("browser soutu auth strategy error strategy=%s err=%v", tr.label, err)
			continue
		}
		if status >= 200 && status < 300 {
			out := map[string]any{}
			if err := json.Unmarshal([]byte(body), &out); err != nil {
				return nil, fmt.Errorf("browser soutu parse json failed: %w", err)
			}
			log.Printf("browser soutu auth strategy success strategy=%s", tr.label)
			return out, nil
		}
		log.Printf("browser soutu auth strategy failed strategy=%s status=%d body=%q", tr.label, status, strings.TrimSpace(body))
	}
	return nil, errors.New("browser soutu request failed after all auth strategies")
}

func runBrowserSoutuFetch(ctx context.Context, apiURL, referer, factor, imageB64, apiKey string) (int, string, error) {
	toJS := func(s string) string {
		b, _ := json.Marshal(s)
		return string(b)
	}
	script := fmt.Sprintf(`(() => {
		window.__soutu_result = null;
		window.__soutu_error = null;
		(async () => {
			try {
				const apiURL = %s;
				const referer = %s;
				const factor = %s;
				const b64 = %s;
				const apiKey = %s;
				const bin = atob(b64);
				const arr = new Uint8Array(bin.length);
				for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
				const blob = new Blob([arr], { type: 'image/jpeg' });
				const fd = new FormData();
				fd.append('factor', factor);
				fd.append('file', blob, 'image.jpg');
				const headers = { 'X-Requested-With': 'XMLHttpRequest', 'Referer': referer };
				if (apiKey) headers['X-API-KEY'] = apiKey;
				const resp = await fetch(apiURL, { method: 'POST', body: fd, credentials: 'include', headers });
				const text = await resp.text();
				window.__soutu_result = { status: resp.status, body: text };
			} catch (e) {
				window.__soutu_error = String(e);
			}
		})();
		return true;
	})()`, toJS(apiURL), toJS(referer), toJS(factor), toJS(imageB64), toJS(apiKey))

	var kicked bool
	if err := chromedp.Run(ctx, chromedp.Evaluate(script, &kicked)); err != nil {
		return 0, "", err
	}

	for i := 0; i < 100; i++ {
		time.Sleep(300 * time.Millisecond)
		var pollRaw string
		pollJS := `(() => {
			if (window.__soutu_error) return JSON.stringify({ done: true, error: String(window.__soutu_error) });
			if (window.__soutu_result) return JSON.stringify({ done: true, status: window.__soutu_result.status, body: window.__soutu_result.body || "" });
			return JSON.stringify({ done: false });
		})()`
		if err := chromedp.Run(ctx, chromedp.Evaluate(pollJS, &pollRaw)); err != nil {
			continue
		}
		out := map[string]any{}
		if err := json.Unmarshal([]byte(pollRaw), &out); err != nil {
			continue
		}
		if done, _ := out["done"].(bool); !done {
			continue
		}
		if errMsg := strings.TrimSpace(toString(out["error"])); errMsg != "" {
			return 0, "", errors.New(errMsg)
		}
		return int(toInt64(out["status"])), toString(out["body"]), nil
	}
	return 0, "", errors.New("browser fetch timeout")
}

func isCloudflareBlocked(status int, body []byte) bool {
	if status != http.StatusForbidden && status != http.StatusUnauthorized {
		return false
	}
	text := strings.ToLower(string(body))
	return strings.Contains(text, "cloudflare") ||
		strings.Contains(text, "cf-mitigated") ||
		strings.Contains(text, "__cf_chl") ||
		strings.Contains(text, "just a moment") ||
		strings.Contains(text, "challenge-platform")
}

func shouldTryCloudflareBypass(status int, body []byte, hasCookies bool) bool {
	if isCloudflareBlocked(status, body) {
		return true
	}
	if status != http.StatusUnauthorized {
		return false
	}
	trimmed := strings.TrimSpace(string(body))
	lower := strings.ToLower(trimmed)
	// Some deployments return 401 with empty/near-empty body when challenge is active.
	if trimmed == "" || trimmed == "{}" {
		return true
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(trimmed), &out); err == nil {
		msg := strings.TrimSpace(toString(out["message"]))
		if msg == "" {
			return true
		}
		lower = strings.ToLower(msg)
	}
	// If we already have cookies and still get explicit auth errors, avoid useless bypass loops.
	if hasCookies {
		return false
	}
	return strings.Contains(lower, "unauthorized") || strings.Contains(lower, "access denied")
}

func ensureCfCookies(cfg Config, client *http.Client) error {
	soutuCFMu.RLock()
	if !soutuCFCookieExpires.IsZero() && time.Now().Before(soutuCFCookieExpires) && len(soutuCFCookies) > 0 {
		soutuCFMu.RUnlock()
		return nil
	}
	soutuCFMu.RUnlock()

	targets := unique([]string{
		strings.TrimSpace(cfg.SoutuAPI),
		strings.TrimSpace(cfg.SoutuURL),
	})
	var lastErr error
	for _, targetURL := range targets {
		if targetURL == "" {
			continue
		}
		log.Printf("trying cloudflare bypass target=%s", targetURL)
		res, err := pollBypassAPI(cfg, client, targetURL)
		if err != nil {
			lastErr = err
			continue
		}
		if applyBypassResult(res) {
			return nil
		}
		lastErr = errors.New("invalid bypass result")
	}
	if lastErr == nil {
		lastErr = errors.New("cloudflare bypass failed for all targets")
	}
	return lastErr
}

func pollBypassAPI(cfg Config, client *http.Client, targetURL string) (bypassResponse, error) {
	deadline := time.Now().Add(time.Duration(cfg.CFBypassPollTimeout * float64(time.Second)))
	interval := time.Duration(cfg.CFBypassPollInterval * float64(time.Second))
	if interval <= 0 {
		interval = 2 * time.Second
	}

	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := callBypassAPI(cfg, client, targetURL)
		if err != nil {
			lastErr = err
			time.Sleep(interval)
			continue
		}
		if len(resp.Cookies) > 0 {
			return resp, nil
		}
		if strings.Contains(resp.Message, "继续轮询") {
			time.Sleep(interval)
			continue
		}
		lastErr = fmt.Errorf("bypass api returned message: %s", resp.Message)
		time.Sleep(interval)
	}
	if lastErr == nil {
		lastErr = errors.New("bypass api poll timeout")
	}
	return bypassResponse{}, lastErr
}

func callBypassAPI(cfg Config, client *http.Client, targetURL string) (bypassResponse, error) {
	directClient := newDirectHTTPClient(client.Timeout)

	payload := map[string]any{
		"url":        targetURL,
		"user_agent": cfg.SoutuUserAgent,
	}
	bs, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, cfg.CFBypassAPIURL, bytes.NewReader(bs))
	if err != nil {
		return bypassResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := directClient.Do(req)
	if err != nil {
		return bypassResponse{}, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return bypassResponse{}, fmt.Errorf("bypass api status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var out bypassResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return bypassResponse{}, err
	}
	return out, nil
}

func newDirectHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	tr, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		cp := tr.Clone()
		cp.Proxy = nil
		return &http.Client{Timeout: timeout, Transport: cp}
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy: nil,
		},
	}
}

func applyBypassResult(res bypassResponse) bool {
	if len(res.Cookies) == 0 {
		log.Printf("bypass result rejected: empty cookies")
		return false
	}
	cookies := map[string]string{}
	hasClearance := false
	for _, c := range res.Cookies {
		if c.Name == "" || c.Value == "" {
			continue
		}
		cookies[c.Name] = c.Value
		if c.Name == "cf_clearance" {
			hasClearance = true
		}
	}
	if len(cookies) == 0 {
		log.Printf("bypass result rejected: no valid cookie pairs")
		return false
	}
	if !hasClearance {
		names := make([]string, 0, len(cookies))
		for k := range cookies {
			names = append(names, k)
		}
		log.Printf("bypass result rejected: cf_clearance missing, got=%v", names)
		return false
	}
	soutuCFMu.Lock()
	soutuCFCookies = cookies
	soutuCFCookieExpires = time.Now().Add(25 * time.Minute)
	soutuCFMu.Unlock()
	log.Printf("bypass result accepted: cookies=%d includes_cf_clearance=true", len(cookies))
	return true
}

func currentCFCookies() map[string]string {
	soutuCFMu.RLock()
	defer soutuCFMu.RUnlock()
	out := make(map[string]string, len(soutuCFCookies))
	for k, v := range soutuCFCookies {
		out[k] = v
	}
	return out
}

func briefError(err error) string {
	if err == nil {
		return "未知错误"
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "未知错误"
	}
	msg = strings.ReplaceAll(msg, "\n", " ")
	r := []rune(msg)
	if len(r) > 80 {
		return string(r[:80]) + "..."
	}
	return msg
}

func generateSoutuAPIKey(userAgent string, globalM int64) string {
	unixTS := time.Now().Unix()
	uaLen := int64(len(userAgent))
	raw := strconv.FormatInt(unixTS*unixTS+uaLen*uaLen+globalM, 10)
	encoded := base64.StdEncoding.EncodeToString([]byte(raw))
	rev := reverseString(encoded)
	return strings.ReplaceAll(rev, "=", "")
}

func reverseString(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

func formatSoutuResult(result map[string]any) string {
	data, ok := result["data"].([]any)
	if !ok || len(data) == 0 {
		return "没有找到匹配的结果"
	}
	top, ok := data[0].(map[string]any)
	if !ok {
		return "没有找到匹配的结果"
	}

	sourceHosts := map[string]string{
		"nhentai": "nhentai.net",
		"ehentai": "e-hentai.org",
		"panda":   "panda.chaika.moe",
	}
	title := toString(top["title"])
	sim := toString(top["similarity"])
	source := toString(top["source"])
	subjectPath := toString(top["subjectPath"])
	pagePath := toString(top["pagePath"])
	url := ""
	if host := sourceHosts[source]; host != "" {
		path := pagePath
		if path == "" {
			path = subjectPath
		}
		url = "https://" + host + path
	}
	msg := "搜图结果：\n标题：" + title + "\n相似度：" + sim + "%"
	if url != "" {
		msg += "\n链接：" + url
	}
	return msg
}

func unique(in []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func helpMessage() string {
	return "使用说明：\n" +
		"【JM漫画】\n" +
		"1) /jm <ID>：下载并发送本子\n" +
		"2) /jm look|验车 <ID或本子名>：查看本子信息（支持名称搜索）\n" +
		"3) /jm search <本子名>：搜索本子并下载（需确认）\n" +
		"4) /jm search | 识图 | /jm识图：开启2分钟识图等待窗口\n" +
		"5) /jm goodluck | /goodluck | 随机本子：随机本子下载\n" +
		"6) /jm mode pdf|zip：设置发送格式\n" +
		"7) /jm enc on|off：设置是否加密\n" +
		"8) /jm passwd <密码>：设置加密密码\n" +
		"9) /jm randpwd on|off：启用随机密码加密\n" +
		"10) /jm fname jm|full|current：设置发送文件命名方式\n" +
		"11) /jm regex on|off：设置正则模式\n" +
		"12) /jm strict on|off：设置严格模式（只处理/jm开头的消息）\n" +
		"12) /jm dedup show|set|clear：重复请求冷却管理（管理员）\n" +
		"13) /jm cfg list|show|set：在线配置开关（管理员）\n" +
		"14) /jm allow add|del <群号>：管理白名单群（管理员）\n" +
		"15) /jm allow list：查看白名单群（管理员）\n" +
		"14) /jm daily on|off：启用/关闭每日本子推荐（管理员）\n" +
		"15) /jm daily add|del <群号>：添加/删除推荐群（管理员）\n" +
		"16) /jm daily now：立即发送每日推荐（管理员）\n" +
		"16) /jm help：查看帮助\n\n" +
		"【AI画图】\n" +
		"1) image on：开启画图功能（管理员）\n" +
		"2) image off：关闭画图功能（管理员）\n" +
		"3) image2 <提示词>：AI 文生图\n" +
		"4) 引用图片后 image2 <提示词>：AI 图生图\n" +
		"5) /image2 <提示词>：同上\n\n" +
		"【哔咔漫画】\n" +
		"1) /bika on|off：启用/关闭哔咔（管理员）\n" +
		"2) /bika login <邮箱> <密码>：登录哔咔账号\n" +
		"3) /bika logout：退出当前账号\n" +
		"4) /bika whoami：查看当前登录状态\n" +
		"5) /bika search <关键词>：搜索漫画\n" +
		"6) /bika look <ID>：查看漫画详情\n" +
		"7) /bika dl <ID> [章节]：下载漫画\n" +
		"8) /bika confirm <序号>：确认搜索结果下载\n" +
		"9) /bika help：查看哔咔帮助"
}

func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

func removeStr(list []string, v string) []string {
	out := make([]string, 0, len(list))
	for _, item := range list {
		if item != v {
			out = append(out, item)
		}
	}
	return out
}

func containsInt64(list []int64, v int64) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

func removeInt64(list []int64, v int64) []int64 {
	out := make([]int64, 0, len(list))
	for _, item := range list {
		if item != v {
			out = append(out, item)
		}
	}
	return out
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func matched(pattern, s string) bool {
	return regexp.MustCompile(pattern).MatchString(s)
}

func mustMatch(pattern, s string) []string {
	return regexp.MustCompile(pattern).FindStringSubmatch(s)
}

func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toInt64(v any) int64 {
	s := toString(v)
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func mapGet(v any, key string) map[string]any {
	m, ok := v.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	if key == "" {
		return m
	}
	mv, ok := m[key].(map[string]any)
	if !ok {
		return map[string]any{}
	}
	return mv
}

type NapcatClient struct {
	url    string
	token  string
	dryRun bool
}

func NewNapcatClient(url, token string, dryRun bool) *NapcatClient {
	return &NapcatClient{url: url, token: token, dryRun: dryRun}
}

func (c *NapcatClient) SendPrivateMessage(userID int64, message string) bool {
	_, err := c.send("send_private_msg", map[string]any{"user_id": userID, "message": []map[string]any{{"type": "text", "data": map[string]any{"text": message}}}}, echo("private_text", userID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) GetMsg(messageID int64) (map[string]any, error) {
	resp, err := c.send("get_msg", map[string]any{"message_id": messageID}, echo("get_msg", messageID), 10*time.Second)
	if err != nil {
		return nil, err
	}
	return mapGet(resp, "data"), nil
}

func (c *NapcatClient) GetImageURL(fileRef string) (string, error) {
	if strings.TrimSpace(fileRef) == "" {
		return "", errors.New("empty file ref")
	}
	resp, err := c.send("get_image", map[string]any{"file": fileRef}, echo("get_image", time.Now().UnixNano()), 10*time.Second)
	if err != nil {
		return "", err
	}
	data := mapGet(resp, "data")
	u := strings.TrimSpace(toString(data["url"]))
	if u != "" {
		return u, nil
	}
	return "", errors.New("get_image returned empty url")
}

func (c *NapcatClient) SendGroupMessage(groupID int64, message string) bool {
	_, err := c.send("send_group_msg", map[string]any{"group_id": groupID, "message": []map[string]any{{"type": "text", "data": map[string]any{"text": message}}}}, echo("group_text", groupID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendGroupJSONCardMessage(groupID int64, message string, nickname string) bool {
	card := buildJSONCardPayload(nickname, message)
	_, err := c.send("send_group_msg", map[string]any{
		"group_id": groupID,
		"message": []map[string]any{
			{"type": "json", "data": map[string]any{"data": card}},
		},
	}, echo("group_json_card", groupID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendPrivateJSONCardMessage(userID int64, message string, nickname string) bool {
	card := buildJSONCardPayload(nickname, message)
	_, err := c.send("send_private_msg", map[string]any{
		"user_id": userID,
		"message": []map[string]any{
			{"type": "json", "data": map[string]any{"data": card}},
		},
	}, echo("private_json_card", userID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendGroupXMLCardMessage(groupID int64, message string, nickname string) bool {
	card := buildXMLCardPayload(nickname, message)
	_, err := c.send("send_group_msg", map[string]any{
		"group_id": groupID,
		"message": []map[string]any{
			{"type": "xml", "data": map[string]any{"data": card}},
		},
	}, echo("group_xml_card", groupID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendPrivateXMLCardMessage(userID int64, message string, nickname string) bool {
	card := buildXMLCardPayload(nickname, message)
	_, err := c.send("send_private_msg", map[string]any{
		"user_id": userID,
		"message": []map[string]any{
			{"type": "xml", "data": map[string]any{"data": card}},
		},
	}, echo("private_xml_card", userID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendGroupMarkdownCardMessage(groupID int64, message string, nickname string) bool {
	md := buildMarkdownCard(nickname, message)
	_, err := c.send("send_group_msg", map[string]any{
		"group_id": groupID,
		"message": []map[string]any{
			{"type": "markdown", "data": map[string]any{"content": md}},
		},
	}, echo("group_md_card", groupID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendPrivateMarkdownCardMessage(userID int64, message string, nickname string) bool {
	md := buildMarkdownCard(nickname, message)
	_, err := c.send("send_private_msg", map[string]any{
		"user_id": userID,
		"message": []map[string]any{
			{"type": "markdown", "data": map[string]any{"content": md}},
		},
	}, echo("private_md_card", userID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendGroupForwardCardMessage(groupID int64, message string, senderID int64, nickname string) bool {
	node := map[string]any{
		"type": "node",
		"data": map[string]any{
			"user_id":  senderID,
			"nickname": nickname,
			"content": []map[string]any{
				{"type": "text", "data": map[string]any{"text": message}},
			},
		},
	}
	_, err := c.send("send_group_forward_msg", map[string]any{
		"group_id": groupID,
		"message":  []map[string]any{node},
	}, echo("group_forward_card", groupID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendPrivateForwardCardMessage(userID int64, message string, senderID int64, nickname string) bool {
	node := map[string]any{
		"type": "node",
		"data": map[string]any{
			"user_id":  senderID,
			"nickname": nickname,
			"content": []map[string]any{
				{"type": "text", "data": map[string]any{"text": message}},
			},
		},
	}
	_, err := c.send("send_private_forward_msg", map[string]any{
		"user_id": userID,
		"message": []map[string]any{node},
	}, echo("private_forward_card", userID), 10*time.Second)
	return err == nil
}

type preparedForwardFile struct {
	candidates []string
	cleanup    func()
}

func (c *NapcatClient) SendGroupForwardBundle(cfg Config, groupID int64, summary string, filePaths []string, senderID int64, nickname string) bool {
	return c.sendForwardBundle(cfg, "send_group_forward_msg", map[string]any{"group_id": groupID}, summary, filePaths, senderID, nickname, echo("group_forward_bundle", groupID))
}

func (c *NapcatClient) SendPrivateForwardBundle(cfg Config, userID int64, summary string, filePaths []string, senderID int64, nickname string) bool {
	return c.sendForwardBundle(cfg, "send_private_forward_msg", map[string]any{"user_id": userID}, summary, filePaths, senderID, nickname, echo("private_forward_bundle", userID))
}

func (c *NapcatClient) sendForwardBundle(cfg Config, action string, baseParams map[string]any, summary string, filePaths []string, senderID int64, nickname string, echoValue string) bool {
	if c.dryRun {
		log.Printf("[local-test] forward bundle action=%s files=%d", action, len(filePaths))
		return true
	}
	if strings.TrimSpace(nickname) == "" {
		nickname = "文件助手"
	}
	if senderID <= 0 {
		senderID = 10000
	}

	prepared := make([]preparedForwardFile, 0, len(filePaths))
	defer func() {
		for _, p := range prepared {
			if p.cleanup != nil {
				p.cleanup()
			}
		}
	}()

	maxCandidates := 1
	for _, p := range filePaths {
		pf, err := c.prepareForwardFile(cfg, p)
		if err != nil {
			log.Printf("prepare forward file failed path=%s err=%v", p, err)
			return false
		}
		if len(pf.candidates) == 0 {
			return false
		}
		if len(pf.candidates) > maxCandidates {
			maxCandidates = len(pf.candidates)
		}
		prepared = append(prepared, pf)
	}

	for idx := 0; idx < maxCandidates; idx++ {
		nodes := make([]map[string]any, 0, len(prepared)+1)
		nodes = append(nodes, map[string]any{
			"type": "node",
			"data": map[string]any{
				"user_id":  senderID,
				"nickname": nickname,
				"content": []map[string]any{
					{"type": "text", "data": map[string]any{"text": summary}},
				},
			},
		})

		for _, pf := range prepared {
			ref := pf.candidates[len(pf.candidates)-1]
			if idx < len(pf.candidates) {
				ref = pf.candidates[idx]
			}
			nodes = append(nodes, map[string]any{
				"type": "node",
				"data": map[string]any{
					"user_id":  senderID,
					"nickname": nickname,
					"content": []map[string]any{
						{"type": "file", "data": map[string]any{"file": ref}},
					},
				},
			})
		}

		params := copyMap(baseParams)
		params["message"] = nodes
		if _, err := c.send(action, params, echoValue, 600*time.Second); err == nil {
			return true
		}
	}
	return false
}

func (c *NapcatClient) prepareForwardFile(cfg Config, filePath string) (preparedForwardFile, error) {
	if streamPath, err := c.uploadFileStream(filePath, 600*time.Second); err == nil && strings.TrimSpace(streamPath) != "" {
		return preparedForwardFile{
			candidates: []string{
				streamPath,
				"file://" + streamPath,
			},
			cleanup: nil,
		}, nil
	}
	remotePath, err := stageForNapcat(cfg, filePath)
	if err != nil {
		return preparedForwardFile{}, err
	}
	dockerPath := filepath.Join(cfg.DockerPath, filepath.Base(remotePath))
	return preparedForwardFile{
		candidates: []string{
			remotePath,
			"file://" + remotePath,
			dockerPath,
			"file://" + dockerPath,
		},
		cleanup: func() { cleanupRemote(cfg, remotePath) },
	}, nil
}

func buildJSONCardPayload(nickname, message string) string {
	nickname = strings.TrimSpace(nickname)
	if nickname == "" {
		nickname = "文件助手"
	}
	text := strings.TrimSpace(message)
	if text == "" {
		text = " "
	}
	if len([]rune(text)) > 1200 {
		text = string([]rune(text)[:1200]) + "..."
	}

	title := nickname
	summary := firstLine(text)
	if summary == "" {
		summary = nickname
	}

	payload := map[string]any{
		"app":    "com.tencent.card.notify",
		"desc":   "消息",
		"view":   "notify",
		"ver":    "1.0.0.0",
		"prompt": summary,
		"meta": map[string]any{
			"notify": map[string]any{
				"title":   title,
				"content": text,
			},
		},
	}
	b, err := json.Marshal(payload)
	if err != nil {
		fallback := map[string]any{
			"app":    "com.tencent.card.notify",
			"desc":   "消息",
			"view":   "notify",
			"ver":    "1.0.0.0",
			"prompt": nickname,
			"meta": map[string]any{
				"notify": map[string]any{
					"title":   nickname,
					"content": "消息发送失败",
				},
			},
		}
		b, _ = json.Marshal(fallback)
	}
	return string(b)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	for _, sep := range []string{"\r\n", "\n", "\r"} {
		if idx := strings.Index(s, sep); idx >= 0 {
			s = s[:idx]
			break
		}
	}
	if len([]rune(s)) > 60 {
		return string([]rune(s)[:60]) + "..."
	}
	return s
}

func buildXMLCardPayload(nickname, message string) string {
	nickname = strings.TrimSpace(nickname)
	if nickname == "" {
		nickname = "文件助手"
	}
	text := strings.TrimSpace(message)
	if text == "" {
		text = " "
	}
	if len([]rune(text)) > 1200 {
		text = string([]rune(text)[:1200]) + "..."
	}
	title := firstLine(text)
	if title == "" {
		title = nickname
	}

	esc := func(s string) string {
		r := strings.NewReplacer(
			"&", "&amp;",
			"<", "&lt;",
			">", "&gt;",
			`"`, "&quot;",
			"'", "&apos;",
		)
		return r.Replace(s)
	}

	return fmt.Sprintf(
		`<?xml version='1.0' encoding='UTF-8' standalone='yes'?><msg serviceID="1" templateID="1" action="" brief="%s"><item layout="2"><title>%s</title><summary>%s</summary></item><source name="%s"/></msg>`,
		esc(title),
		esc(title),
		esc(text),
		esc(nickname),
	)
}

func buildMarkdownCard(nickname, message string) string {
	nickname = strings.TrimSpace(nickname)
	if nickname == "" {
		nickname = "文件助手"
	}
	text := strings.TrimSpace(message)
	if text == "" {
		text = " "
	}
	if len([]rune(text)) > 1200 {
		text = string([]rune(text)[:1200]) + "..."
	}
	return fmt.Sprintf("### %s\n\n%s", nickname, text)
}

func (c *NapcatClient) SendPrivateFile(cfg Config, userID int64, filePath string) bool {
	return c.sendFile(cfg, "send_private_msg", map[string]any{"user_id": userID}, filePath)
}

func (c *NapcatClient) SendGroupImage(groupID int64, imageFile string) bool {
	_, err := c.send("send_group_msg", map[string]any{
		"group_id": groupID,
		"message": []map[string]any{
			{"type": "image", "data": map[string]any{"file": imageFile}},
		},
	}, echo("group_img", groupID), 60*time.Second)
	return err == nil
}

func (c *NapcatClient) SendGroupMsgWithAtAndImage(groupID, userID int64, imageFile string) bool {
	_, err := c.send("send_group_msg", map[string]any{
		"group_id": groupID,
		"message": []map[string]any{
			{"type": "at", "data": map[string]any{"qq": userID}},
			{"type": "image", "data": map[string]any{"file": imageFile}},
		},
	}, echo("group_img_at", groupID), 60*time.Second)
	return err == nil
}

func (c *NapcatClient) SendPrivateMsgWithImage(userID int64, imageFile string) bool {
	_, err := c.send("send_private_msg", map[string]any{
		"user_id": userID,
		"message": []map[string]any{
			{"type": "image", "data": map[string]any{"file": imageFile}},
		},
	}, echo("private_img", userID), 60*time.Second)
	return err == nil
}

func (c *NapcatClient) SendGroupMsgWithAtText(groupID, userID int64, text string) bool {
	_, err := c.send("send_group_msg", map[string]any{
		"group_id": groupID,
		"message": []map[string]any{
			{"type": "at", "data": map[string]any{"qq": userID}},
			{"type": "text", "data": map[string]any{"text": text}},
		},
	}, echo("group_at_txt", groupID), 10*time.Second)
	return err == nil
}

func (c *NapcatClient) SendGroupFile(cfg Config, groupID int64, filePath string) bool {
	return c.sendFile(cfg, "send_group_msg", map[string]any{"group_id": groupID}, filePath)
}

func (c *NapcatClient) sendFile(cfg Config, action string, baseParams map[string]any, filePath string) bool {
	if c.dryRun {
		log.Printf("[local-test] file message action=%s path=%s", action, filePath)
		return true
	}

	if streamPath, err := c.uploadFileStream(filePath, 600*time.Second); err == nil && strings.TrimSpace(streamPath) != "" {
		streamRefs := []string{
			streamPath,
			"file://" + streamPath,
		}
		if c.tryUploadFileFallback(action, baseParams, streamRefs, filepath.Base(filePath)) {
			return true
		}
		if ok, _ := c.tryPrimarySendRefs(action, baseParams, streamRefs); ok {
			return true
		}
		log.Printf("stream upload completed but send still failed, fallback to staged path")
	} else if err != nil {
		log.Printf("upload_file_stream failed, fallback to staged path: %v", err)
	}

	remotePath, err := stageForNapcat(cfg, filePath)
	if err != nil {
		log.Printf("stage file failed: %v", err)
		return false
	}
	defer cleanupRemote(cfg, remotePath)
	dockerPath := filepath.Join(cfg.DockerPath, filepath.Base(remotePath))
	fileRefs := []string{
		remotePath,
		"file://" + remotePath,
		dockerPath,
		"file://" + dockerPath,
	}

	if c.tryUploadFileFallback(action, baseParams, fileRefs, filepath.Base(filePath)) {
		return true
	}
	ok, lastErr := c.tryPrimarySendRefs(action, baseParams, fileRefs)
	if ok {
		return true
	}
	log.Printf("send file failed: %v", lastErr)
	return false
}

func (c *NapcatClient) tryPrimarySendRefs(action string, baseParams map[string]any, fileRefs []string) (bool, error) {
	var lastErr error
	for _, ref := range fileRefs {
		params := copyMap(baseParams)
		params["message"] = []map[string]any{{"type": "file", "data": map[string]any{"file": ref}}}
		_, err := c.send(action, params, echo("file", time.Now().UnixNano()), 600*time.Second)
		if err == nil {
			log.Printf("send file primary action success ref=%s", ref)
			return true, nil
		}
		lastErr = err
		log.Printf("send file primary action failed ref=%s err=%v", ref, err)
		if !isRichMediaTransferErr(err) {
			break
		}
	}
	return false, lastErr
}

func isRichMediaTransferErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "rich media transfer failed") || strings.Contains(msg, "retcode:1200")
}

func (c *NapcatClient) tryUploadFileFallback(sendAction string, baseParams map[string]any, fileRefs []string, fileName string) bool {
	action := ""
	params := map[string]any{}
	switch sendAction {
	case "send_group_msg":
		action = "upload_group_file"
		groupID := toInt64(baseParams["group_id"])
		if groupID <= 0 {
			return false
		}
		params["group_id"] = groupID
	case "send_private_msg":
		action = "upload_private_file"
		userID := toInt64(baseParams["user_id"])
		if userID <= 0 {
			return false
		}
		params["user_id"] = userID
	default:
		return false
	}
	params["name"] = fileName

	for _, fileArg := range fileRefs {
		req := copyMap(params)
		req["file"] = fileArg
		_, err := c.send(action, req, echo("file_upload_fallback", time.Now().UnixNano()), 600*time.Second)
		if err == nil {
			log.Printf("file upload fallback success action=%s file=%s", action, fileArg)
			return true
		}
		log.Printf("file upload fallback failed action=%s file=%s err=%v", action, fileArg, err)
	}
	return false
}

func (c *NapcatClient) send(action string, params map[string]any, echoValue string, timeout time.Duration) (map[string]any, error) {
	if c.dryRun {
		log.Printf("[local-test] action=%s echo=%s", action, echoValue)
		return map[string]any{"status": "ok", "echo": echoValue}, nil
	}
	conn, err := c.openWS()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return c.sendOnConn(conn, action, params, echoValue, timeout)
}

func (c *NapcatClient) openWS() (*websocket.Conn, error) {
	h := http.Header{}
	if c.token != "" {
		h.Set("Authorization", "Bearer "+c.token)
	}
	conn, _, err := websocket.DefaultDialer.Dial(c.url, h)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *NapcatClient) sendOnConn(conn *websocket.Conn, action string, params map[string]any, echoValue string, timeout time.Duration) (map[string]any, error) {
	payload := map[string]any{"action": action, "params": params, "echo": echoValue}
	b, _ := json.Marshal(payload)
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		var resp map[string]any
		if err := json.Unmarshal(msg, &resp); err != nil {
			continue
		}
		if toString(resp["echo"]) != echoValue {
			continue
		}
		if toString(resp["status"]) != "ok" {
			return resp, fmt.Errorf("napcat ret failed: %v", resp)
		}
		return resp, nil
	}
}

func (c *NapcatClient) uploadFileStream(filePath string, timeout time.Duration) (string, error) {
	st, err := os.Stat(filePath)
	if err != nil {
		return "", err
	}
	if st.IsDir() {
		return "", errors.New("upload_file_stream path is directory")
	}
	fileSize := st.Size()
	if fileSize <= 0 {
		return "", errors.New("upload_file_stream empty file")
	}

	expectedSHA256, err := fileSHA256(filePath)
	if err != nil {
		return "", err
	}

	const chunkSize = 64 * 1024
	totalChunks := int((fileSize + chunkSize - 1) / chunkSize)
	streamID := fmt.Sprintf("stream_%d_%d", time.Now().UnixNano(), randIntN(1_000_000))

	conn, err := c.openWS()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	buf := make([]byte, chunkSize)
	for idx := 0; idx < totalChunks; idx++ {
		n, readErr := io.ReadFull(f, buf)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return "", readErr
		}
		if n <= 0 {
			return "", fmt.Errorf("upload_file_stream read empty chunk idx=%d", idx)
		}
		chunkData := base64.StdEncoding.EncodeToString(buf[:n])
		params := map[string]any{
			"stream_id":       streamID,
			"chunk_data":      chunkData,
			"chunk_index":     idx,
			"total_chunks":    totalChunks,
			"file_size":       fileSize,
			"expected_sha256": expectedSHA256,
			"filename":        filepath.Base(filePath),
			"file_retention":  10 * 60 * 1000,
		}
		if _, err := c.sendOnConn(conn, "upload_file_stream", params, echo("file_stream_chunk", idx), timeout); err != nil {
			return "", err
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	resp, err := c.sendOnConn(conn, "upload_file_stream", map[string]any{
		"stream_id":   streamID,
		"is_complete": true,
	}, echo("file_stream_complete", time.Now().UnixNano()), timeout)
	if err != nil {
		return "", err
	}
	filePathOut := strings.TrimSpace(toString(mapGet(resp, "data")["file_path"]))
	if filePathOut == "" {
		return "", fmt.Errorf("upload_file_stream returned empty file_path: %v", resp)
	}
	log.Printf("upload_file_stream success stream_id=%s file_path=%s", streamID, filePathOut)
	return filePathOut, nil
}

func fileSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func stageForNapcat(cfg Config, localFile string) (string, error) {
	remote := filepath.Join(cfg.RemoteTempDir, stagedRemoteName(localFile))
	if strings.ToLower(cfg.TransferMode) == "local" {
		raw, err := os.ReadFile(localFile)
		if err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(remote), 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(remote, raw, 0o644); err != nil {
			return "", err
		}
		return remote, nil
	}
	cmd := exec.Command("scp", "-i", cfg.LocalSSHKey, "-o", "StrictHostKeyChecking=no", localFile, fmt.Sprintf("%s@%s:%s", cfg.RemoteUser, cfg.RemoteHost, remote))
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("scp failed: %v: %s", err, string(out))
	}
	return remote, nil
}

func stagedRemoteName(localFile string) string {
	ext := strings.ToLower(filepath.Ext(localFile))
	if ext == "" {
		ext = ".bin"
	}
	return fmt.Sprintf("jm_%d%s", time.Now().UnixNano(), ext)
}

func cleanupRemote(cfg Config, remotePath string) {
	if remotePath == "" {
		return
	}
	if strings.ToLower(cfg.TransferMode) == "local" {
		_ = os.Remove(remotePath)
		return
	}
	cmd := exec.Command("ssh", "-i", cfg.LocalSSHKey, "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", cfg.RemoteUser, cfg.RemoteHost), fmt.Sprintf("rm -f '%s'", remotePath))
	_, _ = cmd.CombinedOutput()
}

func copyMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func echo(prefix any, id any) string {
	return fmt.Sprintf("%v_%v_%d", prefix, id, time.Now().UnixNano())
}

// JMBridge implementation moved to jm_pure.go
