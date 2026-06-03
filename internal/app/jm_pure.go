package app

import (
	"archive/zip"
	"context"
	"crypto/aes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
	"io"
	"log"
	mrand "math/rand"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jung-kurt/gofpdf"
	_ "golang.org/x/image/webp"
)

const (
	jmAppVersion       = "2.0.16"
	jmAppTokenSecret   = "18comicAPP"
	jmAppTokenSecret2  = "18comicAPPContent"
	jmAppDataSecret    = "185Hcomic3PAPP7R"
	jmDomainListSecret = "diosfjckwpqpdfjkvnqQjsik"
	jmFallbackScramble = 220980
	jmSplitThreshold1  = 268850
	jmSplitThreshold2  = 421926
	pixelToMM          = 0.2645833333
)

var (
	jmDefaultAPIDomains = []string{
		"www.cdnaspa.vip",
		"www.cdnaspa.club",
		"www.cdnplaystation6.vip",
		"www.cdnplaystation6.cc",
	}
	jmDefaultImageDomains = []string{
		"cdn-msp.jmapiproxy1.cc",
		"cdn-msp.jmapiproxy2.cc",
		"cdn-msp2.jmapiproxy2.cc",
		"cdn-msp3.jmapiproxy2.cc",
		"cdn-msp.jmapinodeudzn.net",
		"cdn-msp3.jmapinodeudzn.net",
	}
	jmDomainServerURLs = []string{
		"https://rup4a04-c01.tos-ap-southeast-1.bytepluses.com/newsvr-2025.txt",
		"https://rup4a04-c02.tos-cn-hongkong.bytepluses.com/newsvr-2025.txt",
	}
)

type JMBridge struct {
	optionPath string
	fileDir    string
	mangaDir   string
	cbzDir     string
	tmpDir     string
	timeoutSec int
	localTest  bool
	proxy      string

	client *http.Client

	mu         sync.RWMutex
	apiDomains []string
	imgDomains []string
	domainsOK  bool
	cookieInit bool

	cbzChapterEnabled bool
	cbzSeriesEnabled  bool
}

func NewJMBridge(optionPath, fileDir, mangaDir, cbzDir, tmpDir string, timeoutSec int, localTest bool, proxy string) *JMBridge {
	if timeoutSec <= 0 {
		timeoutSec = 1800
	}
	if strings.TrimSpace(mangaDir) == "" {
		mangaDir = "./manga/"
	}
	if strings.TrimSpace(cbzDir) == "" {
		cbzDir = "./cbz/"
	}
	if strings.TrimSpace(tmpDir) == "" {
		tmpDir = "./tmp"
	}
	jar, _ := cookiejar.New(nil)
	
	// 创建HTTP客户端，支持代理
	transport := &http.Transport{}
	if proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err == nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}
	
	return &JMBridge{
		optionPath: optionPath,
		fileDir:    fileDir,
		mangaDir:   mangaDir,
		cbzDir:     cbzDir,
		tmpDir:     tmpDir,
		timeoutSec: timeoutSec,
		localTest:  localTest,
		proxy:      proxy,
		client: &http.Client{
			Timeout:   60 * time.Second,
			Jar:       jar,
			Transport: transport,
		},
		apiDomains:        append([]string{}, jmDefaultAPIDomains...),
		imgDomains:        append([]string{}, jmDefaultImageDomains...),
		cbzChapterEnabled: true,
		cbzSeriesEnabled:  true,
	}
}

func (j *JMBridge) SetCBZOptions(chapterEnabled, seriesEnabled bool) {
	j.cbzChapterEnabled = chapterEnabled
	j.cbzSeriesEnabled = seriesEnabled
}

func (j *JMBridge) Download(ctx context.Context, number string) error {
	albumID := toJMID(number)
	if albumID == "" {
		return errors.New("empty album id")
	}
	if err := os.MkdirAll(j.fileDir, 0o755); err != nil {
		return err
	}
	outFile := filepath.Join(j.fileDir, fmt.Sprintf("%s.pdf", albumID))
	return j.DownloadTo(ctx, number, outFile, "")
}

func (j *JMBridge) DownloadTo(ctx context.Context, number, outFile, password string) error {
	if j.localTest {
		return buildTestPDF(outFile, number, password)
	}
	if number == "" {
		return errors.New("empty album id")
	}

	albumData, err := j.reqAPI(ctx, "/album", map[string]string{"id": number})
	if err != nil {
		return err
	}
	albumID := toJMID(anyToString(albumData["id"]))
	if albumID == "" {
		albumID = toJMID(number)
	}

	albumTitle := sanitizeFileName(anyToString(albumData["name"]))
	if albumTitle == "" {
		albumTitle = "JM" + albumID
	}

	episodes := parseEpisodes(albumData)
	if len(episodes) == 0 {
		episodes = []EpisodeMeta{{ID: albumID, Sort: "1", Name: albumTitle}}
	}

	tempDir, err := os.MkdirTemp(j.tmpDir, "jmgo_img_")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	pageFiles := make([]string, 0, 128)
	seriesDir := filepath.Join(j.mangaDir, fmt.Sprintf("JM%s_%s", albumID, albumTitle))
	metaDir := filepath.Join(seriesDir, "_meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return err
	}
	_ = writeJSONFile(filepath.Join(metaDir, "album.json"), albumData)

	for epi, ep := range episodes {
		chapterData, chErr := j.reqAPI(ctx, "/chapter", map[string]string{"id": ep.ID})
		if chErr != nil {
			continue
		}
		_ = writeJSONFile(filepath.Join(metaDir, fmt.Sprintf("chapter_%03d_%s.json", epi+1, toJMID(ep.ID))), chapterData)

		photoID := toJMID(anyToString(chapterData["id"]))
		if photoID == "" {
			photoID = toJMID(ep.ID)
		}
		images := anyToStringSlice(chapterData["images"])
		if len(images) == 0 {
			continue
		}

		scrambleID, _ := j.fetchScrambleID(ctx, photoID)
		if scrambleID == "" {
			scrambleID = strconv.Itoa(jmFallbackScramble)
		}

		chapterTitle := sanitizeFileName(anyToString(chapterData["name"]))
		if chapterTitle == "" {
			chapterTitle = sanitizeFileName(ep.Name)
		}
		if chapterTitle == "" {
			chapterTitle = "chapter"
		}
		chapterDir := filepath.Join(seriesDir, fmt.Sprintf("ch_%03d_%s_%s", epi+1, photoID, chapterTitle))
		if err := os.MkdirAll(chapterDir, 0o755); err != nil {
			return err
		}

		for idx, imgName := range images {
			imgBytes, dlErr := j.downloadImage(ctx, photoID, imgName)
			if dlErr != nil {
				continue
			}
			decoded, _, decErr := image.Decode(bytesReader(imgBytes))
			if decErr != nil {
				continue
			}

			num := calcSegmentationNum(scrambleID, photoID, trimExt(imgName))
			if num > 0 {
				decoded = unscrambleImage(decoded, num)
			}
			decoded = normalizeToRGBA8(decoded)

			outPath := filepath.Join(tempDir, fmt.Sprintf("%04d_%05d.png", epi+1, idx+1))
			f, createErr := os.Create(outPath)
			if createErr != nil {
				continue
			}
			_ = png.Encode(f, decoded)
			_ = f.Close()
			pageFiles = append(pageFiles, outPath)

			// chapter local page for manga readers
			chapterPage := filepath.Join(chapterDir, fmt.Sprintf("%05d.png", idx+1))
			cf, cerr := os.Create(chapterPage)
			if cerr != nil {
				continue
			}
			_ = png.Encode(cf, decoded)
			_ = cf.Close()
		}

		_ = writeComicInfoXML(filepath.Join(chapterDir, "ComicInfo.xml"), buildComicInfo(albumData, chapterData, albumID, photoID, epi+1, len(episodes)))
		if j.cbzChapterEnabled {
			if err := os.MkdirAll(j.cbzDir, 0o755); err == nil {
				chapterCBZ := filepath.Join(j.cbzDir, fmt.Sprintf("JM%s_ch%03d_%s.cbz", albumID, epi+1, chapterTitle))
				_ = zipDirToCBZ(chapterDir, chapterCBZ)
			}
		}
	}

	if len(pageFiles) == 0 {
		return errors.New("no pages downloaded")
	}

	_ = writeComicInfoXML(filepath.Join(seriesDir, "ComicInfo.xml"), buildComicInfo(albumData, nil, albumID, "", 0, len(episodes)))
	if j.cbzSeriesEnabled {
		if err := os.MkdirAll(j.cbzDir, 0o755); err == nil {
			seriesCBZ := filepath.Join(j.cbzDir, fmt.Sprintf("JM%s_%s.cbz", albumID, albumTitle))
			_ = zipDirToCBZ(seriesDir, seriesCBZ)
		}
	}

	sort.Strings(pageFiles)
	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil {
		return err
	}
	return buildPDF(outFile, pageFiles, password)
}

func (j *JMBridge) GetAlbum(ctx context.Context, number string) (*Album, error) {
	if j.localTest {
		id := toJMID(number)
		if id == "" {
			id = "123456"
		}
		return &Album{
			ID:          id,
			Title:       "本地测试本子 JM" + id,
			Description: "local-test",
			Tags:        []string{"local-test"},
			Episodes:    1,
			Views:       "0",
		}, nil
	}
	number = toJMID(number)
	if number == "" {
		return nil, errors.New("invalid id")
	}
	data, err := j.reqAPI(ctx, "/album", map[string]string{"id": number})
	if err != nil {
		return nil, err
	}
	return toAlbum(data), nil
}

func (j *JMBridge) SearchBestAlbum(ctx context.Context, keyword string) (*Album, error) {
	if j.localTest {
		return j.GetAlbum(ctx, "123456")
	}
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return nil, errors.New("empty keyword")
	}
	data, err := j.reqAPI(ctx, "/search", map[string]string{
		"main_tag":     "0",
		"search_query": keyword,
		"page":         "1",
		"o":            "mr",
		"t":            "a",
	})
	if err != nil {
		return nil, err
	}

	if redirectID := toJMID(anyToString(data["redirect_aid"])); redirectID != "" {
		return j.GetAlbum(ctx, redirectID)
	}

	content, ok := data["content"].([]any)
	if !ok || len(content) == 0 {
		return nil, errors.New("not found")
	}

	bestID := ""
	bestScore := -1.0
	for _, item := range content {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id := toJMID(anyToString(row["id"]))
		title := anyToString(row["name"])
		score := similarityRatio(keyword, title)
		if score > bestScore {
			bestID = id
			bestScore = score
		}
	}
	if bestID == "" {
		return nil, errors.New("not found")
	}
	return j.GetAlbum(ctx, bestID)
}

func (j *JMBridge) ensureDomains(ctx context.Context) {
	j.mu.RLock()
	if j.domainsOK {
		j.mu.RUnlock()
		return
	}
	j.mu.RUnlock()

	newDomains := j.fetchLatestDomains(ctx)
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(newDomains) > 0 {
		j.apiDomains = mergeDomains(newDomains, jmDefaultAPIDomains)
	}
	j.domainsOK = true
}

func (j *JMBridge) fetchLatestDomains(ctx context.Context) []string {
	for _, u := range jmDomainServerURLs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		resp, err := j.client.Do(req)
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		text := strings.TrimSpace(string(raw))
		for len(text) > 0 && text[0] > 127 {
			text = text[1:]
		}
		decoded, err := decodeRespData(text, "", jmDomainListSecret)
		if err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(decoded), &m); err != nil {
			continue
		}
		server, ok := m["Server"].([]any)
		if !ok || len(server) == 0 {
			continue
		}
		ret := make([]string, 0, len(server))
		for _, v := range server {
			s := strings.TrimSpace(anyToString(v))
			if s != "" {
				ret = append(ret, s)
			}
		}
		if len(ret) > 0 {
			return ret
		}
	}
	return nil
}

func (j *JMBridge) reqAPI(ctx context.Context, path string, query map[string]string) (map[string]any, error) {
	j.ensureDomains(ctx)
	j.ensureCookies(ctx)
	domains := mergeDomains(j.getAPIDomains(), jmDefaultAPIDomains)
	var lastErr error
	lastBody := ""
	lastURL := ""

	for _, domain := range domains {
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		token := md5hex(ts + jmAppTokenSecret)
		tokenParam := ts + "," + jmAppVersion

		u, err := url.Parse("https://" + domain + path)
		if err != nil {
			lastErr = err
			continue
		}
		q := u.Query()
		for k, v := range query {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			lastErr = err
			continue
		}
		for k, v := range apiHeaders(token, tokenParam) {
			req.Header.Set(k, v)
		}

		resp, err := j.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		lastBody = strings.TrimSpace(string(raw))
		lastURL = u.String()
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status=%d", resp.StatusCode)
			continue
		}

		outer, err := parseJSONObject(raw)
		if err != nil {
			lastErr = fmt.Errorf("%w, url=%s, body_prefix=%s", err, lastURL, limitText(lastBody, 240))
			continue
		}
		if toInt64(outer["code"]) != 200 {
			lastErr = fmt.Errorf("api code=%v", outer["code"])
			continue
		}
		encData := anyToString(outer["data"])
		if encData == "" {
			lastErr = errors.New("empty data")
			continue
		}
		decoded, err := decodeRespData(encData, ts, jmAppDataSecret)
		if err != nil {
			lastErr = err
			continue
		}
		var model map[string]any
		if err := json.Unmarshal([]byte(decoded), &model); err != nil {
			lastErr = err
			continue
		}
		return model, nil
	}

	if lastErr == nil {
		lastErr = errors.New("request failed")
	}
	return nil, lastErr
}

func limitText(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

func (j *JMBridge) ensureCookies(ctx context.Context) {
	j.mu.RLock()
	if j.cookieInit {
		j.mu.RUnlock()
		return
	}
	j.mu.RUnlock()

	domains := mergeDomains(j.getAPIDomains(), jmDefaultAPIDomains)
	for _, domain := range domains {
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		token := md5hex(ts + jmAppTokenSecret)
		tokenParam := ts + "," + jmAppVersion

		u := "https://" + domain + "/setting"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		for k, v := range apiHeaders(token, tokenParam) {
			req.Header.Set(k, v)
		}
		resp, err := j.client.Do(req)
		if err != nil {
			continue
		}
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			continue
		}
		j.mu.Lock()
		j.cookieInit = true
		j.mu.Unlock()
		return
	}
}

func (j *JMBridge) fetchScrambleID(ctx context.Context, photoID string) (string, error) {
	domains := mergeDomains(j.getAPIDomains(), jmDefaultAPIDomains)
	pat := regexp.MustCompile(`var\s+scramble_id\s*=\s*(\d+);`)
	for _, domain := range domains {
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		token := md5hex(ts + jmAppTokenSecret2)
		tokenParam := ts + "," + jmAppVersion

		u, _ := url.Parse("https://" + domain + "/chapter_view_template")
		q := u.Query()
		q.Set("id", photoID)
		q.Set("mode", "vertical")
		q.Set("page", "0")
		q.Set("app_img_shunt", "1")
		q.Set("express", "off")
		q.Set("v", ts)
		u.RawQuery = q.Encode()

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		for k, v := range apiHeaders(token, tokenParam) {
			req.Header.Set(k, v)
		}

		resp, err := j.client.Do(req)
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			continue
		}
		m := pat.FindSubmatch(raw)
		if len(m) >= 2 {
			return string(m[1]), nil
		}
	}
	return strconv.Itoa(jmFallbackScramble), nil
}

func (j *JMBridge) downloadImage(ctx context.Context, photoID, imgName string) ([]byte, error) {
	domains := j.getImageDomains()
	if len(domains) == 0 {
		return nil, errors.New("no image domains")
	}
	first := domains[mrand.Intn(len(domains))]
	ordered := append([]string{first}, domains...)
	tried := map[string]struct{}{}

	for _, domain := range ordered {
		if _, ok := tried[domain]; ok {
			continue
		}
		tried[domain] = struct{}{}

		u := fmt.Sprintf("https://%s/media/photos/%s/%s", domain, photoID, imgName)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		for k, v := range imageHeaders(j.getRefererDomain()) {
			req.Header.Set(k, v)
		}
		resp, err := j.client.Do(req)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK && len(b) > 0 {
			return b, nil
		}
	}
	return nil, errors.New("download image failed")
}

func (j *JMBridge) getRefererDomain() string {
	apis := j.getAPIDomains()
	if len(apis) == 0 {
		return "www.cdnaspa.vip"
	}
	return apis[0]
}

func (j *JMBridge) getAPIDomains() []string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if len(j.apiDomains) == 0 {
		return append([]string{}, jmDefaultAPIDomains...)
	}
	return append([]string{}, j.apiDomains...)
}

func (j *JMBridge) getImageDomains() []string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if len(j.imgDomains) == 0 {
		return append([]string{}, jmDefaultImageDomains...)
	}
	return append([]string{}, j.imgDomains...)
}

func mergeDomains(primary, fallback []string) []string {
	out := make([]string, 0, len(primary)+len(fallback))
	seen := map[string]struct{}{}
	add := func(ls []string) {
		for _, d := range ls {
			d = strings.TrimSpace(d)
			if d == "" {
				continue
			}
			if _, ok := seen[d]; ok {
				continue
			}
			seen[d] = struct{}{}
			out = append(out, d)
		}
	}
	add(primary)
	add(fallback)
	return out
}

func apiHeaders(token, tokenParam string) map[string]string {
	return map[string]string{
		"user-agent": "Mozilla/5.0 (Linux; Android 9; V1938CT Build/PQ3A.190705.11211812; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Safari/537.36",
		"token":      token,
		"tokenparam": tokenParam,
	}
}

func imageHeaders(refererDomain string) map[string]string {
	return map[string]string{
		"Accept":           "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
		"X-Requested-With": "com.JMComic3.app",
		"Referer":          "https://" + refererDomain,
		"Accept-Language":  "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
		"user-agent":       "Mozilla/5.0 (Linux; Android 9; V1938CT Build/PQ3A.190705.11211812; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Safari/537.36",
	}
}

type EpisodeMeta struct {
	ID   string
	Sort string
	Name string
}

type ComicInfo struct {
	Title       string
	Series      string
	Number      string
	Summary     string
	Writer      string
	Genre       string
	Web         string
	PageCount   int
	LanguageISO string
	Year        string
	Month       string
	Day         string
	ScanInfo    string
	Notes       string
}

func parseEpisodes(albumData map[string]any) []EpisodeMeta {
	series, ok := albumData["series"].([]any)
	if !ok {
		return nil
	}
	out := make([]EpisodeMeta, 0, len(series))
	for _, item := range series {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id := toJMID(anyToString(m["id"]))
		if id == "" {
			continue
		}
		out = append(out, EpisodeMeta{
			ID:   id,
			Sort: strings.TrimSpace(anyToString(m["sort"])),
			Name: strings.TrimSpace(anyToString(m["name"])),
		})
	}
	return out
}

func parseEpisodeIDs(albumData map[string]any) []string {
	eps := parseEpisodes(albumData)
	ids := make([]string, 0, len(eps))
	for _, ep := range eps {
		if ep.ID != "" {
			ids = append(ids, ep.ID)
		}
	}
	return ids
}

func toAlbum(data map[string]any) *Album {
	episodes := parseEpisodeIDs(data)
	if len(episodes) == 0 {
		episodes = []string{toJMID(anyToString(data["id"]))}
	}
	return &Album{
		ID:          toJMID(anyToString(data["id"])),
		Title:       anyToString(data["name"]),
		Description: anyToString(data["description"]),
		Tags:        anyToStringSlice(data["tags"]),
		Episodes:    len(episodes),
		Views:       anyToString(data["total_views"]),
	}
}

func writeJSONFile(path string, data any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func buildComicInfo(albumData, chapterData map[string]any, albumID, photoID string, chapterIndex, totalChapters int) ComicInfo {
	title := strings.TrimSpace(anyToString(albumData["name"]))
	chapterTitle := ""
	if chapterData != nil {
		chapterTitle = strings.TrimSpace(anyToString(chapterData["name"]))
	}
	displayTitle := title
	if chapterTitle != "" {
		displayTitle = chapterTitle
	}

	authors := strings.Join(anyToStringSlice(albumData["author"]), ", ")
	if authors == "" {
		authors = strings.Join(anyToStringSlice(albumData["authors"]), ", ")
	}
	genres := strings.Join(anyToStringSlice(albumData["tags"]), ", ")
	summary := strings.TrimSpace(anyToString(albumData["description"]))
	if summary == "" {
		summary = "JM Comic"
	}

	number := ""
	if chapterIndex > 0 {
		number = strconv.Itoa(chapterIndex)
	}
	pageCount := 0
	if chapterData != nil {
		pageCount = len(anyToStringSlice(chapterData["images"]))
	}

	pub := strings.TrimSpace(anyToString(albumData["pub_date"]))
	year, month, day := splitDate(pub)

	notes := []string{
		"source=jmcomic-api",
		"album_id=" + albumID,
	}
	if photoID != "" {
		notes = append(notes, "photo_id="+photoID)
	}
	if totalChapters > 0 {
		notes = append(notes, "total_chapters="+strconv.Itoa(totalChapters))
	}

	return ComicInfo{
		Title:       displayTitle,
		Series:      title,
		Number:      number,
		Summary:     summary,
		Writer:      authors,
		Genre:       genres,
		Web:         fmt.Sprintf("https://18comic.vip/album/%s/", albumID),
		PageCount:   pageCount,
		LanguageISO: "zh",
		Year:        year,
		Month:       month,
		Day:         day,
		ScanInfo:    "JM",
		Notes:       strings.Join(notes, "; "),
	}
}

func writeComicInfoXML(path string, ci ComicInfo) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	xml := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Title>%s</Title>
  <Series>%s</Series>
  <Number>%s</Number>
  <Summary>%s</Summary>
  <Writer>%s</Writer>
  <Genre>%s</Genre>
  <Web>%s</Web>
  <PageCount>%d</PageCount>
  <LanguageISO>%s</LanguageISO>
  <Year>%s</Year>
  <Month>%s</Month>
  <Day>%s</Day>
  <ScanInformation>%s</ScanInformation>
  <Notes>%s</Notes>
</ComicInfo>
`,
		xmlEscape(ci.Title),
		xmlEscape(ci.Series),
		xmlEscape(ci.Number),
		xmlEscape(ci.Summary),
		xmlEscape(ci.Writer),
		xmlEscape(ci.Genre),
		xmlEscape(ci.Web),
		ci.PageCount,
		xmlEscape(ci.LanguageISO),
		xmlEscape(ci.Year),
		xmlEscape(ci.Month),
		xmlEscape(ci.Day),
		xmlEscape(ci.ScanInfo),
		xmlEscape(ci.Notes),
	)
	return os.WriteFile(path, []byte(xml), 0o644)
}

func xmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return r.Replace(s)
}

func splitDate(s string) (string, string, string) {
	if s == "" {
		return "", "", ""
	}
	re := regexp.MustCompile(`(\d{4})[-/](\d{1,2})[-/](\d{1,2})`)
	m := re.FindStringSubmatch(s)
	if len(m) == 4 {
		return m[1], m[2], m[3]
	}
	return "", "", ""
}

func zipDirToCBZ(srcDir, outCBZ string) error {
	if err := os.MkdirAll(filepath.Dir(outCBZ), 0o755); err != nil {
		return err
	}
	f, err := os.Create(outCBZ)
	if err != nil {
		return err
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	defer zw.Close()

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		h, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		h.Name = rel
		h.Method = zip.Deflate
		w, err := zw.CreateHeader(h)
		if err != nil {
			return err
		}
		r, err := os.Open(path)
		if err != nil {
			return err
		}
		defer r.Close()
		_, err = io.Copy(w, r)
		return err
	})
}

func anyToString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		if float64(int64(t)) == t {
			return strconv.FormatInt(int64(t), 10)
		}
		return fmt.Sprintf("%v", t)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func anyToStringSlice(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		s := strings.TrimSpace(anyToString(item))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func parseJSONObject(raw []byte) (map[string]any, error) {
	text := strings.TrimSpace(string(raw))
	if strings.HasPrefix(text, "{") {
		var m map[string]any
		if err := json.Unmarshal([]byte(text), &m); err == nil {
			return m, nil
		}
	}
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start >= 0 && end > start {
		var m map[string]any
		if err := json.Unmarshal([]byte(text[start:end+1]), &m); err == nil {
			return m, nil
		}
	}
	return nil, errors.New("json parse failed")
}

func decodeRespData(data, ts, secret string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(data))
	if err != nil {
		return "", err
	}
	key := []byte(md5hex(ts + secret))
	if len(raw)%aes.BlockSize != 0 {
		return "", errors.New("invalid aes data length")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	out := make([]byte, len(raw))
	for bs, be := 0, aes.BlockSize; bs < len(raw); bs, be = bs+aes.BlockSize, be+aes.BlockSize {
		block.Decrypt(out[bs:be], raw[bs:be])
	}
	if len(out) == 0 {
		return "", errors.New("empty decrypted data")
	}
	pad := int(out[len(out)-1])
	if pad <= 0 || pad > aes.BlockSize || pad > len(out) {
		return "", errors.New("invalid padding")
	}
	for i := len(out) - pad; i < len(out); i++ {
		if int(out[i]) != pad {
			return "", errors.New("invalid pkcs7")
		}
	}
	return string(out[:len(out)-pad]), nil
}

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func buildPDF(outFile string, imageFiles []string, password string) error {
	pdf := gofpdf.NewCustom(&gofpdf.InitType{UnitStr: "mm"})
	if strings.TrimSpace(password) != "" {
		perm := gofpdf.CnProtectPrint | gofpdf.CnProtectModify | gofpdf.CnProtectCopy | gofpdf.CnProtectAnnotForms
		pdf.SetProtection(byte(perm), password, password)
	}

	for _, file := range imageFiles {
		// 检测图片实际格式
		imgType, err := detectImageType(file)
		if err != nil {
			log.Printf("skip image %s: %v", file, err)
			continue
		}

		// 如果扩展名与实际格式不匹配，转换图片
		actualFile := file
		ext := strings.ToLower(filepath.Ext(file))
		if (ext == ".jpg" || ext == ".jpeg") && imgType != "JPG" {
			// 扩展名是jpg但实际不是jpg，需要转换
			converted, convErr := convertToJPEG(file)
			if convErr != nil {
				log.Printf("convert image failed %s: %v", file, convErr)
				continue
			}
			actualFile = converted
			defer os.Remove(converted)
			imgType = "JPG"
		}

		f, err := os.Open(actualFile)
		if err != nil {
			return err
		}
		cfg, _, err := image.DecodeConfig(f)
		_ = f.Close()
		if err != nil {
			return err
		}

		// 页面尺寸 = 图片尺寸
		wmm := float64(cfg.Width) * pixelToMM
		hmm := float64(cfg.Height) * pixelToMM

		// 始终使用纵向，通过SizeType控制实际尺寸
		pdf.AddPageFormat("P", gofpdf.SizeType{Wd: wmm, Ht: hmm})

		// 图片填满页面
		pdf.ImageOptions(actualFile, 0, 0, wmm, hmm, false, gofpdf.ImageOptions{ImageType: imgType}, 0, "")
	}
	return pdf.OutputFileAndClose(outFile)
}

func detectImageType(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// 读取文件头
	header := make([]byte, 12)
	_, err = f.Read(header)
	if err != nil {
		return "", err
	}

	// 检测格式
	if header[0] == 0xFF && header[1] == 0xD8 {
		return "JPG", nil
	}
	if header[0] == 0x89 && header[1] == 0x50 && header[2] == 0x4E && header[3] == 0x47 {
		return "PNG", nil
	}
	if string(header[0:4]) == "RIFF" && string(header[8:12]) == "WEBP" {
		return "WEBP", nil
	}
	return "", fmt.Errorf("unknown image format")
}

func convertToJPEG(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	img, _, err := image.Decode(f)
	if err != nil {
		return "", err
	}

	tmpFile := strings.TrimSuffix(file, filepath.Ext(file)) + "_converted.jpg"
	out, err := os.Create(tmpFile)
	if err != nil {
		return "", err
	}
	defer out.Close()

	return tmpFile, jpeg.Encode(out, img, &jpeg.Options{Quality: 90})
}

// imageToPDF 把图片转换为PDF文件
func imageToPDF(imagePath, pdfPath string) error {
	f, err := os.Open(imagePath)
	if err != nil {
		return err
	}
	cfg, _, err := image.DecodeConfig(f)
	f.Close()
	if err != nil {
		return err
	}

	pdf := gofpdf.NewCustom(&gofpdf.InitType{UnitStr: "mm"})
	wmm := float64(cfg.Width) * pixelToMM
	hmm := float64(cfg.Height) * pixelToMM
	pdf.AddPageFormat("P", gofpdf.SizeType{Wd: wmm, Ht: hmm})

	ext := strings.ToLower(filepath.Ext(imagePath))
	imgType := "PNG"
	if ext == ".jpg" || ext == ".jpeg" {
		imgType = "JPG"
	} else if ext == ".webp" {
		imgType = "WEBP"
	}

	pdf.ImageOptions(imagePath, 0, 0, wmm, hmm, false, gofpdf.ImageOptions{ImageType: imgType}, 0, "")
	return pdf.OutputFileAndClose(pdfPath)
}

func buildTestPDF(outFile, number, password string) error {
	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil {
		return err
	}
	pdf := gofpdf.New("P", "mm", "A4", "")
	if strings.TrimSpace(password) != "" {
		perm := gofpdf.CnProtectPrint | gofpdf.CnProtectModify | gofpdf.CnProtectCopy | gofpdf.CnProtectAnnotForms
		pdf.SetProtection(byte(perm), password, password)
	}
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 18)
	pdf.Cell(0, 14, "JM Local Test PDF")
	pdf.Ln(16)
	pdf.SetFont("Arial", "", 13)
	pdf.MultiCell(0, 8, fmt.Sprintf("album_id=JM%s\ncreated_at=%s", toJMID(number), time.Now().Format(time.RFC3339)), "", "L", false)
	return pdf.OutputFileAndClose(outFile)
}

func toJMID(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "JM"), "jm")
	digits := regexp.MustCompile(`\d+`).FindString(s)
	return digits
}

func trimExt(name string) string {
	ext := filepath.Ext(name)
	if ext == "" {
		return name
	}
	return strings.TrimSuffix(name, ext)
}

func calcSegmentationNum(scrambleID, aid, filename string) int {
	sid, _ := strconv.Atoi(toJMID(scrambleID))
	id, _ := strconv.Atoi(toJMID(aid))
	if sid <= 0 || id <= 0 {
		return 0
	}
	if id < sid {
		return 0
	}
	if id < jmSplitThreshold1 {
		return 10
	}
	x := 10
	if id >= jmSplitThreshold2 {
		x = 8
	}
	h := md5hex(fmt.Sprintf("%d%s", id, filename))
	if h == "" {
		return 0
	}
	n := int(h[len(h)-1])
	n = n % x
	n = n*2 + 2
	return n
}

func unscrambleImage(src image.Image, num int) image.Image {
	if num <= 1 {
		return src
	}
	b := src.Bounds()
	w := b.Dx()
	h := b.Dy()
	if w <= 0 || h <= 0 {
		return src
	}

	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	over := h % num
	for i := 0; i < num; i++ {
		move := h / num
		ySrc := h - (move * (i + 1)) - over
		yDst := move * i
		if i == 0 {
			move += over
		} else {
			yDst += over
		}
		if ySrc < 0 {
			ySrc = 0
		}
		if ySrc+move > h {
			move = h - ySrc
		}
		if move <= 0 {
			continue
		}
		dstRect := image.Rect(0, yDst, w, yDst+move)
		draw.Draw(dst, dstRect, src, image.Point{X: 0, Y: ySrc}, draw.Src)
	}
	return dst
}

func normalizeToRGBA8(src image.Image) image.Image {
	b := src.Bounds()
	dst := image.NewRGBA(image.Rect(0, 0, b.Dx(), b.Dy()))
	draw.Draw(dst, dst.Bounds(), src, b.Min, draw.Src)
	return dst
}

func similarityRatio(a, b string) float64 {
	ar := []rune(strings.ToLower(strings.TrimSpace(a)))
	br := []rune(strings.ToLower(strings.TrimSpace(b)))
	if len(ar) == 0 && len(br) == 0 {
		return 1
	}
	if len(ar) == 0 || len(br) == 0 {
		return 0
	}
	d := levenshtein(ar, br)
	maxLen := len(ar)
	if len(br) > maxLen {
		maxLen = len(br)
	}
	return 1 - (float64(d) / float64(maxLen))
}

func levenshtein(a, b []rune) int {
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr := make([]int, len(b)+1)
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			curr[j] = min3(
				curr[j-1]+1,
				prev[j]+1,
				prev[j-1]+cost,
			)
		}
		prev = curr
	}
	return prev[len(b)]
}

func min3(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= a && b <= c {
		return b
	}
	return c
}

func bytesReader(b []byte) *byteReader {
	return &byteReader{b: b}
}

type byteReader struct {
	b []byte
	i int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
