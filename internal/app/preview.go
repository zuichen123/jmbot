package app

import (
	"archive/zip"
	"compress/gzip"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type previewBook struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	ModTime   time.Time `json:"mod_time"`
	PageCount int       `json:"page_count"`
	Path      string    `json:"-"`
}

type previewMetaResp struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	PageCount int    `json:"page_count"`
	Download  string `json:"download"`
}

var (
	jmIDInNameRe  = regexp.MustCompile(`(?i)jm[\s_-]*([0-9]{3,})`)
	bikaIDInNameRe = regexp.MustCompile(`(?i)^bika_([a-f0-9]{24,})`)
	plainIDNameRe = regexp.MustCompile(`(?:^|[^0-9])([0-9]{5,})(?:[^0-9]|$)`)

	previewBooksCacheMu sync.RWMutex
	previewBooksCache   []previewBook
	previewBooksCacheAt time.Time

	previewMangaCacheMu sync.RWMutex
	previewMangaCache   map[string]previewMangaPages

	previewCBZPageCacheMu sync.Mutex
	previewCBZPageCache   = map[string]previewCBZPage{}
)

type previewMangaPages struct {
	Pages     []string
	ExpiresAt time.Time
}

type previewCBZPage struct {
	Raw       []byte
	Ext       string
	ExpiresAt time.Time
}

type cachedCBZReader struct {
	Reader    *zip.ReadCloser
	Entries   []*zip.File
	ModTime   time.Time
	Size      int64
	ExpiresAt time.Time
}

var (
	cachedCBZReadersMu sync.Mutex
	cachedCBZReaders   = map[string]*cachedCBZReader{}
)

func getCachedCBZReader(cbzPath string) (*cachedCBZReader, error) {
	st, err := os.Stat(cbzPath)
	if err != nil {
		return nil, err
	}
	modTime := st.ModTime()
	size := st.Size()

	cachedCBZReadersMu.Lock()
	defer cachedCBZReadersMu.Unlock()

	if cached, ok := cachedCBZReaders[cbzPath]; ok {
		if cached.ModTime == modTime && cached.Size == size && time.Now().Before(cached.ExpiresAt) {
			return cached, nil
		}
		cached.Reader.Close()
		delete(cachedCBZReaders, cbzPath)
	}

	r, err := zip.OpenReader(cbzPath)
	if err != nil {
		return nil, err
	}
	entries := collectImageEntries(r.File)
	cached := &cachedCBZReader{
		Reader:    r,
		Entries:   entries,
		ModTime:   modTime,
		Size:      size,
		ExpiresAt: time.Now().Add(10 * time.Minute),
	}
	cachedCBZReaders[cbzPath] = cached

	// 清理过期缓存
	if len(cachedCBZReaders) > 64 {
		now := time.Now()
		for k, v := range cachedCBZReaders {
			if now.After(v.ExpiresAt) {
				v.Reader.Close()
				delete(cachedCBZReaders, k)
			}
		}
	}

	return cached, nil
}

func generateETag(modTime time.Time, size int64) string {
	h := md5.Sum([]byte(fmt.Sprintf("%d-%d", modTime.UnixNano(), size)))
	return fmt.Sprintf(`"%x"`, h)
}

func (a *App) registerPreviewRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/search", a.gzipMiddleware(a.handlePreviewSearch))
	mux.HandleFunc("/api/comic/", a.gzipMiddleware(a.handlePreviewComicAPI))
	mux.HandleFunc("/", a.handlePreviewPage)
}

func (a *App) gzipMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := &gzipResponseWriter{Writer: gz, ResponseWriter: w}
		next(gzw, r)
	}
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w *gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (a *App) handlePreviewPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	p := strings.Trim(r.URL.Path, "/")
	if p == "" {
		writeHTML(w, previewHomeHTML())
		return
	}
	if id, ok := parseJMPathID(p); ok {
		writeHTML(w, previewViewerHTML(id))
		return
	}
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte("not found"))
}

func (a *App) handlePreviewSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	books, err := a.listPreviewBooks()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if q != "" {
		lq := strings.ToLower(q)
		filtered := make([]previewBook, 0, len(books))
		for _, b := range books {
			if strings.Contains(strings.ToLower(b.ID), lq) || strings.Contains(strings.ToLower(b.Title), lq) || strings.Contains(strings.ToLower(b.Name), lq) {
				filtered = append(filtered, b)
			}
		}
		books = filtered
	}
	if len(books) > 100 {
		books = books[:100]
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": books})
}

func (a *App) handlePreviewComicAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	remain := strings.TrimPrefix(r.URL.Path, "/api/comic/")
	parts := strings.Split(strings.Trim(remain, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	id := normalizeJMID(parts[0])
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("invalid id"))
		return
	}
	book, hasCBZ, err := a.findBookByID(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	if len(parts) >= 2 && parts[1] == "download" {
		if !hasCBZ {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "cbz not found"})
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(book.Path)))
		http.ServeFile(w, r, book.Path)
		return
	}

	if len(parts) >= 3 && parts[1] == "page" {
		pageNo, err := strconv.Atoi(parts[2])
		if err != nil || pageNo <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("invalid page"))
			return
		}
		if mangaPath, ok, err := a.findMangaPageByID(id, pageNo); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(err.Error()))
			return
		} else if ok {
			w.Header().Set("Cache-Control", "public, max-age=3600")
			http.ServeFile(w, r, mangaPath)
			return
		}
		if !hasCBZ {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("comic not found"))
			return
		}
		// 生成ETag用于CBZ页面缓存
		cbzStat, statErr := os.Stat(book.Path)
		if statErr == nil {
			etag := generateETag(cbzStat.ModTime(), int64(pageNo))
			if match := r.Header.Get("If-None-Match"); match == etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", etag)
		}
		raw, ext, err := readCBZPage(book.Path, pageNo)
		if err != nil {
			if strings.Contains(err.Error(), "out of range") {
				w.WriteHeader(http.StatusNotFound)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		ct := mime.TypeByExtension(ext)
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(raw)
		return
	}

	pageCount, hasManga, err := a.countMangaPagesByID(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if !hasManga {
		if !hasCBZ {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "comic not found"})
			return
		}
		pageCount, err = countCBZPages(book.Path)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
	}
	title := "JM" + id
	download := ""
	if hasCBZ {
		title = book.Title
		download = fmt.Sprintf("/api/comic/%s/download", id)
	}
	writeJSON(w, http.StatusOK, previewMetaResp{
		ID:        id,
		Title:     title,
		PageCount: pageCount,
		Download:  download,
	})
}

func (a *App) listPreviewBooks() ([]previewBook, error) {
	previewBooksCacheMu.RLock()
	if time.Since(previewBooksCacheAt) < 60*time.Second && len(previewBooksCache) > 0 {
		out := make([]previewBook, len(previewBooksCache))
		copy(out, previewBooksCache)
		previewBooksCacheMu.RUnlock()
		return out, nil
	}
	previewBooksCacheMu.RUnlock()

	cfg := a.currentConfig()
	root := strings.TrimSpace(cfg.CBZDir)
	if root == "" {
		root = "./cbz/"
	}
	entries := make([]previewBook, 0, 64)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(d.Name()), ".cbz") {
			return nil
		}
		id := extractIDFromName(d.Name())
		if id == "" {
			return nil
		}
		st, stErr := os.Stat(path)
		if stErr != nil {
			return nil
		}
		entries = append(entries, previewBook{
			ID:      id,
			Title:   deriveTitleFromName(d.Name(), id),
			Name:    d.Name(),
			Size:    st.Size(),
			ModTime: st.ModTime(),
			Path:    path,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	best := map[string]previewBook{}
	for _, b := range entries {
		cur, ok := best[b.ID]
		if !ok || scorePreviewBook(b) > scorePreviewBook(cur) {
			best[b.ID] = b
		}
	}
	out := make([]previewBook, 0, len(best))
	for _, b := range best {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ModTime.After(out[j].ModTime)
	})

	previewBooksCacheMu.Lock()
	previewBooksCache = make([]previewBook, len(out))
	copy(previewBooksCache, out)
	previewBooksCacheAt = time.Now()
	previewBooksCacheMu.Unlock()

	return out, nil
}

func (a *App) findBookByID(id string) (previewBook, bool, error) {
	books, err := a.listPreviewBooks()
	if err != nil {
		return previewBook{}, false, err
	}
	for _, b := range books {
		if b.ID == id {
			return b, true, nil
		}
	}
	return previewBook{}, false, nil
}

func extractIDFromName(name string) string {
	if m := jmIDInNameRe.FindStringSubmatch(name); len(m) > 1 {
		return normalizeJMID(m[1])
	}
	if m := bikaIDInNameRe.FindStringSubmatch(name); len(m) > 1 {
		return "bika_" + m[1]
	}
	if m := plainIDNameRe.FindStringSubmatch(name); len(m) > 1 {
		return normalizeJMID(m[1])
	}
	return ""
}

func normalizeJMID(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	s = strings.TrimPrefix(s, "jm")
	re := regexp.MustCompile(`^[0-9]{3,}$`)
	if !re.MatchString(s) {
		return ""
	}
	return s
}

func parseJMPathID(pathVal string) (string, bool) {
	p := strings.Split(strings.TrimSpace(pathVal), "/")[0]
	id := normalizeJMID(p)
	return id, id != ""
}

func deriveTitleFromName(name, id string) string {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	re := regexp.MustCompile(`(?i)^jm[\s_-]*` + regexp.QuoteMeta(id) + `[\s_-]*`)
	base = re.ReplaceAllString(base, "")
	base = strings.TrimSpace(base)
	if base == "" {
		base = "JM" + id
	}
	return base
}

func scorePreviewBook(b previewBook) int {
	score := 0
	lower := strings.ToLower(b.Name)
	if !strings.Contains(lower, "_ch") && !strings.Contains(lower, "ch00") && !strings.Contains(lower, "ch0") {
		score += 20
	}
	if strings.HasPrefix(lower, "jm"+b.ID+"_") {
		score += 10
	}
	if b.Size > 0 {
		score += int(b.Size / (1024 * 1024))
	}
	return score
}

func countCBZPages(cbzPath string) (int, error) {
	cached, err := getCachedCBZReader(cbzPath)
	if err != nil {
		return 0, err
	}
	return len(cached.Entries), nil
}

func readCBZPage(cbzPath string, pageNo int) ([]byte, string, error) {
	if pageNo > 0 {
		if raw, ext, ok := getCachedCBZPage(cbzPath, pageNo); ok {
			return raw, ext, nil
		}
	}
	cached, err := getCachedCBZReader(cbzPath)
	if err != nil {
		return nil, "", err
	}
	if pageNo <= 0 || pageNo > len(cached.Entries) {
		return nil, "", fmt.Errorf("page out of range")
	}
	target := cached.Entries[pageNo-1]
	rc, err := target.Open()
	if err != nil {
		return nil, "", err
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return nil, "", err
	}
	ext := strings.ToLower(filepath.Ext(target.Name))
	setCachedCBZPage(cbzPath, pageNo, raw, ext)
	return raw, ext, nil
}

func collectImageEntries(files []*zip.File) []*zip.File {
	out := make([]*zip.File, 0, len(files))
	for _, f := range files {
		if f.FileInfo().IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(f.Name))
		switch ext {
		case ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif":
			out = append(out, f)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func getCachedCBZPage(cbzPath string, pageNo int) ([]byte, string, bool) {
	key := fmt.Sprintf("%s|%d", cbzPath, pageNo)
	now := time.Now()
	previewCBZPageCacheMu.Lock()
	defer previewCBZPageCacheMu.Unlock()
	item, ok := previewCBZPageCache[key]
	if !ok || now.After(item.ExpiresAt) {
		delete(previewCBZPageCache, key)
		return nil, "", false
	}
	return item.Raw, item.Ext, true
}

func setCachedCBZPage(cbzPath string, pageNo int, raw []byte, ext string) {
	key := fmt.Sprintf("%s|%d", cbzPath, pageNo)
	cp := make([]byte, len(raw))
	copy(cp, raw)

	previewCBZPageCacheMu.Lock()
	defer previewCBZPageCacheMu.Unlock()
	now := time.Now()
	previewCBZPageCache[key] = previewCBZPage{
		Raw:       cp,
		Ext:       ext,
		ExpiresAt: now.Add(30 * time.Minute),
	}
	if len(previewCBZPageCache) <= 1024 {
		return
	}
	for k, v := range previewCBZPageCache {
		if now.After(v.ExpiresAt) {
			delete(previewCBZPageCache, k)
		}
	}
	if len(previewCBZPageCache) > 1024 {
		previewCBZPageCache = map[string]previewCBZPage{}
	}
}

func (a *App) countMangaPagesByID(id string) (int, bool, error) {
	pages, ok, err := a.listMangaPagesByID(id)
	if err != nil {
		return 0, false, err
	}
	return len(pages), ok && len(pages) > 0, nil
}

func (a *App) findMangaPageByID(id string, pageNo int) (string, bool, error) {
	pages, ok, err := a.listMangaPagesByID(id)
	if err != nil {
		return "", false, err
	}
	if !ok || pageNo <= 0 || pageNo > len(pages) {
		return "", false, nil
	}
	return pages[pageNo-1], true, nil
}

func (a *App) listMangaPagesByID(id string) ([]string, bool, error) {
	cfg := a.currentConfig()
	root := strings.TrimSpace(cfg.MangaDir)
	if root == "" {
		root = "./manga/"
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	bestDir := ""
	var bestModTime time.Time
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		dirID := extractIDFromName(name)
		if dirID != id {
			continue
		}
		st, stErr := os.Stat(filepath.Join(root, name))
		if stErr != nil {
			continue
		}
		if bestDir == "" || st.ModTime().After(bestModTime) {
			bestDir = filepath.Join(root, name)
			bestModTime = st.ModTime()
		}
	}
	if bestDir == "" {
		return nil, false, nil
	}

	cacheKey := bestDir
	now := time.Now()
	previewMangaCacheMu.RLock()
	if previewMangaCache != nil {
		if cached, ok := previewMangaCache[cacheKey]; ok && now.Before(cached.ExpiresAt) {
			pages := make([]string, len(cached.Pages))
			copy(pages, cached.Pages)
			previewMangaCacheMu.RUnlock()
			return pages, true, nil
		}
	}
	previewMangaCacheMu.RUnlock()

	pages := make([]string, 0, 256)
	err = filepath.WalkDir(bestDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		if isImageExt(filepath.Ext(d.Name())) {
			pages = append(pages, path)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	sort.Strings(pages)

	previewMangaCacheMu.Lock()
	if previewMangaCache == nil {
		previewMangaCache = map[string]previewMangaPages{}
	}
	previewMangaCache[cacheKey] = previewMangaPages{
		Pages:     pages,
		ExpiresAt: now.Add(5 * time.Minute),
	}
	previewMangaCacheMu.Unlock()

	out := make([]string, len(pages))
	copy(out, pages)
	return out, true, nil
}

func isImageExt(ext string) bool {
	switch strings.ToLower(ext) {
	case ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".avif":
		return true
	default:
		return false
	}
}

func writeHTML(w http.ResponseWriter, html string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(html))
}

func previewHomeHTML() string {
	return `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>JM 本地预览</title>
<style>
/* Design System */
:root {
  --text: #1d1d1f;
  --text-secondary: #86868b;
  --text-tertiary: #6e6e73;
  --bg: #f5f5f7;
  --bg-secondary: #ffffff;
  --glass: rgba(255,255,255,0.72);
  --glass-border: rgba(255,255,255,0.85);
  --card-bg: rgba(255,255,255,0.65);
  --card-border: rgba(0,0,0,0.04);
  --line: rgba(0,0,0,0.08);
  --btn-bg: rgba(0,0,0,0.04);
  --accent: #0071e3;
  --accent-hover: #0077ed;
  --accent-light: rgba(0,113,227,0.08);
  --shadow: 0 4px 28px rgba(0,0,0,0.08);
  --shadow-hover: 0 12px 40px rgba(0,0,0,0.15);
  --radius: 20px;
  --radius-sm: 12px;
  --transition: 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}

/* Dark Mode */
@media (prefers-color-scheme: dark) {
  :root {
    --text: #f5f5f7;
    --text-secondary: #a1a1a6;
    --text-tertiary: #8e8e93;
    --bg: #000000;
    --bg-secondary: #1c1c1e;
    --glass: rgba(28,28,30,0.72);
    --glass-border: rgba(255,255,255,0.1);
    --card-bg: rgba(28,28,30,0.65);
    --card-border: rgba(255,255,255,0.06);
    --line: rgba(255,255,255,0.1);
    --btn-bg: rgba(255,255,255,0.08);
    --accent: #0a84ff;
    --accent-hover: #409cff;
    --accent-light: rgba(10,132,255,0.15);
    --shadow: 0 4px 28px rgba(0,0,0,0.4);
    --shadow-hover: 0 12px 40px rgba(0,0,0,0.5);
  }
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

body::before {
  content: '';
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(0,113,227,0.08), transparent);
  pointer-events: none;
  z-index: -1;
}

@media (prefers-color-scheme: dark) {
  body::before {
    background: radial-gradient(ellipse 80% 50% at 50% -20%, rgba(10,132,255,0.15), transparent);
  }
}

.wrap {
  max-width: 1100px;
  margin: 0 auto;
  padding: 24px 20px 40px;
}

/* Header */
.header {
  background: var(--glass);
  border: 1px solid var(--glass-border);
  border-radius: var(--radius);
  backdrop-filter: saturate(180%) blur(20px);
  -webkit-backdrop-filter: saturate(180%) blur(20px);
  box-shadow: var(--shadow);
  padding: 24px;
  margin-bottom: 20px;
  animation: fadeIn 0.4s ease;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(8px); }
  to { opacity: 1; transform: translateY(0); }
}

h1 {
  font-size: 26px;
  font-weight: 600;
  letter-spacing: -0.5px;
  margin-bottom: 4px;
}

.sub {
  color: var(--text-secondary);
  font-size: 14px;
  margin-bottom: 16px;
}

/* Search */
.search-wrap {
  position: relative;
}

.search-icon {
  position: absolute;
  left: 14px;
  top: 50%;
  transform: translateY(-50%);
  width: 18px;
  height: 18px;
  color: var(--text-tertiary);
  pointer-events: none;
}

input {
  width: 100%;
  height: 44px;
  padding: 0 14px 0 40px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: var(--bg-secondary);
  color: var(--text);
  font-size: 15px;
  outline: none;
  transition: var(--transition);
}

input::placeholder { color: var(--text-tertiary); }
input:focus {
  border-color: var(--accent);
  box-shadow: 0 0 0 3px var(--accent-light);
}

.spinner {
  position: absolute;
  right: 12px;
  top: 50%;
  transform: translateY(-50%);
  width: 18px;
  height: 18px;
  border: 2px solid var(--line);
  border-top-color: var(--accent);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
}

.spinner.active {
  opacity: 1;
  animation: spin 0.8s linear infinite;
}

@keyframes spin { to { transform: translateY(-50%) rotate(360deg); } }

/* Grid */
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 16px;
}

/* Card */
.card {
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: var(--radius-sm);
  overflow: hidden;
  cursor: pointer;
  transition: var(--transition);
  animation: fadeIn 0.4s ease backwards;
}

.card:nth-child(1) { animation-delay: 0.02s; }
.card:nth-child(2) { animation-delay: 0.04s; }
.card:nth-child(3) { animation-delay: 0.06s; }
.card:nth-child(4) { animation-delay: 0.08s; }
.card:nth-child(5) { animation-delay: 0.1s; }
.card:nth-child(6) { animation-delay: 0.12s; }
.card:nth-child(n+7) { animation-delay: 0.14s; }

.card:hover {
  transform: translateY(-4px);
  box-shadow: var(--shadow-hover);
  background: var(--bg-secondary);
}

/* Cover */
.cover {
  position: relative;
  width: 100%;
  aspect-ratio: 3/4;
  background: var(--btn-bg);
  overflow: hidden;
}

.cover img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.cover-placeholder {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-tertiary);
}

.cover-placeholder svg {
  width: 40px;
  height: 40px;
  opacity: 0.3;
}

.dl-btn {
  position: absolute;
  bottom: 8px;
  right: 8px;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background: rgba(0,0,0,0.5);
  backdrop-filter: blur(10px);
  display: flex;
  align-items: center;
  justify-content: center;
  color: #fff;
  opacity: 0;
  transform: scale(0.9);
  transition: var(--transition);
  text-decoration: none;
}

.card:hover .dl-btn {
  opacity: 1;
  transform: scale(1);
}

.dl-btn:hover {
  background: var(--accent);
}

.dl-btn svg { width: 14px; height: 14px; }

/* Info */
.info {
  padding: 12px;
}

.title {
  font-size: 13px;
  font-weight: 500;
  color: var(--text);
  line-height: 1.4;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  margin-bottom: 8px;
}

.title-id {
  color: var(--accent);
  font-weight: 600;
}

/* Tags */
.tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.tag {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  padding: 3px 8px;
  background: var(--accent-light);
  border-radius: 6px;
  color: var(--accent);
  font-size: 11px;
  font-weight: 500;
}

.tag svg { width: 10px; height: 10px; }

/* Empty */
.empty {
  grid-column: 1 / -1;
  text-align: center;
  padding: 60px 20px;
  color: var(--text-secondary);
}

.empty-icon {
  width: 56px;
  height: 56px;
  margin: 0 auto 16px;
  opacity: 0.25;
}

.empty-text { font-size: 16px; margin-bottom: 4px; }
.empty-hint { font-size: 13px; opacity: 0.7; }

/* Responsive */
@media (max-width: 680px) {
  .wrap { padding: 16px 12px 32px; }
  .header { padding: 18px; border-radius: 14px; }
  h1 { font-size: 22px; }
  .grid {
    grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
    gap: 12px;
  }
  .info { padding: 10px; }
  .title { font-size: 12px; }
}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <h1>JM 本地预览</h1>
    <p class="sub">输入 JM 号或关键词搜索</p>
    <div class="search-wrap">
      <input id="q" placeholder="搜索 JM 号或标题..." />
      <svg class="search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
        <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
      </svg>
      <div class="spinner" id="spinner"></div>
    </div>
  </div>
  <div id="grid" class="grid"></div>
</div>
<script>
const q = document.getElementById('q');
const grid = document.getElementById('grid');
const spinner = document.getElementById('spinner');
let timer = null;

const icons = {
  image: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="3" y="3" width="18" height="18" rx="2"/><circle cx="8.5" cy="8.5" r="1.5"/><path d="m21 15-5-5L5 21"/></svg>',
  download: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
  pages: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="9" y1="3" x2="9" y2="21"/></svg>',
  size: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
  empty: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/><line x1="8" y1="11" x2="14" y2="11"/></svg>'
};

function formatSize(bytes) {
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1024 / 1024).toFixed(1) + ' MB';
}

function renderEmpty(kw) {
  return '<div class="empty">' +
    '<div class="empty-icon">' + icons.empty + '</div>' +
    '<div class="empty-text">未找到漫画</div>' +
    (kw ? '<div class="empty-hint">尝试其他关键词</div>' : '') +
    '</div>';
}

function renderCard(it) {
  const coverUrl = '/api/comic/' + it.id + '/page/1';
  const size = formatSize(it.size);
  return '<div class="card" onclick="location.href=\'/' + it.id + '\'" data-id="' + it.id + '">' +
    '<div class="cover">' +
    '<div class="cover-placeholder">' + icons.image + '</div>' +
    '<img src="' + coverUrl + '" loading="lazy" onerror="this.style.display=\'none\'" onload="this.previousElementSibling.style.display=\'none\'" />' +
    '<a class="dl-btn" href="/api/comic/' + it.id + '/download" onclick="event.stopPropagation()" title="下载">' + icons.download + '</a>' +
    '</div>' +
    '<div class="info">' +
    '<div class="title"><span class="title-id">JM' + it.id + '</span> ' + it.title + '</div>' +
    '<div class="tags">' +
    '<span class="tag page-count-tag" data-id="' + it.id + '">' + icons.pages + ' ...</span>' +
    '<span class="tag">' + icons.size + ' ' + size + '</span>' +
    '</div>' +
    '</div>' +
    '</div>';
}

const pageObserver = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (!entry.isIntersecting) return;
    const el = entry.target;
    pageObserver.unobserve(el);
    const id = el.dataset.id;
    fetch('/api/comic/' + id).then(r => r.json()).then(meta => {
      if (meta.page_count > 0) {
        el.innerHTML = icons.pages + ' ' + meta.page_count + 'P';
      } else {
        el.style.display = 'none';
      }
    }).catch(() => { el.style.display = 'none'; });
  });
}, { rootMargin: '200px' });

function observePageCounts() {
  document.querySelectorAll('.page-count-tag').forEach(el => pageObserver.observe(el));
}

async function load() {
  clearTimeout(timer);
  spinner.classList.add('active');
  try {
    const r = await fetch('/api/search?q=' + encodeURIComponent(q.value.trim()));
    const data = await r.json();
    const items = data.items || [];
    if (items.length === 0) {
      grid.innerHTML = renderEmpty(q.value.trim());
    } else {
      grid.innerHTML = items.map(renderCard).join('');
      observePageCounts();
    }
  } catch (e) {
    grid.innerHTML = renderEmpty('');
  } finally {
    timer = setTimeout(() => spinner.classList.remove('active'), 200);
  }
}

q.addEventListener('input', load);
load();
</script>
</body>
</html>`
}

func previewViewerHTML(id string) string {
	page := map[string]string{"id": id}
	b, _ := json.Marshal(page)
	return `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>JM` + id + ` 预览</title>
<style>
/* Design System */
:root {
  --text: #1d1d1f;
  --text-secondary: #86868b;
  --text-tertiary: #6e6e73;
  --bg: #f5f5f7;
  --bg-secondary: #ffffff;
  --glass: rgba(255,255,255,0.72);
  --glass-border: rgba(255,255,255,0.85);
  --btn-bg: rgba(0,0,0,0.04);
  --btn-border: rgba(0,0,0,0.06);
  --btn-hover: rgba(0,0,0,0.08);
  --accent: #0071e3;
  --accent-hover: #0077ed;
  --accent-light: rgba(0,113,227,0.1);
  --shadow: 0 4px 28px rgba(0,0,0,0.08);
  --shadow-lg: 0 12px 40px rgba(0,0,0,0.12);
  --radius: 18px;
  --radius-sm: 10px;
  --transition: 0.25s cubic-bezier(0.4, 0, 0.2, 1);
}

/* Dark Mode */
@media (prefers-color-scheme: dark) {
  :root {
    --text: #f5f5f7;
    --text-secondary: #a1a1a6;
    --text-tertiary: #8e8e93;
    --bg: #000000;
    --bg-secondary: #1c1c1e;
    --glass: rgba(28,28,30,0.72);
    --glass-border: rgba(255,255,255,0.1);
    --btn-bg: rgba(255,255,255,0.08);
    --btn-border: rgba(255,255,255,0.1);
    --btn-hover: rgba(255,255,255,0.12);
    --accent: #0a84ff;
    --accent-hover: #409cff;
    --accent-light: rgba(10,132,255,0.15);
    --shadow: 0 4px 28px rgba(0,0,0,0.4);
    --shadow-lg: 0 12px 40px rgba(0,0,0,0.5);
  }
}

* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }

body {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "SF Pro Display", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--text);
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

/* Background */
body::before {
  content: '';
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: radial-gradient(ellipse 100% 60% at 50% -10%, rgba(0,113,227,0.06), transparent);
  pointer-events: none;
  z-index: -1;
}

@media (prefers-color-scheme: dark) {
  body::before {
    background: radial-gradient(ellipse 100% 60% at 50% -10%, rgba(10,132,255,0.12), transparent);
  }
}

.shell {
  min-height: 100%;
  padding: 16px;
}

/* Toolbar */
.bar {
  position: sticky;
  top: 12px;
  max-width: 1100px;
  margin: 0 auto 16px;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px;
  background: var(--glass);
  border: 1px solid var(--glass-border);
  border-radius: var(--radius);
  backdrop-filter: saturate(180%) blur(20px);
  -webkit-backdrop-filter: saturate(180%) blur(20px);
  box-shadow: var(--shadow);
  z-index: 100;
  animation: slideDown 0.35s ease;
}

@keyframes slideDown {
  from { opacity: 0; transform: translateY(-12px); }
  to { opacity: 1; transform: translateY(0); }
}

/* Buttons */
.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 5px;
  padding: 8px 12px;
  background: var(--btn-bg);
  border: 1px solid var(--btn-border);
  border-radius: 999px;
  color: var(--text);
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: var(--transition);
  text-decoration: none;
  white-space: nowrap;
}

.btn svg {
  width: 15px;
  height: 15px;
  flex-shrink: 0;
}

.btn:hover { background: var(--btn-hover); }
.btn:active { transform: scale(0.97); }
.btn.primary {
  background: var(--accent);
  border-color: var(--accent);
  color: #fff;
}
.btn.primary:hover { background: var(--accent-hover); }

/* Title */
.title {
  flex: 1;
  min-width: 80px;
  padding: 0 8px;
  font-size: 14px;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

/* Badge */
.badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 6px 10px;
  background: var(--accent-light);
  border-radius: 999px;
  color: var(--accent);
  font-size: 12px;
  font-weight: 500;
  white-space: nowrap;
}

.badge svg { width: 12px; height: 12px; }

/* Page indicator dots */
.page-dots {
  display: flex;
  gap: 3px;
  padding: 0 6px;
}

.dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--btn-border);
  transition: var(--transition);
}

.dot.active { background: var(--accent); transform: scale(1.2); }

/* Viewer */
.viewer {
  max-width: 1100px;
  margin: 0 auto;
  min-height: calc(100vh - 120px);
  background: var(--glass);
  border: 1px solid var(--glass-border);
  border-radius: 20px;
  backdrop-filter: blur(4px);
  -webkit-backdrop-filter: blur(4px);
  box-shadow: var(--shadow-lg);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
  animation: fadeIn 0.4s ease;
}

@keyframes fadeIn {
  from { opacity: 0; }
  to { opacity: 1; }
}

/* Image */
.img-wrap {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: center;
}

img {
  max-width: 100%;
  max-height: calc(100vh - 180px);
  object-fit: contain;
  border-radius: 12px;
  box-shadow: var(--shadow-lg);
}


/* Error state */
.error {
  text-align: center;
  padding: 48px;
  color: var(--text-secondary);
}

.error-icon {
  width: 48px;
  height: 48px;
  margin: 0 auto 12px;
  opacity: 0.4;
}

/* Responsive */
@media (max-width: 720px) {
  .shell { padding: 10px; }
  .bar {
    top: 8px;
    border-radius: 14px;
    padding: 5px;
    gap: 4px;
    flex-wrap: wrap;
  }
  .title {
    order: 10;
    flex-basis: 100%;
    text-align: center;
    padding: 6px 0 0;
    font-size: 13px;
  }
  .btn { padding: 7px 10px; font-size: 12px; }
  .btn svg { width: 14px; height: 14px; }
  .viewer { border-radius: 14px; padding: 12px; min-height: calc(100vh - 140px); }
  img { max-height: calc(100vh - 200px); }
}
</style>
</head>
<body>
<div class="shell">
<div class="bar">
  <button class="btn" id="back" title="返回首页">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <path d="m15 18-6-6 6-6"/>
    </svg>
  </button>
  <button class="btn" id="prev" title="上一页 (←/A)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="15 18 9 12 15 6"/>
    </svg>
  </button>
  <button class="btn primary" id="next" title="下一页 (→/D)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="9 18 15 12 9 6"/>
    </svg>
  </button>
  <div class="page-dots" id="dots"></div>
  <div class="title" id="title">加载中...</div>
  <div class="badge" id="badge">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/></svg>
    <span id="badge-text">- / -</span>
  </div>
  <button class="btn" id="fullscreen" title="全屏 (F)">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/>
      <line x1="21" y1="3" x2="14" y2="10"/><line x1="3" y1="21" x2="10" y2="14"/>
    </svg>
  </button>
  <a class="btn" id="download" href="#" title="下载">
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
      <polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>
    </svg>
  </a>
</div>
<div class="viewer">
  <div class="img-wrap">
    <img id="img" alt="page"/>
  </div>
</div>
</div>
<script>
const state = ` + string(b) + `;
let page = 1;
let total = 1;
const img = document.getElementById('img');
const titleEl = document.getElementById('title');
const badgeText = document.getElementById('badge-text');
const dots = document.getElementById('dots');
const dlBtn = document.getElementById('download');

function renderDots() {
  const maxDots = Math.min(total, 9);
  const half = Math.floor(maxDots / 2);
  let start = Math.max(1, page - half);
  let end = start + maxDots - 1;
  if (end > total) { end = total; start = Math.max(1, end - maxDots + 1); }
  let html = '';
  for (let i = start; i <= end; i++) {
    html += '<div class="dot' + (i === page ? ' active' : '') + '"></div>';
  }
  dots.innerHTML = html;
}

async function init() {
  try {
    const r = await fetch('/api/comic/' + state.id);
    if (!r.ok) {
      showError('未找到本地漫画：JM' + state.id);
      return;
    }
    const meta = await r.json();
    total = Math.max(1, meta.page_count || 1);
    if (meta.download) {
      dlBtn.href = meta.download;
      dlBtn.style.display = '';
    } else {
      dlBtn.style.display = 'none';
    }
    titleEl.textContent = 'JM' + meta.id + (meta.title ? ' - ' + meta.title : '');
    render();
  } catch (e) {
    showError('加载失败');
  }
}

function showError(msg) {
  skeleton.style.display = 'none';
  document.querySelector('.viewer').innerHTML = '<div class="error">' +
    '<div class="error-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">' +
    '<circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>' +
    '</svg></div><div>' + msg + '</div></div>';
}

function render() {
  if (page < 1) page = 1;
  if (page > total) page = total;
  img.src = '/api/comic/' + state.id + '/page/' + page;
  badgeText.textContent = page + ' / ' + total;
  renderDots();
  preloadPages();
}

const preloadCache = {};
function preloadPages() {
  const toPreload = [];
  for (let i = 1; i <= 3; i++) {
    if (page + i <= total) toPreload.push(page + i);
    if (page - i >= 1) toPreload.push(page - i);
  }
  toPreload.forEach(p => {
    if (preloadCache[p]) return;
    preloadCache[p] = true;
    const im = new Image();
    im.src = '/api/comic/' + state.id + '/page/' + p;
  });
}

document.getElementById('prev').onclick = () => { if (page > 1) { page--; render(); } };
document.getElementById('next').onclick = () => { if (page < total) { page++; render(); } };
document.getElementById('back').onclick = () => { location.href = '/'; };
document.getElementById('fullscreen').onclick = async () => {
  if (!document.fullscreenElement) await document.documentElement.requestFullscreen();
  else await document.exitFullscreen();
};

window.addEventListener('keydown', (e) => {
  if (e.key === 'ArrowLeft' || e.key.toLowerCase() === 'a') { if (page > 1) { page--; render(); } }
  if (e.key === 'ArrowRight' || e.key.toLowerCase() === 'd') { if (page < total) { page++; render(); } }
  if (e.key.toLowerCase() === 'f') document.getElementById('fullscreen').click();
});

img.addEventListener('click', () => { if (page < total) { page++; render(); } });
init();
</script>
</body>
</html>`
}
