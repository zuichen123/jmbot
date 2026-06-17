package app

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	jmIDInNameRe  = regexp.MustCompile(`(?i)jm[\s_-]*([0-9]{3,})`)
	bikaIDInNameRe = regexp.MustCompile(`(?i)^bika_([a-f0-9]{24,})`)
	bikaIDRawRe    = regexp.MustCompile(`(?i)\b([a-f0-9]{24,})\b`)
	plainIDNameRe  = regexp.MustCompile(`(?:^|[^0-9])([0-9]{5,})(?:[^0-9]|$)`)
)

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
	// 匹配无前缀的bika ID（24位以上十六进制字符串）
	if m := bikaIDRawRe.FindStringSubmatch(name); len(m) > 1 {
		return "bika_" + strings.ToLower(m[1])
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

func normalizeComicID(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	// 支持 bika_ 和 title_ 前缀
	if strings.HasPrefix(s, "bika_") || strings.HasPrefix(s, "title_") {
		return s
	}
	return normalizeJMID(s)
}

func parseJMPathID(pathVal string) (string, bool) {
	p := strings.Split(strings.TrimSpace(pathVal), "/")[0]
	id := normalizeComicID(p)
	return id, id != ""
}

func deriveTitleFromName(name, id string) string {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	// 清理无效UTF-8字符
	if !utf8.ValidString(base) {
		base = strings.ToValidUTF8(base, "")
	}
	// 如果id包含title_前缀，直接返回清理后的文件名
	if strings.HasPrefix(id, "title_") {
		return base
	}
	// 安全处理：如果id包含无效UTF-8，直接返回清理后的文件名
	if !utf8.ValidString(id) {
		return base
	}
	re := regexp.MustCompile(`(?i)^jm[\s_-]*` + regexp.QuoteMeta(id) + `[\s_-]*`)
	base = re.ReplaceAllString(base, "")
	base = strings.TrimSpace(base)
	if base == "" {
		base = "JM" + id
	}
	return base
}

type previewBook struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	ModTime   time.Time `json:"mod_time"`
	PageCount int       `json:"page_count"`
	Path      string    `json:"-"`
}

var (
	previewBooksCacheMu sync.RWMutex
	previewBooksCache   []previewBook
	previewBooksCacheAt time.Time
)

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
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(d.Name()), ".cbz") {
			return nil
		}
		id := extractIDFromName(d.Name())
		// 如果没有提取到ID，用文件名（去掉扩展名）作为ID
		if id == "" {
			base := strings.TrimSuffix(d.Name(), filepath.Ext(d.Name()))
			if base == "" {
				return nil
			}
			// 清理无效UTF-8字符后作为ID
			if !utf8.ValidString(base) {
				base = strings.ToValidUTF8(base, "")
			}
			if base == "" {
				return nil
			}
			id = "title_" + base
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
	err = filepath.WalkDir(bestDir, func(path string, d os.DirEntry, err error) error {
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

type previewMangaPages struct {
	Pages     []string
	ExpiresAt time.Time
}

var (
	previewMangaCacheMu sync.RWMutex
	previewMangaCache   map[string]previewMangaPages
)
