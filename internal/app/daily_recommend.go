package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// startDailyRecommend 启动每日推荐定时任务
func (a *App) startDailyRecommend() {
	cfg := a.currentConfig()
	if !cfg.DailyRecommendEnabled {
		log.Printf("[Daily] 每日推荐未启用")
		return
	}

	log.Printf("[Daily] 每日推荐已启用，发送时间: %02d:%02d, 群数: %d", 
		cfg.DailyRecommendHour, cfg.DailyRecommendMinute, len(cfg.DailyRecommendGroups))

	go func() {
		for {
			now := time.Now()
			next := time.Date(now.Year(), now.Month(), now.Day(), 
				cfg.DailyRecommendHour, cfg.DailyRecommendMinute, 0, 0, now.Location())
			if now.After(next) {
				next = next.Add(24 * time.Hour)
			}
			duration := next.Sub(now)
			log.Printf("[Daily] 下次发送时间: %v (等待 %v)", next.Format("2006-01-02 15:04:05"), duration)
			time.Sleep(duration)
			a.sendDailyRecommend()
		}
	}()
}

// sendDailyRecommend 发送每日推荐
func (a *App) sendDailyRecommend() {
	cfg := a.currentConfig()
	if len(cfg.DailyRecommendGroups) == 0 {
		return
	}

	log.Printf("[Daily] 开始获取每日推荐")

	// 优先获取哔咔日榜
	var albums []DailyAlbum
	bikaAlbums := a.getBikaDailyAlbums()
	log.Printf("[Daily] 哔咔日榜获取: %d 本", len(bikaAlbums))

	// 哔咔不可用或数量不足时，补充JM周榜
	if len(bikaAlbums) < 15 {
		jmAlbums := a.getJMDailyAlbums()
		log.Printf("[Daily] JM周榜获取: %d 本", len(jmAlbums))
		albums = append(bikaAlbums, jmAlbums...)
	} else {
		albums = bikaAlbums
	}

	if len(albums) == 0 {
		log.Printf("[Daily] 没有可用的本子")
		return
	}

	// 限制数量为5个，避免节点过多导致发送超时
	if len(albums) > 5 {
		albums = albums[:5]
	}

	// 发送到开启的群
	for _, groupID := range cfg.DailyRecommendGroups {
		a.sendDailyAlbumList(groupID, albums, cfg)
	}
}

type DailyAlbum struct {
	ID       string
	Title    string
	Author   string
	Tags     string
	Episodes int
	Source   string // "Bika" 或 "JM"
	CoverURL string // 哔咔封面URL
}

func (a *App) sendDailyAlbumList(groupID int64, albums []DailyAlbum, cfg Config) {
	senderID := cfg.CardUserID
	nickname := cfg.CardNickname
	if senderID <= 0 {
		senderID = 10000
	}
	if nickname == "" {
		nickname = "每日推荐"
	}

	nodes := make([]map[string]any, 0, len(albums)*2+1)

	// 标题节点
	titleMsg := "【每日本子推荐】\n回复 序号 下载（可批量：1 2 3）"
	nodes = append(nodes, map[string]any{
		"type": "node",
		"data": map[string]any{
			"user_id":  senderID,
			"nickname": nickname,
			"content": []map[string]any{
				{"type": "text", "data": map[string]any{"text": titleMsg}},
			},
		},
	})

	// 每个本子：介绍 + 封面图
	for i, album := range albums {
		if i >= 5 {
			break
		}

		tags := album.Tags
		if len(tags) > 40 {
			tags = tags[:40] + "..."
		}

		infoMsg := fmt.Sprintf("%d. [%s] %s\n作者：%s\n标签：%s\n章节数：%d", 
			i+1, album.Source, album.Title, album.Author, tags, album.Episodes)

		// 介绍节点
		nodes = append(nodes, map[string]any{
			"type": "node",
			"data": map[string]any{
				"user_id":  senderID,
				"nickname": fmt.Sprintf("%d. %s", i+1, album.Title),
				"content": []map[string]any{
					{"type": "text", "data": map[string]any{"text": infoMsg}},
				},
			},
		})

		// 封面PDF
		coverPath := ""
		if album.Source == "Bika" && album.CoverURL != "" {
			coverPath = a.downloadBikaCover(album.ID)
		} else if album.Source == "JM" {
			if mangaPath, ok, _ := a.findMangaPageByID(album.ID, 1); ok && fileExists(mangaPath) {
				coverPath = mangaPath
			}
		}

		if coverPath != "" && fileExists(coverPath) {
			// 把封面图片转换为PDF
			pdfPath := filepath.Join(a.tmpDir(), fmt.Sprintf("daily_cover_%s_%d.pdf", album.ID, time.Now().UnixNano()))
			if err := imageToPDF(coverPath, pdfPath); err == nil {
				if pf, err := a.bot.prepareForwardFile(cfg, pdfPath); err == nil && len(pf.candidates) > 0 {
					nodes = append(nodes, map[string]any{
						"type": "node",
						"data": map[string]any{
							"user_id":  senderID,
							"nickname": fmt.Sprintf("%d. %s", i+1, album.Title),
							"content": []map[string]any{
								{"type": "file", "data": map[string]any{"file": pf.candidates[0]}},
							},
						},
					})
				}
			}
		}
	}

	// 发送 - 每个群单独构建nodes避免共享引用问题，失败重试3次
	for _, gid := range []int64{groupID} {
		nodesCopy := make([]map[string]any, len(nodes))
		copy(nodesCopy, nodes)
		
		params := map[string]any{
			"group_id": gid,
			"message":  nodesCopy,
		}
		
		sent := false
		for retry := 0; retry < 3; retry++ {
			_, err := a.bot.send("send_group_forward_msg", params, echo("daily_recommend", gid), 300*time.Second)
			if err == nil {
				sent = true
				break
			}
			log.Printf("[Daily] 发送转发消息到群%d失败 (重试%d/3): %v", gid, retry+1, err)
			time.Sleep(3 * time.Second)
		}
		if !sent {
			log.Printf("[Daily] 发送转发消息到群%d最终失败", gid)
		}
	}

	// 延迟清理封面临时文件（等待发送完成）
	go func() {
		time.Sleep(5 * time.Minute)
		for _, album := range albums {
			if album.Source == "Bika" {
				for _, ext := range []string{".jpg", ".jpeg", ".png", ".webp"} {
					coverPath := filepath.Join(a.tmpDir(), fmt.Sprintf("bika_cover_%s%s", album.ID, ext))
					_ = os.Remove(coverPath)
				}
			}
		}
	}()

	// 缓存供回复下载
	dailyCacheKey := fmt.Sprintf("daily:%d", groupID)
	var searchResults []SearchResultItem
	for _, album := range albums {
		searchResults = append(searchResults, SearchResultItem{
			Source: album.Source,
			ID:     album.ID,
			Title:  album.Title,
			Tags:   strings.Split(album.Tags, ", "),
		})
	}
	a.searchMu.Lock()
	a.search[dailyCacheKey] = PendingSearch{
		At:         time.Now(),
		AggResults: searchResults,
	}
	a.searchMu.Unlock()
}

// downloadBikaCover 下载哔咔封面到临时文件
func (a *App) downloadBikaCover(comicID string) string {
	token := a.getBikaUserToken(0)
	if token == "" {
		return ""
	}

	pages, _, err := a.bika.GetChapterImages(comicID, 1, 1, token)
	if err != nil || len(pages) == 0 {
		return ""
	}

	first := pages[0]
	imageURL := buildBikaImageURL(first.Media.FileServer, first.Media.Path)
	tmpPath := filepath.Join(a.tmpDir(), fmt.Sprintf("bika_cover_%s%s", comicID, filepath.Ext(first.Media.OriginalName)))
	if err := downloadBikaFile(imageURL, tmpPath, token, "original"); err != nil {
		return ""
	}
	return tmpPath
}

// getBikaDailyAlbums 获取哔咔日榜
func (a *App) getBikaDailyAlbums() []DailyAlbum {
	if a.bika == nil {
		return nil
	}

	token := a.getBikaUserToken(0)
	if token == "" {
		return nil
	}

	endpoint := "comics/leaderboard?tt=H24&ct=VC" // tt=H24 日榜
	respBody, err := a.bika.makeRequest("GET", endpoint, nil, token)
	if err != nil {
		log.Printf("[Daily] 哔咔日榜获取失败: %v", err)
		return nil
	}

	var resp struct {
		Code int `json:"code"`
		Data struct {
			Comics []struct {
				ID          string   `json:"_id"`
				Title       string   `json:"title"`
				Author      string   `json:"author"`
				Tags        []string `json:"tags"`
				Categories  []string `json:"categories"`
				EPSCount    int      `json:"epsCount"`
				Description string   `json:"description"`
			} `json:"comics"`
		} `json:"data"`
	}

	if err := json.Unmarshal(respBody, &resp); err != nil {
		log.Printf("[Daily] 哔咔日榜解析失败: %v", err)
		return nil
	}

	var albums []DailyAlbum
	for _, comic := range resp.Data.Comics {
		tags := strings.Join(append(comic.Tags, comic.Categories...), ", ")
		albums = append(albums, DailyAlbum{
			ID:       comic.ID,
			Title:    comic.Title,
			Author:   comic.Author,
			Tags:     tags,
			Episodes: comic.EPSCount,
			Source:   "Bika",
			CoverURL: "yes",
		})
	}

	return albums
}

// getJMDailyAlbums 获取JM周榜
func (a *App) getJMDailyAlbums() []DailyAlbum {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var albums []DailyAlbum
	keywords := []string{"本周热门", "热门推荐"}

	for _, keyword := range keywords {
		data, err := a.jm.reqAPI(ctx, "/search", map[string]string{
			"main_tag":     "0",
			"search_query": keyword,
			"page":         "1",
			"o":            "mv",
			"t":            "a",
		})
		if err != nil {
			continue
		}

		content, ok := data["content"].([]any)
		if !ok {
			continue
		}

		for _, item := range content {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			id := toJMID(anyToString(row["id"]))
			title := anyToString(row["name"])
			if id == "" || title == "" {
				continue
			}

			tags := ""
			if t := row["tags"]; t != nil {
				if tagList, ok := t.([]any); ok {
					var tagStrs []string
					for _, tag := range tagList {
						if s, ok := tag.(string); ok {
							tagStrs = append(tagStrs, s)
						}
					}
					tags = strings.Join(tagStrs, ", ")
				}
			}

			albums = append(albums, DailyAlbum{
				ID:    id,
				Title: title,
				Tags:  tags,
				Source: "JM",
			})
		}
		if len(albums) >= 15 {
			break
		}
	}

	if len(albums) > 15 {
		albums = albums[:15]
	}

	return albums
}
