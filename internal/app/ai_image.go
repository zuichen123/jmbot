package app

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"napcat-jm-go/internal/aiimage"
)

const defaultWaitingImage = "base64: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"

func (a *App) handleAIImageCommand(rawMessage string, data map[string]any, messageType string, groupID, userID int64) bool {
	if m := mustMatch(`^(?:/)?image\s+(on|off|help)$`, rawMessage); m != nil {
		switch m[1] {
		case "on":
			if !a.requireAdmin(messageType, groupID, userID, "仅管理员可开启 AI 画图") {
				return true
			}
			a.cfgMu.Lock()
			a.cfg.AIImageEnabled = true
			a.cfgMu.Unlock()
			a.saveConfig()
			a.sendMessage(messageType, groupID, userID, "AI 画图功能已开启，使用 image2 <提示词> 生成图片")
			return true
		case "off":
			if !a.requireAdmin(messageType, groupID, userID, "仅管理员可关闭 AI 画图") {
				return true
			}
			a.cfgMu.Lock()
			a.cfg.AIImageEnabled = false
			a.cfgMu.Unlock()
			a.saveConfig()
			a.sendMessage(messageType, groupID, userID, "AI 画图功能已关闭")
			return true
		case "help":
			a.sendMessage(messageType, groupID, userID, "AI 画图使用说明：\n1) image on - 开启画图功能\n2) image off - 关闭画图功能\n3) image2 <提示词> - 生成图片\n4) 引用图片后 image2 <提示词> - 图生图")
			return true
		}
	}

	m := mustMatch(`(?:^|])(?:\s*)image2\s+(.+)$`, rawMessage)
	if m == nil {
		return false
	}
	prompt := strings.TrimSpace(m[1])
	if prompt == "" {
		return false
	}

	cfg := a.currentConfig()
	if !cfg.AIImageEnabled {
		a.sendMessage(messageType, groupID, userID, "AI 画图功能未开启，请发送 image on 开启")
		return true
	}
	if cfg.AIImageAPIKey == "" {
		a.sendMessage(messageType, groupID, userID, "AI 画图未配置 API Key（请联系管理员设置 ai_image_api_key）")
		return true
	}

	aiCfg := aiimage.Config{
		BaseURL:    cfg.AIImageBaseURL,
		APIKey:     cfg.AIImageAPIKey,
		Model:      cfg.AIImageModel,
		Size:       cfg.AIImageSize,
		Timeout:    time.Duration(cfg.AIImageTimeout) * time.Second,
		MaxRetries: cfg.AIImageMaxRetries,
	}

	// 提取图片
	useImageToImage := false
	imageBytes, extractErr := a.extractAIImageBytes(data)
	if extractErr != nil {
		log.Printf("[AI Image] 提取图片失败: %v", extractErr)
	}
	if len(imageBytes) > 0 {
		useImageToImage = true
		log.Printf("[AI Image] 图生图模式，图片大小: %d bytes", len(imageBytes))
	} else {
		log.Printf("[AI Image] 文生图模式，提示词: %q", prompt)
	}

	// 发送等待提示
	a.sendMessage(messageType, groupID, userID, "正在生成图片...")

	var result *aiimage.Result
	var err error
	startTime := time.Now()

	if useImageToImage {
		log.Printf("[AI Image] 开始图生图 (模型: %s, 超时: %v)", aiCfg.Model, aiCfg.Timeout)
		result, err = aiimage.EditImage(aiCfg, prompt, imageBytes)
		if err != nil && strings.Contains(err.Error(), "不支持图生图") {
			log.Printf("[AI Image] 图生图不支持，回退到文生图")
			a.sendMessage(messageType, groupID, userID, "当前模型不支持图生图，正在尝试文生图...")
			result, err = aiimage.GenerateImage(aiCfg, prompt)
		}
	} else {
		log.Printf("[AI Image] 开始文生图 (模型: %s, 超时: %v)", aiCfg.Model, aiCfg.Timeout)
		result, err = aiimage.GenerateImage(aiCfg, prompt)
	}
	elapsed := time.Since(startTime)

	if err != nil {
		log.Printf("[AI Image] 生图失败 [%s] 提示词: %q 耗时: %v 错误: %v",
			map[bool]string{true: "图生图", false: "文生图"}[useImageToImage], prompt, elapsed, err)
		// 通知用户
		msg := fmt.Sprintf("图片生成失败: %s", err.Error())
		if messageType == "group" && groupID > 0 {
			a.bot.SendGroupMsgWithAtText(groupID, userID, msg)
		} else {
			a.bot.SendPrivateMessage(userID, msg)
		}
		// 通知管理员
		adminID := cfg.AdminID
		if adminID > 0 {
			mode := map[bool]string{true: "图生图", false: "文生图"}[useImageToImage]
			adminMsg := fmt.Sprintf("【AI生图失败通知】\n模式: %s\n提示词: %s\n模型: %s\n耗时: %v\n错误: %s", mode, prompt, aiCfg.Model, elapsed, err.Error())
			a.bot.SendPrivateMessage(adminID, adminMsg)
		}
		return true
	}

	// 处理返回结果
	var imageRef string
	if result.B64JSON != "" {
		log.Printf("[AI Image] 生图成功 (b64json) [%s] 提示词: %q 耗时: %v 数据大小: %d bytes",
			map[bool]string{true: "图生图", false: "文生图"}[useImageToImage], prompt, elapsed, len(result.B64JSON))
		imageRef = "base64://" + strings.TrimPrefix(result.B64JSON, "data:image/")
		if idx := strings.Index(imageRef, ","); idx > 0 {
			imageRef = "base64://" + imageRef[idx+1:]
		}
	} else if strings.HasPrefix(result.ImageURL, "data:") {
		log.Printf("[AI Image] 生图成功 (data URI) [%s] 提示词: %q 耗时: %v",
			map[bool]string{true: "图生图", false: "文生图"}[useImageToImage], prompt, elapsed)
		if idx := strings.Index(result.ImageURL, ","); idx > 0 {
			imageRef = "base64://" + result.ImageURL[idx+1:]
		}
	} else if result.ImageURL != "" {
		log.Printf("[AI Image] 生图成功 (URL) [%s] 提示词: %q 耗时: %v URL: %s",
			map[bool]string{true: "图生图", false: "文生图"}[useImageToImage], prompt, elapsed, result.ImageURL)
		imageRef = result.ImageURL
	} else {
		log.Printf("[AI Image] 生图返回为空 [%s] 提示词: %q 耗时: %v", map[bool]string{true: "图生图", false: "文生图"}[useImageToImage], prompt, elapsed)
		msg := "AI 画图返回为空"
		if messageType == "group" && groupID > 0 {
			a.bot.SendGroupMsgWithAtText(groupID, userID, msg)
		} else {
			a.bot.SendPrivateMessage(userID, msg)
		}
		adminID := cfg.AdminID
		if adminID > 0 {
			mode := map[bool]string{true: "图生图", false: "文生图"}[useImageToImage]
			a.bot.SendPrivateMessage(adminID, fmt.Sprintf("【AI生图异常】\n模式: %s\n提示词: %s\n原因: 返回数据为空", mode, prompt))
		}
		return true
	}

	// 发送生成的图片
	if messageType == "group" && groupID > 0 {
		a.bot.SendGroupMsgWithAtAndImage(groupID, userID, imageRef)
	} else {
		a.bot.SendPrivateMsgWithImage(userID, imageRef)
	}
	return true
}

func (a *App) extractAIImageBytes(data map[string]any) ([]byte, error) {
	// 尝试从回复消息中提取图片
	replyID := extractReplyMessageID(data)
	if replyID > 0 {
		msgData, err := a.bot.GetMsg(replyID)
		if err == nil && msgData != nil {
			sources := extractSoutuImageSourcesFromEvent(msgData)
			for _, src := range sources {
				if src.ImageURL != "" {
					return downloadImageBytes(src.ImageURL)
				}
				if len(src.ImageBytes) > 0 {
					return src.ImageBytes, nil
				}
			}
			refs := extractSoutuImageFileRefsFromEvent(msgData)
			for _, ref := range refs {
				u, getErr := a.bot.GetImageURL(ref)
				if getErr == nil && u != "" {
					return downloadImageBytes(u)
				}
			}
			// 尝试从被回复消息的 image 字段直接提取
			if img, ok := extractImageFromMessage(msgData); ok {
				return downloadImageBytes(img)
			}
		}
	}

	// 尝试从当前消息中提取图片（image2 与图片在同一消息的情况）
	sources := extractSoutuImageSourcesFromEvent(data)
	for _, src := range sources {
		if src.ImageURL != "" {
			return downloadImageBytes(src.ImageURL)
		}
		if len(src.ImageBytes) > 0 {
			return src.ImageBytes, nil
		}
	}

	refs := extractSoutuImageFileRefsFromEvent(data)
	for _, ref := range refs {
		u, err := a.bot.GetImageURL(ref)
		if err == nil && u != "" {
			return downloadImageBytes(u)
		}
	}

	return nil, nil
}

func extractReplyMessageID(data map[string]any) int64 {
	msg, ok := data["message"].([]any)
	if !ok {
		return 0
	}
	for _, seg := range msg {
		m, ok := seg.(map[string]any)
		if !ok || toString(m["type"]) != "reply" {
			continue
		}
		dm := mapGet(m, "data")
		return toInt64(dm["id"])
	}
	return 0
}

func downloadImageBytes(imageURL string) ([]byte, error) {
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
		return nil, fmt.Errorf("download image status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// extractImageFromMessage 从 NapCat 消息的 records 或 message 字段提取图片 URL
func extractImageFromMessage(data map[string]any) (string, bool) {
	// 从 records 数组中提取（被回复消息的图片在这里）
	if records, ok := data["records"].([]any); ok {
		for _, rec := range records {
			if rm, ok := rec.(map[string]any); ok {
				if url := findImageInElements(rm); url != "" {
					return url, true
				}
			}
		}
	}
	// 从 elements 中提取
	if url := findImageInElements(data); url != "" {
		return url, true
	}
	return "", false
}

func findImageInElements(data map[string]any) string {
	elems, ok := data["elements"].([]any)
	if !ok {
		return ""
	}
	for _, e := range elems {
		m, ok := e.(map[string]any)
		if !ok || toString(m["type"]) != "image" {
			continue
		}
		d := mapGet(m, "data")
		if u := toString(d["url"]); u != "" {
			return u
		}
	}
	return ""
}
