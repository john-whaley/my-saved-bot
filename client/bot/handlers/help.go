package handlers

import (
	"strings"

	"github.com/celestix/gotgproto/dispatcher"
	"github.com/celestix/gotgproto/ext"
	"github.com/krau/SaveAny-Bot/config"
)

const defaultStartMessage = "欢迎使用保存视频机器人"

func handleHelpCmd(ctx *ext.Context, update *ext.Update) error {
	text := strings.TrimSpace(config.C().StartMessage)
	if text == "" {
		text = defaultStartMessage
	}
	ctx.Reply(update, ext.ReplyTextString(text), nil)
	return dispatcher.EndGroups
}
