package captionfilter

import (
	"regexp"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/krau/SaveAny-Bot/config"
)

var (
	mentionRegexp    = regexp.MustCompile(`@[A-Za-z0-9_]{1,32}`)
	urlRegexp        = regexp.MustCompile(`https?://\S+|t\.me/\S+`)
	whitespaceRegexp = regexp.MustCompile(`[ \t\r\f\v]+`)
	blankLinesRegexp = regexp.MustCompile(`\n{3,}`)
)

// Apply removes or drops source captions according to config.toml.
func Apply(caption string) string {
	cfg := config.C().CaptionFilter
	if !cfg.Enable || caption == "" {
		return caption
	}

	text := caption
	for _, word := range cfg.DropIfContains {
		word = strings.TrimSpace(word)
		if word != "" && strings.Contains(text, word) {
			return ""
		}
	}
	for _, pattern := range cfg.DropIfRegex {
		if matches(pattern, text) {
			return ""
		}
	}

	if cfg.RemoveMentions {
		text = mentionRegexp.ReplaceAllString(text, "")
	}
	if cfg.RemoveURLs {
		text = urlRegexp.ReplaceAllString(text, "")
	}
	for _, word := range cfg.RemoveWords {
		word = strings.TrimSpace(word)
		if word != "" {
			text = strings.ReplaceAll(text, word, "")
		}
	}
	for _, pattern := range cfg.RemoveRegex {
		text = replaceRegexp(pattern, text)
	}

	text = cleanSpacing(text)
	if cfg.MaxLength > 0 && len([]rune(text)) > cfg.MaxLength {
		text = string([]rune(text)[:cfg.MaxLength])
		text = strings.TrimSpace(text)
	}
	return text
}

func matches(pattern, text string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Warnf("Invalid caption filter regex %q: %v", pattern, err)
		return false
	}
	return re.MatchString(text)
}

func replaceRegexp(pattern, text string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return text
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Warnf("Invalid caption filter regex %q: %v", pattern, err)
		return text
	}
	return re.ReplaceAllString(text, "")
}

func cleanSpacing(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(whitespaceRegexp.ReplaceAllString(line, " "))
	}
	text = strings.Join(lines, "\n")
	text = blankLinesRegexp.ReplaceAllString(text, "\n\n")
	return strings.TrimSpace(text)
}
