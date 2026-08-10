package ytdlp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/log"
	ytdlp "github.com/lrstanley/go-ytdlp"

	"github.com/krau/SaveAny-Bot/config"
	"github.com/krau/SaveAny-Bot/pkg/enums/ctxkey"
	"github.com/krau/SaveAny-Bot/pkg/storagetypes"
)

// Execute implements core.Executable.
func (t *Task) Execute(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Infof("Starting yt-dlp download task %s", t.ID)

	if t.Progress != nil {
		t.Progress.OnStart(ctx, t)
	}

	// Create temporary directory for downloads
	tempDir, err := os.MkdirTemp(config.C().Temp.BasePath, "ytdlp-*")
	if err != nil {
		logger.Errorf("Failed to create temp directory: %v", err)
		if t.Progress != nil {
			t.Progress.OnDone(ctx, t, err)
		}
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir) // Clean up temp directory

	logger.Debugf("Created temp directory: %s", tempDir)

	// Download files using yt-dlp
	downloadedFiles, err := t.downloadFiles(ctx, tempDir)
	if err != nil {
		logger.Errorf("yt-dlp download failed: %v", err)
		if t.Progress != nil {
			t.Progress.OnDone(ctx, t, err)
		}
		return err
	}

	if len(downloadedFiles) == 0 {
		err := errors.New("no files were downloaded")
		logger.Error(err.Error())
		if t.Progress != nil {
			t.Progress.OnDone(ctx, t, err)
		}
		return err
	}

	metadata := loadMetadata(ctx, tempDir)
	logger.Infof("Transferring %d file(s) to storage %s", len(downloadedFiles), t.Storage.Name())
	for _, filePath := range downloadedFiles {
		if err := t.transferFile(ctx, filePath, metadata[metadataKey(filePath)]); err != nil {
			logger.Errorf("File transfer failed: %v", err)
			if t.Progress != nil {
				t.Progress.OnDone(ctx, t, err)
			}
			return err
		}
	}

	logger.Infof("yt-dlp task %s completed successfully", t.ID)
	if t.Progress != nil {
		t.Progress.OnDone(ctx, t, nil)
	}

	return nil
}

// downloadFiles downloads files using yt-dlp and returns the list of downloaded file paths
func (t *Task) downloadFiles(ctx context.Context, tempDir string) ([]string, error) {
	logger := log.FromContext(ctx)

	// Configure yt-dlp command with essential settings
	// Always set output path to ensure files go to temp directory
	cmd := ytdlp.New().
		Output(filepath.Join(tempDir, "%(title)s.%(ext)s"))

	// Apply config-based format/quality defaults only when the user passes no
	// custom flags. Any user flag means they take full control of yt-dlp.
	if len(t.Flags) == 0 {
		cmd = applyFormatConfig(cmd, config.C().Ytdlp)
	}
	// Note: If custom flags are provided, users have full control over format/quality
	// The output path is always set above to ensure downloads go to the correct directory

	if t.Progress != nil {
		t.Progress.OnProgress(ctx, t, "Downloading...")
	}

	args := t.buildArgs(config.C().Ytdlp)
	logger.Infof("Executing yt-dlp for %d URL(s) with %d flag(s)", len(t.URLs), len(args)-len(t.URLs))

	// Run with context for cancellation support
	result, err := cmd.Run(ctx, args...)
	if err != nil {
		// Check if context was canceled
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		return nil, fmt.Errorf("yt-dlp execution failed: %w", err)
	}

	if result.ExitCode != 0 {
		return nil, fmt.Errorf("yt-dlp exited with code %d: %s", result.ExitCode, result.Stderr)
	}

	// List downloaded files
	files, err := os.ReadDir(tempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read temp directory: %w", err)
	}

	var downloadedFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.EqualFold(filepath.Ext(file.Name()), ".json") {
			continue
		}
		fullPath := filepath.Join(tempDir, file.Name())
		downloadedFiles = append(downloadedFiles, fullPath)
		logger.Debugf("Downloaded file: %s", file.Name())
	}

	return downloadedFiles, nil
}

func (t *Task) buildArgs(cfg config.YtdlpConfig) []string {
	flags := append([]string(nil), t.Flags...)
	if !hasWriteInfoJSONFlag(flags) {
		flags = append(flags, "--write-info-json")
	}
	if strings.TrimSpace(cfg.MergeOutputFormat) != "" && !hasMergeOutputFormatFlag(flags) {
		flags = append(flags, "--merge-output-format", cfg.MergeOutputFormat)
	}
	if strings.TrimSpace(cfg.Cookies) != "" && !hasCookieFlag(flags) {
		flags = append(flags, "--cookies", cfg.Cookies)
	}
	return append(flags, t.URLs...)
}

func hasWriteInfoJSONFlag(flags []string) bool {
	for _, flag := range flags {
		if flag == "--write-info-json" || flag == "--no-write-info-json" {
			return true
		}
	}
	return false
}

func hasMergeOutputFormatFlag(flags []string) bool {
	for _, flag := range flags {
		if flag == "--merge-output-format" || strings.HasPrefix(flag, "--merge-output-format=") {
			return true
		}
	}
	return false
}

func hasCookieFlag(flags []string) bool {
	for _, flag := range flags {
		if flag == "--cookies" || strings.HasPrefix(flag, "--cookies=") ||
			flag == "--cookies-from-browser" || strings.HasPrefix(flag, "--cookies-from-browser=") {
			return true
		}
	}
	return false
}

type videoMetadata struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	WebpageURL  string `json:"webpage_url"`
}

func loadMetadata(ctx context.Context, tempDir string) map[string]videoMetadata {
	logger := log.FromContext(ctx)
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		logger.Warnf("Failed to read yt-dlp metadata directory: %v", err)
		return nil
	}
	metadata := make(map[string]videoMetadata)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".info.json") {
			continue
		}
		path := filepath.Join(tempDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			logger.Warnf("Failed to read yt-dlp metadata %s: %v", entry.Name(), err)
			continue
		}
		var meta videoMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			logger.Warnf("Failed to parse yt-dlp metadata %s: %v", entry.Name(), err)
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".info.json")
		metadata[base] = meta
	}
	return metadata
}

func metadataKey(filePath string) string {
	name := filepath.Base(filePath)
	return strings.TrimSuffix(name, filepath.Ext(name))
}

func captionFromMetadata(meta videoMetadata) (string, bool) {
	parts := make([]string, 0, 2)
	if title := strings.TrimSpace(meta.Title); title != "" {
		parts = append(parts, title)
	}
	if description := strings.TrimSpace(meta.Description); description != "" {
		parts = append(parts, description)
	}
	caption := strings.TrimSpace(strings.Join(parts, "\n\n"))
	return caption, caption != ""
}

// transferFile transfers a single file to storage
func (t *Task) transferFile(ctx context.Context, filePath string, meta videoMetadata) error {
	logger := log.FromContext(ctx)

	// Check if file exists
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warnf("Downloaded file not found: %s", filePath)
			return nil // Not a fatal error
		}
		return fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}

	// Open file
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer f.Close()

	// Set content length in context for storage
	ctx = context.WithValue(ctx, ctxkey.ContentLength, fileInfo.Size())
	if caption, ok := captionFromMetadata(meta); ok {
		ctx = storagetypes.WithSourceCaption(ctx, caption)
	}

	// Save to storage
	fileName := filepath.Base(filePath)
	// Remove special characters from filename if needed
	fileName = sanitizeFilename(fileName)
	destPath := filepath.Join(t.StorPath, fileName)

	logger.Infof("Transferring file %s to %s:%s", fileName, t.Storage.Name(), destPath)

	if err := t.Storage.Save(ctx, f, destPath); err != nil {
		return fmt.Errorf("failed to save file %s to storage: %w", fileName, err)
	}

	logger.Infof("Successfully transferred file %s", fileName)

	if t.Progress != nil {
		t.Progress.OnProgress(ctx, t, fmt.Sprintf("Transferred: %s", fileName))
	}

	return nil
}

// sanitizeFilename removes or replaces problematic characters in filenames
func sanitizeFilename(name string) string {
	// yt-dlp with --restrict-filenames should already handle most cases
	// but we can do additional sanitization if needed
	name = strings.ReplaceAll(name, ":", "_")
	name = strings.ReplaceAll(name, "\"", "'")
	return name
}
