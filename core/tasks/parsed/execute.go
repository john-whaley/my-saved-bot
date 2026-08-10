package parsed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/log"
	"github.com/duke-git/lancet/v2/retry"
	"github.com/krau/SaveAny-Bot/common/utils/fsutil"
	"github.com/krau/SaveAny-Bot/common/utils/ioutil"
	"github.com/krau/SaveAny-Bot/config"
	"github.com/krau/SaveAny-Bot/pkg/enums/ctxkey"
	"github.com/krau/SaveAny-Bot/pkg/parser"
	"github.com/krau/SaveAny-Bot/pkg/storagetypes"
	"github.com/krau/SaveAny-Bot/pkg/taskevent"
	"github.com/krau/SaveAny-Bot/storage"
	"golang.org/x/sync/errgroup"
)

func (t *Task) Execute(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Infof("Starting Parsed item task %s", t.item.Title)
	if t.progress != nil {
		t.progress.OnStart(ctx, t)
	}

	var err error
	if batchSaver, ok := t.Stor.(storage.StorageBatchSaver); ok && len(t.item.Resources) > 1 && !t.stream {
		err = t.processResourceBatch(ctx, batchSaver)
	} else {
		err = t.processResources(ctx)
	}

	if err != nil {
		logger.Errorf("Error during Parsed item task execution: %v", err)
	} else {
		logger.Infof("Parsed item task %s completed successfully", t.item.Title)
	}
	if t.progress != nil {
		t.progress.OnDone(ctx, t, err)
	}
	return err
}

func (t *Task) processResources(ctx context.Context) error {
	logger := log.FromContext(ctx)
	eg, gctx := errgroup.WithContext(ctx)
	eg.SetLimit(config.C().Workers)
	for _, resource := range t.item.Resources {
		resource := resource
		eg.Go(func() error {
			if err := t.markProcessing(resource); err != nil {
				return err
			}
			defer t.unmarkProcessing(resource)

			err := t.processResource(gctx, resource)
			t.downloaded.Add(1)
			if errors.Is(err, context.Canceled) {
				logger.Debug("Resource processing canceled")
				return err
			}
			if err != nil {
				logger.Errorf("Error processing resource %s: %v", resource.URL, err)
				return fmt.Errorf("failed to process resource %s: %w", resource.URL, err)
			}
			return nil
		})
	}
	return eg.Wait()
}

type cachedResource struct {
	resource parser.Resource
	file     *fsutil.File
	size     int64
}

func (t *Task) processResourceBatch(ctx context.Context, batchSaver storage.StorageBatchSaver) error {
	logger := log.FromContext(ctx)
	cached := make([]*cachedResource, len(t.item.Resources))
	defer func() {
		for _, item := range cached {
			if item == nil || item.file == nil {
				continue
			}
			if err := item.file.CloseAndRemove(); err != nil {
				logger.Warnf("Failed to cleanup parsed batch cache file %s: %v", item.file.Name(), err)
			}
		}
	}()

	eg, gctx := errgroup.WithContext(ctx)
	eg.SetLimit(config.C().Workers)
	for index, resource := range t.item.Resources {
		index, resource := index, resource
		eg.Go(func() error {
			if err := t.markProcessing(resource); err != nil {
				return err
			}
			defer t.unmarkProcessing(resource)

			file, size, err := t.downloadResourceToCache(gctx, resource, index)
			t.downloaded.Add(1)
			if errors.Is(err, context.Canceled) {
				logger.Debug("Resource processing canceled")
				return err
			}
			if err != nil {
				logger.Errorf("Error processing resource %s: %v", resource.URL, err)
				return fmt.Errorf("failed to process resource %s: %w", resource.URL, err)
			}
			cached[index] = &cachedResource{resource: resource, file: file, size: size}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	caption, preserveCaption := t.sourceCaption()
	items := make([]storagetypes.BatchItem, 0, len(cached))
	for _, item := range cached {
		if item == nil || item.file == nil {
			return fmt.Errorf("resource batch cache is incomplete")
		}
		if _, err := item.file.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek parsed batch cache file: %w", err)
		}
		items = append(items, storagetypes.BatchItem{
			Reader:          item.file,
			StoragePath:     path.Join(t.StorPath, item.resource.Filename),
			Size:            item.size,
			SourceGroupKey:  t.sourceGroupKey(),
			Caption:         caption,
			PreserveCaption: preserveCaption,
		})
	}
	return batchSaver.SaveBatch(ctx, items)
}

func (t *Task) markProcessing(resource parser.Resource) error {
	t.processingMu.RLock()
	if t.processing[resource.ID()] != nil {
		t.processingMu.RUnlock()
		return fmt.Errorf("resource %s is already being processed", resource.ID())
	}
	t.processingMu.RUnlock()
	t.processingMu.Lock()
	t.processing[resource.ID()] = &resource
	t.processingMu.Unlock()
	return nil
}

func (t *Task) unmarkProcessing(resource parser.Resource) {
	t.processingMu.Lock()
	delete(t.processing, resource.ID())
	t.processingMu.Unlock()
}

func (t *Task) processResource(ctx context.Context, resource parser.Resource) error {
	logger := log.FromContext(ctx)
	err := retry.Retry(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, resource.URL, nil)
		if err != nil {
			return err
		}
		if resource.Headers != nil {
			for k, v := range resource.Headers {
				req.Header.Set(k, v)
			}
		}
		resp, err := t.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to download resource %s: %w", resource.URL, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to download resource %s: %s", resource.URL, resp.Status)
		}
		ctx = context.WithValue(ctx, ctxkey.ContentLength, func() int64 {
			if resource.Size > 0 {
				return resource.Size
			}
			return resp.ContentLength
		}())
		ctx = t.withSourceCaption(ctx)
		if t.stream {
			return t.Stor.Save(ctx, resp.Body, path.Join(t.StorPath, resource.Filename))
		}
		cacheFile, err := fsutil.CreateFile(filepath.Join(config.C().Temp.BasePath,
			fmt.Sprintf("resource_%s_%s", t.ID, resource.Filename)))
		if err != nil {
			return fmt.Errorf("failed to create cache file for resource %s: %w", resource.URL, err)
		}
		defer func() {
			if err := cacheFile.CloseAndRemove(); err != nil {
				logger.Errorf("Failed to close and remove cache file: %v", err)
			}
		}()
		wr := ioutil.NewProgressWriter(cacheFile, func(n int) {
			downloaded := t.downloadedBytes.Add(int64(n))
			if t.progress != nil {
				t.progress.OnProgress(ctx, t)
			}
			taskevent.Emit(ctx, taskevent.Event{
				TaskID:          t.ID,
				Phase:           taskevent.PhaseProgress,
				TotalBytes:      t.totalBytes,
				DownloadedBytes: downloaded,
			})
		})

		copyResultCh := make(chan error, 1)
		go func() {
			_, err := io.Copy(wr, resp.Body)
			copyResultCh <- err
		}()
		select {
		case err := <-copyResultCh:
			if err != nil {
				return fmt.Errorf("failed to copy resource %s to cache file: %w", resource.URL, err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		_, err = cacheFile.Seek(0, 0)
		if err != nil {
			return fmt.Errorf("failed to seek cache file for resource %s: %w", resource.URL, err)
		}
		return t.Stor.Save(ctx, cacheFile, path.Join(t.StorPath, resource.Filename))
	}, retry.Context(ctx), retry.RetryTimes(uint(config.C().Retry)))
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (t *Task) downloadResourceToCache(ctx context.Context, resource parser.Resource, index int) (*fsutil.File, int64, error) {
	logger := log.FromContext(ctx)
	var cacheFile *fsutil.File
	var size int64
	err := retry.Retry(func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, resource.URL, nil)
		if err != nil {
			return err
		}
		if resource.Headers != nil {
			for k, v := range resource.Headers {
				req.Header.Set(k, v)
			}
		}
		resp, err := t.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to download resource %s: %w", resource.URL, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to download resource %s: %s", resource.URL, resp.Status)
		}
		localFile, err := fsutil.CreateFile(filepath.Join(config.C().Temp.BasePath,
			fmt.Sprintf("parsed_batch_%s_%d_%s", t.ID, index, resource.Filename)))
		if err != nil {
			return fmt.Errorf("failed to create cache file for resource %s: %w", resource.URL, err)
		}
		cleanup := true
		defer func() {
			if cleanup {
				if err := localFile.CloseAndRemove(); err != nil {
					logger.Warnf("Failed to cleanup parsed batch cache file %s: %v", localFile.Name(), err)
				}
			}
		}()
		wr := ioutil.NewProgressWriter(localFile, func(n int) {
			downloaded := t.downloadedBytes.Add(int64(n))
			if t.progress != nil {
				t.progress.OnProgress(ctx, t)
			}
			taskevent.Emit(ctx, taskevent.Event{
				TaskID:          t.ID,
				Phase:           taskevent.PhaseProgress,
				TotalBytes:      t.totalBytes,
				DownloadedBytes: downloaded,
			})
		})

		copyResultCh := make(chan error, 1)
		go func() {
			_, err := io.Copy(wr, resp.Body)
			copyResultCh <- err
		}()
		select {
		case err := <-copyResultCh:
			if err != nil {
				return fmt.Errorf("failed to copy resource %s to cache file: %w", resource.URL, err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		stat, err := localFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat cache file for resource %s: %w", resource.URL, err)
		}
		if _, err := localFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek cache file for resource %s: %w", resource.URL, err)
		}
		size = stat.Size()
		cacheFile = localFile
		cleanup = false
		return nil
	}, retry.Context(ctx), retry.RetryTimes(uint(config.C().Retry)))
	if ctx.Err() != nil {
		return nil, 0, ctx.Err()
	}
	return cacheFile, size, err
}

func (t *Task) sourceCaption() (string, bool) {
	caption := strings.TrimSpace(t.item.Description)
	return caption, caption != ""
}

func (t *Task) withSourceCaption(ctx context.Context) context.Context {
	caption, ok := t.sourceCaption()
	if !ok {
		return ctx
	}
	return storagetypes.WithSourceCaption(ctx, caption)
}

func (t *Task) sourceGroupKey() string {
	if t.item.URL != "" {
		return t.item.URL
	}
	if t.item.Title != "" {
		return t.item.Title
	}
	return t.ID
}
