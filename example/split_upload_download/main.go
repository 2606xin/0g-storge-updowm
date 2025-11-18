package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	zg_common "github.com/0glabs/0g-storage-client/common"
	"github.com/0glabs/0g-storage-client/common/blockchain"
	"github.com/0glabs/0g-storage-client/core"
	"github.com/0glabs/0g-storage-client/indexer"
	"github.com/0glabs/0g-storage-client/node"
	"github.com/0glabs/0g-storage-client/transfer"
	"github.com/openweb3/web3go"
	"github.com/sirupsen/logrus"
)

const (
	chunkSizeBytes = 400 * 1024 * 1024 // 10MB
	chunkCount     = 10               // 10个分片
	stateFileName  = "upload_state.json"
)

type config struct {
	sourcePath        string
	splitDir          string
	downloadDir       string
	indexerURL        string
	nodeList          string
	rpcURL            string
	privateKey        string
	expectedRep       uint
	taskSize          uint
	method            string
	fullTrusted       bool
	withProof         bool
	timeout           time.Duration
	routines          int
	nRetries          int
	step              int64
	chunkRetries      int
	retryDelay        time.Duration
	fallbackUntrusted bool
}

type chunkResult struct {
	chunkIndex int
	path       string
	root       string
	txHash     string
}

type uploadState struct {
	Results map[string]chunkResult `json:"results"`
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		logrus.Fatalf("执行失败: %v", err)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.sourcePath, "file", "", "要上传的4GB源文件路径")
	flag.StringVar(&cfg.splitDir, "split-dir", "./chunks", "存储400MB分片文件的目录")
	flag.StringVar(&cfg.downloadDir, "download-dir", "./downloads", "下载分片的目录")
	flag.StringVar(&cfg.rpcURL, "url", "", "区块链节点的RPC URL")
	flag.StringVar(&cfg.privateKey, "key", "", "用于签名交易的私钥")
	flag.StringVar(&cfg.indexerURL, "indexer", "", "索引器端点（除非提供--nodes，否则必需）")
	flag.StringVar(&cfg.nodeList, "nodes", "", "逗号分隔的存储节点URL列表（--indexer的替代选项）")
	flag.UintVar(&cfg.expectedRep, "expected-replica", 1, "上传的预期复制因子")
	flag.UintVar(&cfg.taskSize, "task-size", 10, "每次上传RPC的段数")
	flag.StringVar(&cfg.method, "method", "min", "查询索引器时的节点选择方法")
	flag.BoolVar(&cfg.fullTrusted, "full-trusted", true, "仅限制上传到受信任的节点")
	flag.BoolVar(&cfg.withProof, "download-proof", false, "下载时使用Merkle证明验证")
	flag.DurationVar(&cfg.timeout, "timeout", 0, "可选的端到端超时（例如：30m, 2h）")
	flag.IntVar(&cfg.routines, "routines", runtime.GOMAXPROCS(0), "上传/下载工作线程的协程数")
	flag.IntVar(&cfg.nRetries, "n-retries", 5, "提交交易时gas价格调整的重试次数")
	flag.Int64Var(&cfg.step, "gas-step", 15, "gas乘数步长（15 => 1.5x）")
	flag.IntVar(&cfg.chunkRetries, "chunk-retries", 5, "每个分片的最大上传尝试次数（>=1）")
	flag.DurationVar(&cfg.retryDelay, "chunk-retry-delay", 5*time.Second, "分片上传失败后的等待时间")
	flag.BoolVar(&cfg.fallbackUntrusted, "fallback-untrusted", true, "在失败重试时允许自动切换到非 full-trusted 节点")
	flag.Parse()
	return cfg
}

func run(cfg config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	// 创建必要的目录
	if err := os.MkdirAll(cfg.splitDir, 0o755); err != nil {
		return fmt.Errorf("创建分片目录失败: %w", err)
	}
	if err := os.MkdirAll(cfg.downloadDir, 0o755); err != nil {
		return fmt.Errorf("创建下载目录失败: %w", err)
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if cfg.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	// 步骤1: 将4GB文件切分成10个400MB的文件
	logrus.Infof("正在将 %s 切分成 %d 个 %d MB 的分片...", cfg.sourcePath, chunkCount, chunkSizeBytes/(1024*1024))
	chunkPaths, err := splitFileIntoChunks(cfg.sourcePath, cfg.splitDir)
	if err != nil {
		return fmt.Errorf("切分文件失败: %w", err)
	}
	logrus.Infof("成功切分成 %d 个分片", len(chunkPaths))

	state, err := loadUploadState(cfg.splitDir)
	if err != nil {
		return fmt.Errorf("加载上传状态失败: %w", err)
	}
	if state.Results == nil {
		state.Results = make(map[string]chunkResult)
	}

	// 初始化Web3客户端
	w3client := blockchain.MustNewWeb3(cfg.rpcURL, cfg.privateKey)
	defer w3client.Close()

	// 初始化传输环境
	env, err := newTransferEnv(ctx, cfg, w3client)
	if err != nil {
		return fmt.Errorf("初始化传输环境失败: %w", err)
	}
	defer env.Close()

	// 步骤2: 上传所有分片
	logrus.Infof("开始上传 %d 个分片...", len(chunkPaths))
	results := make([]chunkResult, 0, len(chunkPaths))
	for idx, path := range chunkPaths {
		if cached, ok := state.Results[path]; ok {
			cached.chunkIndex = idx
			state.Results[path] = cached
			results = append(results, cached)
			logrus.Infof("检测到分片 %d 已上传 (root=%s)，跳过重传", idx+1, cached.root)
			continue
		}

		logrus.Infof("正在上传分片 %d/%d: %s", idx+1, len(chunkPaths), filepath.Base(path))
		res, err := uploadChunkWithRetry(ctx, env, cfg, idx, path)
		if err != nil {
			return fmt.Errorf("上传分片 %s 失败: %w", path, err)
		}
		state.Results[path] = res
		if err := saveUploadState(cfg.splitDir, state); err != nil {
			return fmt.Errorf("保存上传状态失败: %w", err)
		}
		results = append(results, res)
		logrus.Infof("分片 %d 上传成功; tx=%s root=%s", idx+1, res.txHash, res.root)
	}
	logrus.Infof("所有分片上传完成！")

	// 步骤3: 下载所有分片
	logrus.Infof("开始下载 %d 个分片...", len(results))
	for _, res := range results {
		target := filepath.Join(cfg.downloadDir, fmt.Sprintf("chunk_%02d.bin", res.chunkIndex))
		logrus.Infof("正在下载分片 %d (root=%s) 到 %s ...", res.chunkIndex+1, res.root, target)
		if err := env.downloader.Download(ctx, res.root, target, cfg.withProof); err != nil {
			return fmt.Errorf("下载分片 %s 失败: %w", res.root, err)
		}
		logrus.Infof("分片 %d 下载成功", res.chunkIndex+1)
	}

	// 步骤4: 合并下载的分片
	logrus.Info("开始合并下载的分片...")
	mergedFile := filepath.Join(cfg.downloadDir, filepath.Base(cfg.sourcePath)+".merged")
	if err := mergeChunks(cfg.downloadDir, mergedFile, len(results)); err != nil {
		return fmt.Errorf("合并分片失败: %w", err)
	}
	logrus.Infof("分片合并成功！输出文件: %s", mergedFile)

	// 步骤5: 验证合并后的文件
	if err := verifyMergedFile(cfg.sourcePath, mergedFile); err != nil {
		logrus.Warnf("文件验证: %v", err)
	} else {
		logrus.Info("✓ 文件验证成功！合并后的文件与原始文件完全一致")
	}

	logrus.Infof("完成！已成功切分、上传、下载和合并 %d 个分片。", len(results))
	logrus.Infof("分片文件保存在: %s", cfg.splitDir)
	logrus.Infof("下载文件保存在: %s", cfg.downloadDir)
	logrus.Infof("合并文件保存在: %s", mergedFile)
	return nil
}

func validateConfig(cfg config) error {
	if cfg.sourcePath == "" {
		return errors.New("--file 参数是必需的")
	}
	if cfg.rpcURL == "" || cfg.privateKey == "" {
		return errors.New("--url 和 --key 参数是必需的")
	}
	if cfg.indexerURL == "" && cfg.nodeList == "" {
		return errors.New("必须提供 --indexer 或 --nodes 参数之一")
	}
	if cfg.indexerURL != "" && cfg.nodeList != "" {
		return errors.New("--indexer 和 --nodes 参数互斥")
	}
	return nil
}

func splitFileIntoChunks(srcPath, destDir string) ([]string, error) {
	// 检查源文件是否存在
	info, err := os.Stat(srcPath)
	if err != nil {
		return nil, fmt.Errorf("获取源文件信息失败: %w", err)
	}

	expectedSize := int64(chunkSizeBytes * chunkCount)
	if info.Size() != expectedSize {
		logrus.Warnf("源文件大小 (%d 字节) 与预期大小 (%d 字节) 不匹配，将按实际大小切分", info.Size(), expectedSize)
	}

	// 打开源文件
	in, err := os.Open(srcPath)
	if err != nil {
		return nil, fmt.Errorf("打开源文件失败: %w", err)
	}
	defer in.Close()

	base := filepath.Base(srcPath)
	chunkPaths := make([]string, 0, chunkCount)

	// 切分文件
	for i := 0; i < chunkCount; i++ {
		chunkPath := filepath.Join(destDir, fmt.Sprintf("%s.part%02d", base, i))
		out, err := os.Create(chunkPath)
		if err != nil {
			return nil, fmt.Errorf("创建分片文件 %s 失败: %w", chunkPath, err)
		}

		// 复制数据到分片文件
		written, err := io.CopyN(out, in, chunkSizeBytes)
		if err != nil && err != io.EOF {
			out.Close()
			return nil, fmt.Errorf("切分分片 %d 失败: %w", i, err)
		}

		if err := out.Close(); err != nil {
			return nil, fmt.Errorf("关闭分片文件 %s 失败: %w", chunkPath, err)
		}

		// 如果写入的数据少于预期，说明文件已经读取完毕
		if written < chunkSizeBytes {
			logrus.Infof("分片 %d 实际大小: %d 字节（小于预期的 %d 字节）", i, written, chunkSizeBytes)
			chunkPaths = append(chunkPaths, chunkPath)
			break
		}

		chunkPaths = append(chunkPaths, chunkPath)
		logrus.Infof("创建分片 %d: %s (%d 字节)", i+1, chunkPath, written)
	}

	return chunkPaths, nil
}

type transferEnv struct {
	cfg        config
	w3         *web3go.Client
	indexer    *indexer.Client
	nodes      []*node.ZgsClient
	uploader   *transfer.Uploader
	downloader transfer.IDownloader
}

func newTransferEnv(ctx context.Context, cfg config, w3 *web3go.Client) (*transferEnv, error) {
	env := &transferEnv{
		cfg: cfg,
		w3:  w3,
	}

	logOpt := zg_common.LogOption{Logger: logrus.StandardLogger()}

	if cfg.indexerURL != "" {
		// 使用索引器
		client, err := indexer.NewClient(cfg.indexerURL, indexer.IndexerClientOption{
			LogOption:   logOpt,
			FullTrusted: cfg.fullTrusted,
		})
		if err != nil {
			return nil, fmt.Errorf("初始化索引器客户端失败: %w", err)
		}
		env.indexer = client
		env.downloader = client
		return env, nil
	}

	// 使用直接节点列表
	urls := splitNodes(cfg.nodeList)
	if len(urls) == 0 {
		return nil, errors.New("未提供存储节点")
	}
	clients := node.MustNewZgsClients(urls)
	env.nodes = clients

	uploader, err := transfer.NewUploader(ctx, w3, &transfer.SelectedNodes{Trusted: clients}, logOpt)
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("初始化上传器失败: %w", err)
	}
	uploader.WithRoutines(cfg.routines)
	env.uploader = uploader

	downloader, err := transfer.NewDownloader(clients, logOpt)
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("初始化下载器失败: %w", err)
	}
	downloader.WithRoutines(cfg.routines)
	env.downloader = downloader

	return env, nil
}

func (e *transferEnv) Close() {
	if e.indexer != nil {
		e.indexer.Close()
	}
	for _, c := range e.nodes {
		c.Close()
	}
}

func (e *transferEnv) uploaderFor(ctx context.Context, segNum uint64, dropped []string, fullTrusted bool) (*transfer.Uploader, func(), error) {
	if e.indexer != nil {
		uploader, err := e.indexer.NewUploaderFromIndexerNodes(ctx, segNum, e.w3, e.cfg.expectedRep, dropped, e.cfg.method, fullTrusted)
		if err != nil {
			return nil, nil, fmt.Errorf("从索引器选择节点失败: %w", err)
		}
		uploader.WithRoutines(e.cfg.routines)
		return uploader, func() {}, nil
	}
	return e.uploader, func() {}, nil
}

func uploadChunk(ctx context.Context, env *transferEnv, cfg config, chunkIndex int, path string, fullTrusted bool, dropped []string) (chunkResult, error) {
	file, err := core.Open(path)
	if err != nil {
		return chunkResult{}, fmt.Errorf("打开分片文件失败: %w", err)
	}
	defer file.Close()

	uploader, closer, err := env.uploaderFor(ctx, file.NumSegments(), dropped, fullTrusted)
	if err != nil {
		return chunkResult{}, err
	}
	defer closer()

	opt := transfer.UploadOption{
		FinalityRequired: transfer.FileFinalized,
		ExpectedReplica:  cfg.expectedRep,
		TaskSize:         cfg.taskSize,
		Method:           cfg.method,
		FullTrusted:      fullTrusted,
		SkipTx:           false,
		NRetries:         cfg.nRetries,
		Step:             cfg.step,
	}

	txHash, root, err := uploader.Upload(ctx, file, opt)
	if err != nil {
		return chunkResult{}, err
	}
	return chunkResult{
		chunkIndex: chunkIndex,
		path:       path,
		root:       root.Hex(),
		txHash:     txHash.Hex(),
	}, nil
}

func splitNodes(list string) []string {
	parts := strings.Split(list, ",")
	out := make([]string, 0, len(parts))
	for _, item := range parts {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func uploadChunkWithRetry(ctx context.Context, env *transferEnv, cfg config, chunkIndex int, path string) (chunkResult, error) {
	attempts := cfg.chunkRetries
	if attempts < 1 {
		attempts = 1
	}
	fullTrusted := cfg.fullTrusted
	dropped := make([]string, 0)
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		res, err := uploadChunk(ctx, env, cfg, chunkIndex, path, fullTrusted, dropped)
		if err == nil {
			if attempt > 1 {
				logrus.Infof("分片 %d 在第 %d 次尝试后成功", chunkIndex+1, attempt)
			}
			return res, nil
		}

		lastErr = err
		logrus.WithFields(logrus.Fields{
			"chunk":   chunkIndex + 1,
			"attempt": attempt,
			"total":   attempts,
		}).Warnf("分片上传失败: %v", err)

		var rpcErr *node.RPCError
		if errors.As(err, &rpcErr) {
			if !containsString(dropped, rpcErr.URL) {
				dropped = append(dropped, rpcErr.URL)
				logrus.Warnf("节点 %s 被标记为不可用，将在后续重试中跳过", rpcErr.URL)
			}
		}

		if attempt == attempts {
			break
		}

		if cfg.indexerURL != "" && cfg.fallbackUntrusted && fullTrusted {
			logrus.Warn("切换到非 full-trusted 节点集进行重试")
			fullTrusted = false
		}

		if err := sleepWithContext(ctx, cfg.retryDelay); err != nil {
			return chunkResult{}, err
		}
	}

	return chunkResult{}, fmt.Errorf("分片 %s 在 %d 次尝试后失败: %w", filepath.Base(path), attempts, lastErr)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func containsString(list []string, target string) bool {
	for _, item := range list {
		if item == target {
			return true
		}
	}
	return false
}

func loadUploadState(dir string) (uploadState, error) {
	path := filepath.Join(dir, stateFileName)
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return uploadState{Results: make(map[string]chunkResult)}, nil
		}
		return uploadState{}, err
	}
	defer file.Close()

	var state uploadState
	if err := json.NewDecoder(file).Decode(&state); err != nil {
		return uploadState{}, err
	}
	if state.Results == nil {
		state.Results = make(map[string]chunkResult)
	}
	return state, nil
}

func saveUploadState(dir string, state uploadState) error {
	path := filepath.Join(dir, stateFileName)
	tmpPath := path + ".tmp"

	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(state); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// mergeChunks 合并下载的分片文件成一个完整文件
func mergeChunks(downloadDir, outputPath string, numChunks int) error {
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer outFile.Close()

	var totalBytes int64
	for i := 0; i < numChunks; i++ {
		chunkPath := filepath.Join(downloadDir, fmt.Sprintf("chunk_%02d.bin", i))
		
		logrus.Infof("正在合并分片 %d/%d: %s", i+1, numChunks, filepath.Base(chunkPath))
		
		inFile, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("打开分片文件 %s 失败: %w", chunkPath, err)
		}

		written, err := io.Copy(outFile, inFile)
		inFile.Close()

		if err != nil {
			return fmt.Errorf("复制分片 %s 失败: %w", chunkPath, err)
		}

		totalBytes += written
		logrus.Infof("已合并分片 %d (%s)", i+1, formatBytesSize(written))
	}

	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("同步输出文件失败: %w", err)
	}

	logrus.Infof("合并完成！总大小: %s", formatBytesSize(totalBytes))
	return nil
}

// verifyMergedFile 验证合并后的文件与原始文件是否一致
func verifyMergedFile(originalPath, mergedPath string) error {
	// 检查文件大小
	origInfo, err := os.Stat(originalPath)
	if err != nil {
		return fmt.Errorf("无法获取原始文件信息: %w", err)
	}

	mergedInfo, err := os.Stat(mergedPath)
	if err != nil {
		return fmt.Errorf("无法获取合并文件信息: %w", err)
	}

	if origInfo.Size() != mergedInfo.Size() {
		return fmt.Errorf("文件大小不匹配: 原始=%d 字节, 合并=%d 字节", origInfo.Size(), mergedInfo.Size())
	}

	logrus.Infof("文件大小匹配: %s", formatBytesSize(origInfo.Size()))

	// 比较文件内容
	logrus.Info("正在验证文件内容...")
	origFile, err := os.Open(originalPath)
	if err != nil {
		return fmt.Errorf("打开原始文件失败: %w", err)
	}
	defer origFile.Close()

	mergedFile, err := os.Open(mergedPath)
	if err != nil {
		return fmt.Errorf("打开合并文件失败: %w", err)
	}
	defer mergedFile.Close()

	bufSize := 1024 * 1024 // 1MB buffer
	origBuf := make([]byte, bufSize)
	mergedBuf := make([]byte, bufSize)

	var totalRead int64
	for {
		n1, err1 := io.ReadFull(origFile, origBuf)
		n2, err2 := io.ReadFull(mergedFile, mergedBuf)

		if n1 != n2 {
			return fmt.Errorf("读取字节数不匹配: 原始=%d, 合并=%d (位置=%d)", n1, n2, totalRead)
		}

		if n1 > 0 {
			for i := 0; i < n1; i++ {
				if origBuf[i] != mergedBuf[i] {
					return fmt.Errorf("文件内容不匹配: 位置=%d", totalRead+int64(i))
				}
			}
			totalRead += int64(n1)
		}

		if err1 == io.EOF && err2 == io.EOF {
			break
		}
		if err1 == io.ErrUnexpectedEOF && err2 == io.ErrUnexpectedEOF {
			break
		}
		if err1 != nil && err1 != io.EOF && err1 != io.ErrUnexpectedEOF {
			return fmt.Errorf("读取原始文件失败: %w", err1)
		}
		if err2 != nil && err2 != io.EOF && err2 != io.ErrUnexpectedEOF {
			return fmt.Errorf("读取合并文件失败: %w", err2)
		}
	}

	logrus.Infof("已验证 %s 数据", formatBytesSize(totalRead))
	return nil
}

// formatBytesSize 格式化字节大小为人类可读格式
func formatBytesSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
