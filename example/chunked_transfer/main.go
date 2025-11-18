package main

import (
	"context"
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
	chunkSizeBytes = 400 * 1024 * 1024
	chunkCount     = 10
)

var totalSizeBytes = int64(chunkSizeBytes * chunkCount)

type config struct {
	sourcePath  string
	splitDir    string
	downloadDir string
	indexerURL  string
	nodeList    string
	rpcURL      string
	privateKey  string
	expectedRep uint
	taskSize    uint
	method      string
	fullTrusted bool
	withProof   bool
	timeout     time.Duration
	routines    int
	nRetries    int
	step        int64
}

type chunkResult struct {
	path   string
	root   string
	txHash string
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		logrus.Fatalf("execution failed: %v", err)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.sourcePath, "file", "", "Path to the 4 GiB source file")
	flag.StringVar(&cfg.splitDir, "split-dir", "./chunks", "Directory to store 400 MiB chunk files")
	flag.StringVar(&cfg.downloadDir, "download-dir", "./downloads", "Directory for downloaded chunks")
	flag.StringVar(&cfg.rpcURL, "url", "", "RPC URL for blockchain node")
	flag.StringVar(&cfg.privateKey, "key", "", "Private key for signing transactions")
	flag.StringVar(&cfg.indexerURL, "indexer", "", "Indexer endpoint (required unless --nodes is provided)")
	flag.StringVar(&cfg.nodeList, "nodes", "", "Comma separated storage node URLs (optional alternative to --indexer)")
	flag.UintVar(&cfg.expectedRep, "expected-replica", 1, "Expected replication factor for uploads")
	flag.UintVar(&cfg.taskSize, "task-size", 10, "Segments per upload RPC")
	flag.StringVar(&cfg.method, "method", "min", "Node selection method when querying indexer")
	flag.BoolVar(&cfg.fullTrusted, "full-trusted", true, "Restrict uploads to trusted nodes only")
	flag.BoolVar(&cfg.withProof, "download-proof", false, "Download with Merkle proof verification")
	flag.DurationVar(&cfg.timeout, "timeout", 0, "Optional end-to-end timeout (e.g. 30m, 2h)")
	flag.IntVar(&cfg.routines, "routines", runtime.GOMAXPROCS(0), "Number of routines for upload/download workers")
	flag.IntVar(&cfg.nRetries, "n-retries", 5, "Retries for gas price adjustments when submitting txs")
	flag.Int64Var(&cfg.step, "gas-step", 15, "Gas multiplier step (15 => 1.5x)")
	flag.Parse()
	return cfg
}

func run(cfg config) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}

	if err := os.MkdirAll(cfg.splitDir, 0o755); err != nil {
		return fmt.Errorf("create split dir: %w", err)
	}
	if err := os.MkdirAll(cfg.downloadDir, 0o755); err != nil {
		return fmt.Errorf("create download dir: %w", err)
	}

	ctx := context.Background()
	var cancel context.CancelFunc
	if cfg.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}

	logrus.Infof("Splitting %s into %d × %d MiB chunks ...", cfg.sourcePath, chunkCount, chunkSizeBytes/(1024*1024))
	chunkPaths, err := splitFileIntoChunks(cfg.sourcePath, cfg.splitDir)
	if err != nil {
		return err
	}

	w3client := blockchain.MustNewWeb3(cfg.rpcURL, cfg.privateKey)
	defer w3client.Close()

	env, err := newTransferEnv(ctx, cfg, w3client)
	if err != nil {
		return err
	}
	defer env.Close()

	results := make([]chunkResult, 0, len(chunkPaths))
	for idx, path := range chunkPaths {
		logrus.Infof("Uploading chunk %d/%d: %s", idx+1, len(chunkPaths), path)
		res, err := uploadChunk(ctx, env, cfg, path)
		if err != nil {
			return fmt.Errorf("upload chunk %s: %w", path, err)
		}
		results = append(results, res)
		logrus.Infof("Uploaded chunk %s; tx=%s root=%s", filepath.Base(path), res.txHash, res.root)
	}

	for _, res := range results {
		target := filepath.Join(cfg.downloadDir, filepath.Base(res.path))
		logrus.Infof("Downloading chunk %s to %s ...", res.root, target)
		if err := env.downloader.Download(ctx, res.root, target, cfg.withProof); err != nil {
			return fmt.Errorf("download %s: %w", res.root, err)
		}
	}

	logrus.Infof("Completed splitting, uploading, and downloading %d chunks.", len(results))
	return nil
}

func validateConfig(cfg config) error {
	if cfg.sourcePath == "" {
		return errors.New("--file is required")
	}
	if cfg.rpcURL == "" || cfg.privateKey == "" {
		return errors.New("--url and --key are required")
	}
	if cfg.indexerURL == "" && cfg.nodeList == "" {
		return errors.New("either --indexer or --nodes must be provided")
	}
	if cfg.indexerURL != "" && cfg.nodeList != "" {
		return errors.New("--indexer and --nodes are mutually exclusive")
	}
	return nil
}

func splitFileIntoChunks(srcPath, destDir string) ([]string, error) {
	info, err := os.Stat(srcPath)
	if err != nil {
		return nil, fmt.Errorf("stat source file: %w", err)
	}
	if info.Size() != totalSizeBytes {
		return nil, fmt.Errorf("expected file size %d bytes (4 GiB), got %d", totalSizeBytes, info.Size())
	}

	in, err := os.Open(srcPath)
	if err != nil {
		return nil, fmt.Errorf("open source file: %w", err)
	}
	defer in.Close()

	base := filepath.Base(srcPath)
	chunkPaths := make([]string, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunkPath := filepath.Join(destDir, fmt.Sprintf("%s.part%02d", base, i))
		out, err := os.Create(chunkPath)
		if err != nil {
			return nil, fmt.Errorf("create chunk %s: %w", chunkPath, err)
		}
		if _, err := io.CopyN(out, in, chunkSizeBytes); err != nil {
			out.Close()
			return nil, fmt.Errorf("split chunk %d: %w", i, err)
		}
		if err := out.Close(); err != nil {
			return nil, fmt.Errorf("close chunk %s: %w", chunkPath, err)
		}
		chunkPaths = append(chunkPaths, chunkPath)
	}

	if _, err := io.CopyN(io.Discard, in, 1); err == nil {
		return nil, fmt.Errorf("source file larger than %d bytes", totalSizeBytes)
	} else if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("post-split read failed: %w", err)
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
		client, err := indexer.NewClient(cfg.indexerURL, indexer.IndexerClientOption{
			LogOption:   logOpt,
			FullTrusted: cfg.fullTrusted,
		})
		if err != nil {
			return nil, fmt.Errorf("init indexer client: %w", err)
		}
		env.indexer = client
		env.downloader = client
		return env, nil
	}

	urls := splitNodes(cfg.nodeList)
	if len(urls) == 0 {
		return nil, errors.New("no storage nodes provided")
	}
	clients := node.MustNewZgsClients(urls)
	env.nodes = clients

	uploader, err := transfer.NewUploader(ctx, w3, &transfer.SelectedNodes{Trusted: clients}, logOpt)
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("init uploader: %w", err)
	}
	uploader.WithRoutines(cfg.routines)
	env.uploader = uploader

	downloader, err := transfer.NewDownloader(clients, logOpt)
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("init downloader: %w", err)
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

func (e *transferEnv) uploaderFor(ctx context.Context, segNum uint64) (*transfer.Uploader, func(), error) {
	if e.indexer != nil {
		uploader, err := e.indexer.NewUploaderFromIndexerNodes(ctx, segNum, e.w3, e.cfg.expectedRep, nil, e.cfg.method, e.cfg.fullTrusted)
		if err != nil {
			return nil, nil, fmt.Errorf("select nodes from indexer: %w", err)
		}
		uploader.WithRoutines(e.cfg.routines)
		return uploader, func() {}, nil
	}
	return e.uploader, func() {}, nil
}

func uploadChunk(ctx context.Context, env *transferEnv, cfg config, path string) (chunkResult, error) {
	file, err := core.Open(path)
	if err != nil {
		return chunkResult{}, fmt.Errorf("open chunk: %w", err)
	}
	defer file.Close()

	uploader, closer, err := env.uploaderFor(ctx, file.NumSegments())
	if err != nil {
		return chunkResult{}, err
	}
	defer closer()

	opt := transfer.UploadOption{
		FinalityRequired: transfer.FileFinalized,
		ExpectedReplica:  cfg.expectedRep,
		TaskSize:         cfg.taskSize,
		Method:           cfg.method,
		FullTrusted:      cfg.fullTrusted,
		SkipTx:           false,
		NRetries:         cfg.nRetries,
		Step:             cfg.step,
	}

	txHash, root, err := uploader.Upload(ctx, file, opt)
	if err != nil {
		return chunkResult{}, err
	}
	return chunkResult{
		path:   path,
		root:   root.Hex(),
		txHash: txHash.Hex(),
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


