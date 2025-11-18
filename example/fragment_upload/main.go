package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	defaultFileSize     = int64(4 * 1024 * 1024 * 1024) // 4 GiB
	defaultFragmentSize = int64(400 * 1024 * 1024)      // 400 MiB
	defaultBufferSize   = int64(4 * 1024 * 1024)        // 4 MiB
)

var rootRegexp = regexp.MustCompile(`0x[0-9a-fA-F]{64}`)

type config struct {
	cliPath      string
	tmpDir       string
	downloadDir  string
	url          string
	privateKey   string
	indexer      string
	nodes        string
	tags         string
	fileSize     int64
	fragmentSize int64
	taskSize     int
	timeout      string
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.cliPath, "cli-path", "./0g-storage-client", "Path to 0g-storage-client binary")
	flag.StringVar(&cfg.tmpDir, "workdir", "./tmp-fragments", "Working directory to place generated files")
	flag.StringVar(&cfg.downloadDir, "download-dir", "./downloads", "Directory to restore downloaded fragments")
	flag.StringVar(&cfg.url, "url", "", "RPC URL for blockchain node")
	flag.StringVar(&cfg.privateKey, "key", "", "Private key for signing transactions")
	flag.StringVar(&cfg.indexer, "indexer", "", "Indexer endpoint; required for download")
	flag.StringVar(&cfg.nodes, "nodes", "", "Comma separated storage node RPC endpoints (optional if indexer provided)")
	flag.StringVar(&cfg.tags, "tags", "0x", "Optional tags to attach to upload transaction")
	flag.Int64Var(&cfg.fileSize, "file-size", defaultFileSize, "Size of the generated test file in bytes")
	flag.Int64Var(&cfg.fragmentSize, "fragment-size", defaultFragmentSize, "Fragment size in bytes (4 GiB / 10 = 400 MiB)")
	flag.IntVar(&cfg.taskSize, "task-size", 10, "Number of segments per upload RPC")
	flag.StringVar(&cfg.timeout, "timeout", "0", "Optional CLI timeout duration (e.g. 30m)")
	flag.Parse()
	return cfg
}

func run(cfg config) error {
	if cfg.url == "" || cfg.privateKey == "" {
		return errors.New("--url and --key are required")
	}
	if err := os.MkdirAll(cfg.tmpDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.downloadDir, 0o755); err != nil {
		return err
	}

	sourcePath := filepath.Join(cfg.tmpDir, "sample-4gb.bin")
	fmt.Printf("Generating %d bytes at %s ...\n", cfg.fileSize, sourcePath)
	if err := generateFile(sourcePath, cfg.fileSize); err != nil {
		return fmt.Errorf("generate file: %w", err)
	}

	fmt.Printf("Uploading file via %s ...\n", cfg.cliPath)
	uploadOutput, err := uploadViaCLI(cfg, sourcePath)
	if err != nil {
		return err
	}

	roots := rootRegexp.FindAllString(uploadOutput, -1)
	if len(roots) == 0 {
		return errors.New("failed to parse merkle roots from upload output")
	}
	fmt.Printf("Upload produced %d root(s): %s\n", len(roots), strings.Join(roots, ", "))

	for i, root := range roots {
		targetPath := filepath.Join(cfg.downloadDir, fmt.Sprintf("fragment-%02d.bin", i))
		fmt.Printf("Downloading root %s to %s ...\n", root, targetPath)
		if err := downloadViaCLI(cfg, root, targetPath); err != nil {
			return err
		}
	}

	fmt.Println("Done. Verify downloaded files as needed.")
	return nil
}

func generateFile(path string, size int64) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, defaultBufferSize)
	var written int64
	var seed byte
	for written < size {
		fillBuffer(buf, &seed)
		chunk := buf
		if remaining := size - written; remaining < int64(len(buf)) {
			chunk = buf[:remaining]
		}
		n, err := file.Write(chunk)
		if err != nil {
			return err
		}
		written += int64(n)
	}
	return file.Sync()
}

func fillBuffer(buf []byte, seed *byte) {
	for i := range buf {
		buf[i] = *seed
		*seed++
	}
}

func uploadViaCLI(cfg config, file string) (string, error) {
	args := []string{
		"upload",
		"--file", file,
		"--url", cfg.url,
		"--key", cfg.privateKey,
		"--tags", cfg.tags,
		"--fragment-size", fmt.Sprintf("%d", cfg.fragmentSize),
		"--task-size", fmt.Sprintf("%d", cfg.taskSize),
		"--skip-tx=false",
	}
	if cfg.timeout != "0" {
		args = append(args, "--timeout", cfg.timeout)
	}
	if cfg.indexer != "" {
		args = append(args, "--indexer", cfg.indexer)
	} else if cfg.nodes != "" {
		args = append(args, "--node", cfg.nodes)
	} else {
		return "", errors.New("either --indexer or --nodes must be provided")
	}

	out, err := runCommand(cfg.cliPath, args...)
	if err != nil {
		return "", fmt.Errorf("upload failed: %w\n%s", err, out)
	}
	return out, nil
}

func downloadViaCLI(cfg config, root, target string) error {
	if cfg.indexer == "" {
		return errors.New("--indexer is required for download")
	}
	args := []string{
		"download",
		"--indexer", cfg.indexer,
		"--root", root,
		"--file", target,
	}
	out, err := runCommand(cfg.cliPath, args...)
	if err != nil {
		return fmt.Errorf("download failed: %w\n%s", err, out)
	}
	_ = out
	return nil
}

func runCommand(bin string, args ...string) (string, error) {
	cmd := exec.Command(bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String() + stderr.String(), err
	}
	return stdout.String(), nil
}


