package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	mergeInputDir    string
	mergeOutputFile  string
	mergePattern     string
	mergeDeleteParts bool

	mergeCmd = &cobra.Command{
		Use:   "merge",
		Short: "Merge split file parts into a single file",
		Long: `Merge multiple split file parts (e.g., file.part00, file.part01, ...) into a single complete file.
		
The command will automatically sort the parts by their numeric suffix and merge them in order.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return executeMerge()
		},
	}
)

func init() {
	mergeCmd.Flags().StringVarP(&mergeInputDir, "input-dir", "i", "", "Directory containing the split file parts (required)")
	mergeCmd.Flags().StringVarP(&mergeOutputFile, "output", "o", "", "Output file path for the merged file (required)")
	mergeCmd.Flags().StringVarP(&mergePattern, "pattern", "p", "*.part[0-9][0-9]", "File pattern to match split parts (default: *.part[0-9][0-9])")
	mergeCmd.Flags().BoolVarP(&mergeDeleteParts, "delete-parts", "d", false, "Delete part files after successful merge")

	mergeCmd.MarkFlagRequired("input-dir")
	mergeCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(mergeCmd)
}

type partFile struct {
	path  string
	index int
	name  string
}

func executeMerge() error {
	// 验证输入目录
	if _, err := os.Stat(mergeInputDir); os.IsNotExist(err) {
		return fmt.Errorf("输入目录不存在: %s", mergeInputDir)
	}

	// 查找所有分片文件
	parts, err := findPartFiles(mergeInputDir, mergePattern)
	if err != nil {
		return fmt.Errorf("查找分片文件失败: %w", err)
	}

	if len(parts) == 0 {
		return fmt.Errorf("在目录 %s 中未找到匹配模式 %s 的分片文件", mergeInputDir, mergePattern)
	}

	logrus.Infof("找到 %d 个分片文件", len(parts))

	// 按索引排序
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].index < parts[j].index
	})

	// 显示将要合并的文件
	logrus.Info("将按以下顺序合并文件:")
	for _, part := range parts {
		info, _ := os.Stat(part.path)
		sizeStr := "unknown"
		if info != nil {
			sizeStr = formatBytes(info.Size())
		}
		logrus.Infof("  [%02d] %s (%s)", part.index, part.name, sizeStr)
	}

	// 创建输出文件
	outputDir := filepath.Dir(mergeOutputFile)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			return fmt.Errorf("创建输出目录失败: %w", err)
		}
	}

	outFile, err := os.Create(mergeOutputFile)
	if err != nil {
		return fmt.Errorf("创建输出文件失败: %w", err)
	}
	defer outFile.Close()

	// 合并所有分片
	logrus.Infof("开始合并到 %s ...", mergeOutputFile)
	var totalBytes int64
	for idx, part := range parts {
		logrus.Infof("正在合并分片 %d/%d: %s", idx+1, len(parts), part.name)

		inFile, err := os.Open(part.path)
		if err != nil {
			return fmt.Errorf("打开分片文件 %s 失败: %w", part.path, err)
		}

		written, err := io.Copy(outFile, inFile)
		inFile.Close()

		if err != nil {
			return fmt.Errorf("复制分片 %s 失败: %w", part.path, err)
		}

		totalBytes += written
		logrus.Infof("已合并 %s (%s)", part.name, formatBytes(written))
	}

	if err := outFile.Sync(); err != nil {
		return fmt.Errorf("同步输出文件失败: %w", err)
	}

	logrus.Infof("合并成功！总大小: %s", formatBytes(totalBytes))
	logrus.Infof("输出文件: %s", mergeOutputFile)

	// 如果指定，删除分片文件
	if mergeDeleteParts {
		logrus.Info("正在删除分片文件...")
		for _, part := range parts {
			if err := os.Remove(part.path); err != nil {
				logrus.Warnf("删除分片文件 %s 失败: %v", part.path, err)
			} else {
				logrus.Infof("已删除: %s", part.name)
			}
		}
		logrus.Info("分片文件删除完成")
	}

	return nil
}

func findPartFiles(dir, pattern string) ([]partFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	parts := make([]partFile, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// 匹配 .partXX 格式的文件
		if strings.Contains(name, ".part") {
			// 提取分片编号
			idx := -1
			if _, err := fmt.Sscanf(name, "%*s.part%d", &idx); err == nil && idx >= 0 {
				parts = append(parts, partFile{
					path:  filepath.Join(dir, name),
					index: idx,
					name:  name,
				})
			}
		}
	}

	return parts, nil
}

func formatBytes(bytes int64) string {
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
