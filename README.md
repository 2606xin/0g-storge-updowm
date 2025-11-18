# 4GB 文件切分上传下载示例

这个示例程序演示了如何使用 0g-storage-client 将一个 4GB 的文件切分成 10 个 400MB 的分片，然后上传到 0g-storage 网络，并下载这些分片。工程再example/split_upload_download下

## 功能

1. **文件切分**: 将 4GB 源文件切分成 10 个 400MB 的分片文件
2. **上传分片**: 将每个分片上传到 0g-storage 网络
3. **下载分片**: 从 0g-storage 网络下载所有分片
4. **断点续传**: 失败后可以从未完成的分片继续上传，状态保存在 `split-dir/upload_state.json`
5. **文件合并**: 下载分片后自动将原来文件merage:


下面是程序使用中的截图：
分片上传：

<img width="1437" height="596" alt="image" src="https://github.com/user-attachments/assets/d27f481f-0125-4832-8b08-3d2384eb2ef0" />

链上记录：
<img width="2409" height="540" alt="image" src="https://github.com/user-attachments/assets/8aee062a-9fe0-4903-a24a-240201aa44c8" />
<img width="2409" height="540" alt="image" src="https://github.com/user-attachments/assets/3ab41a88-80bc-4a87-88b9-ec62d28c54fb" />

下载文件：
<img width="1433" height="474" alt="image" src="https://github.com/user-attachments/assets/57eb04a5-5463-4e06-9f6f-bbd057a7da8d" />
合并文件：
<img width="1437" height="596" alt="image" src="https://github.com/user-attachments/assets/4d4b2927-fe6a-47e5-8e30-646b47518529" />


## 使用方法

### 编译程序

```bash
cd example/split_upload_download
go build -o split_upload_download.exe main.go
```

### 运行程序

#### 使用索引器（推荐）

```bash
./split_upload_download \
  --file /path/to/4gb-file.bin \
  --url <blockchain_rpc_url> \
  --key <private_key> \
  --indexer <indexer_url> \
  --split-dir ./chunks \
  --download-dir ./downloads
```

#### 使用直接节点列表

```bash
./split_upload_download \
  --file /path/to/4gb-file.bin \
  --url <blockchain_rpc_url> \
  --key <private_key> \
  --nodes <node1_url>,<node2_url>,<node3_url> \
  --split-dir ./chunks \
  --download-dir ./downloads
```

## 参数说明

### 必需参数

- `--file`: 要上传的 4GB 源文件路径
- `--url`: 区块链节点的 RPC URL
- `--key`: 用于签名交易的私钥
- `--indexer` 或 `--nodes`: 索引器端点或存储节点 URL 列表（二选一）

### 可选参数

- `--split-dir`: 存储分片文件的目录（默认: `./chunks`）
- `--download-dir`: 下载文件的目录（默认: `./downloads`）
- `--expected-replica`: 预期复制因子（默认: 1）
- `--task-size`: 每次上传 RPC 的段数（默认: 10）
- `--method`: 节点选择方法（默认: `min`）
- `--full-trusted`: 仅使用受信任的节点（默认: true）
- `--download-proof`: 下载时使用 Merkle 证明验证（默认: false）
- `--timeout`: 端到端超时时间（例如: `30m`, `2h`）
- `--routines`: 上传/下载工作线程的协程数（默认: CPU 核心数）
- `--n-retries`: 提交交易时 gas 价格调整的重试次数（默认: 5）
- `--gas-step`: gas 乘数步长（默认: 15，表示 1.5x）
- `--chunk-retries`: 单个分片的最大上传尝试次数（默认: 3，至少为 1）
- `--chunk-retry-delay`: 分片上传失败后等待多久再重试（默认: 5s）
- `--fallback-untrusted`: 当 `--full-trusted=true` 且分片失败时，是否允许自动退回到非 trusted 节点集合（默认: true）

## 示例

```bash
# 使用索引器上传和下载
./split_upload_download \
  --file ./large-file.bin \
  --url https://rpc.example.com \
  --key 0x1234567890abcdef... \
  --indexer https://indexer.example.com \
  --split-dir ./chunks \
  --download-dir ./downloads \
  --expected-replica 2 \
  --routines 8



## 输出

程序会：

1. 在 `--split-dir` 目录中创建 10 个分片文件（格式: `原文件名.part00`, `原文件名.part01`, ...）
2. 上传每个分片到 0g-storage 网络
3. 在 `--download-dir` 目录中下载所有分片（格式: `chunk_00.bin`, `chunk_01.bin`, ...）

每个分片上传后会显示：

- 交易哈希 (tx)
- Merkle 根 (root)

## 注意事项

- 源文件大小应该接近 4GB（4,294,967,296 字节），但程序会处理实际大小
- 确保有足够的磁盘空间存储分片文件和下载文件
- 上传和下载过程可能需要较长时间，取决于网络状况
- 如果使用 `--download-proof`，下载速度可能会稍慢，但会验证数据完整性
- 如果索引器返回的节点质量不稳定，可以：
  - 在命令行中显式使用 `--nodes url1,url2,...` 指定经过挑选的节点
  - 增加 `--chunk-retries` / `--chunk-retry-delay`，并保持 `--fallback-untrusted=true`，程序会在失败时自动拉取新的节点、跳过出现错误的节点 URL
  - 调低 `--task-size` 或 `--routines` 减少并发，避免节点端超时
- 程序会在 `split-dir` 下生成 `upload_state.json` 保存每个分片的上传结果，后续再次运行时会跳过已经成功的分片；如需重新上传，可删除该文件或对应条目
