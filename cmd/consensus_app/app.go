package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/blockchain/consensus/consensusnode"
	"github.com/blockchain/consensus/logger" // Sử dụng logger tùy chỉnh của bạn
	"gopkg.in/yaml.v3"
)

const defaultConfigPath = "node_config.yaml" // Đổi tên hằng số để rõ ràng hơn

// loadConfigFromFile tải cấu hình từ một tệp YAML.
func loadConfigFromFile(path string, cfg *consensusnode.NodeConfig) error {
	logger.Info("Đang tải cấu hình từ tệp:", path)
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("không thể đọc tệp cấu hình YAML '%s': %w", path, err)
	}

	tempCfg := consensusnode.DefaultNodeConfig() // Lấy giá trị mặc định

	err = yaml.Unmarshal(yamlFile, &tempCfg) // Unmarshal vào cấu hình tạm thời đã có giá trị mặc định
	if err != nil {
		return fmt.Errorf("không thể giải mã (unmarshal) tệp cấu hình YAML '%s': %w", path, err)
	}

	*cfg = tempCfg // Gán cấu hình đã được merge (mặc định + YAML) vào cfg chính
	logger.Info("Đã tải cấu hình từ tệp '%s' thành công.", path)
	return nil
}

func main() {
	// Sử dụng nodeCtx để quản lý vòng đời của node và các goroutine liên quan.
	nodeCtx, nodeCancelFunc := context.WithCancel(context.Background())
	defer nodeCancelFunc()

	var configPath string
	if len(os.Args) < 2 {
		logger.Info("Không có tệp cấu hình nào được chỉ định, sử dụng tệp mặc định:", defaultConfigPath)
		configPath = defaultConfigPath
	} else {
		configPath = os.Args[1]
		logger.Info("Sử dụng tệp cấu hình được chỉ định:", configPath)
	}

	cfg := consensusnode.DefaultNodeConfig() //
	if err := loadConfigFromFile(configPath, &cfg); err != nil {
		logger.Error("Không thể tải cấu hình từ tệp '%s': %v. Chương trình không thể tiếp tục.", configPath, err)
		os.Exit(1) // Thoát với mã lỗi
	}

	logger.Info(fmt.Sprintf("Cấu hình đã tải: Node Type = %s, MasterNodeAddress = %s", cfg.NodeType, cfg.MasterNodeAddress))

	if cfg.ListenAddress == "" {
		logger.Error("Lỗi cấu hình: 'listenAddress' không được để trống.")
		os.Exit(1)
	}
	if cfg.NodeType == "" {
		logger.Error("Lỗi cấu hình: 'nodeType' không được để trống.")
		os.Exit(1)
	}

	node, err := consensusnode.NewManagedNode(nodeCtx, cfg) // Truyền nodeCtx vào NewManagedNode
	if err != nil {
		logger.Error("Không thể tạo ManagedNode:", err)
		os.Exit(1)
	}

	if err := node.Start(); err != nil { //
		logger.Error("Không thể khởi động ManagedNode:", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Chờ tín hiệu dừng hoặc context của node bị hủy
	select {
	case recvdSignal := <-sigCh:
		logger.Info("Đã nhận tín hiệu OS:", recvdSignal, ". Đang tắt node...")
	case <-node.Context().Done(): // Nếu context của node bị hủy từ bên trong (ví dụ: lỗi nghiêm trọng)
		logger.Info("Context của node đã hủy. Đang tắt node...")
	}

	// Gọi nodeCancelFunc() để đảm bảo tất cả các goroutine sử dụng nodeCtx
	// nhận được tín hiệu dừng. Điều này cũng sẽ được gọi bởi defer ở đầu hàm main.
	logger.Info("Gửi tín hiệu hủy context cho các thành phần của node...")
	nodeCancelFunc() // Gửi tín hiệu hủy

	if err := node.Stop(); err != nil { // node.Stop() cũng sử dụng context của node để dừng các thành phần bên trong
		logger.Error("Lỗi khi dừng ManagedNode:", err)
		os.Exit(1)
	}

	logger.Info("ManagedNode đã tắt thành công.")
}
