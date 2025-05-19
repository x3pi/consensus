package main

import (
	"context"
	"fmt" // Cần thiết cho fmt.Printf và fmt.Errorf
	"log"
	"os" // Sử dụng os.ReadFile thay cho ioutil.ReadFile
	"os/signal"
	"syscall"

	// !!! QUAN TRỌNG: SỬA ĐƯỜNG DẪN IMPORT NÀY !!!
	// Thay thế "[TÊN_MODULE_CỦA_BẠN]/consensusnode" bằng đường dẫn import đúng của bạn.
	// Tên module thường được định nghĩa trong tệp go.mod ở thư mục gốc của dự án.
	// Ví dụ: nếu module của bạn là "example.com/myproject", và package consensusnode nằm trong thư mục "consensusnode"
	// ở gốc dự án, thì đường dẫn sẽ là "example.com/myproject/consensusnode".
	// Nếu "consensusnode" là một thư mục con trực tiếp trong module của bạn (ví dụ module tên là "mychain")
	// thì đường dẫn có thể là "mychain/consensusnode".

	"github.com/blockchain/consensus/consensusnode"
	"gopkg.in/yaml.v3" // Thêm package để xử lý YAML
)

const defaultConfigFile = "node_config.yaml" // Tên file cấu hình mặc định

// loadConfigFromFile tải cấu hình từ một tệp YAML.
func loadConfigFromFile(path string, cfg *consensusnode.NodeConfig) error {
	log.Printf("Đang tải cấu hình từ tệp: %s", path)
	// Sử dụng os.ReadFile thay cho ioutil.ReadFile
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("không thể đọc tệp cấu hình YAML '%s': %w", path, err)
	}
	err = yaml.Unmarshal(yamlFile, cfg)
	if err != nil {
		return fmt.Errorf("không thể giải mã (unmarshal) tệp cấu hình YAML '%s': %w", path, err)
	}
	log.Printf("Đã tải cấu hình từ tệp '%s' thành công.", path)
	return nil
}

func main() {
	// Tạo context chính cho ứng dụng, có thể bị hủy
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Đảm bảo context được hủy khi main thoát

	// Lấy đường dẫn tệp cấu hình từ argument dòng lệnh hoặc dùng mặc định
	var configPath string
	if len(os.Args) < 2 {
		log.Printf("Không có tệp cấu hình nào được chỉ định, sử dụng tệp mặc định: %s\n", defaultConfigFile)
		configPath = defaultConfigFile
	} else {
		configPath = os.Args[1]
		log.Printf("Sử dụng tệp cấu hình được chỉ định: %s\n", configPath)
	}

	// Khởi tạo một struct NodeConfig rỗng để điền dữ liệu từ tệp.
	var cfg consensusnode.NodeConfig

	// Tải cấu hình từ tệp YAML.
	if err := loadConfigFromFile(configPath, &cfg); err != nil {
		// Nếu lỗi xảy ra khi tải tệp cấu hình (dù là mặc định hay được chỉ định),
		// chương trình sẽ thoát.
		log.Fatalf("Không thể tải cấu hình từ tệp '%s': %v. Chương trình không thể tiếp tục.", configPath, err)
	}

	// Log thông tin cấu hình đã tải
	// Sử dụng %+v để hiển thị cả tên trường và giá trị của struct
	log.Printf("Cấu hình đã tải: %+v", cfg)

	// (Tùy chọn) Kiểm tra các trường cấu hình quan trọng sau khi tải
	// Ví dụ:
	if cfg.ListenAddress == "" {
		log.Fatalf("Lỗi cấu hình: 'listenAddress' không được để trống trong tệp %s", configPath)
	}
	if cfg.NodeType == "" {
		log.Fatalf("Lỗi cấu hình: 'nodeType' không được để trống trong tệp %s", configPath)
	}
	// Thêm các kiểm tra khác nếu cần cho các trường bắt buộc

	// Tạo một ManagedNode mới với cấu hình đã tải
	// Đảm bảo rằng bạn đã sửa đúng đường dẫn import ở trên để consensusnode.NewManagedNode và consensusnode.NodeConfig được nhận diện.
	node, err := consensusnode.NewManagedNode(ctx, cfg)
	if err != nil {
		log.Fatalf("Không thể tạo ManagedNode: %v", err)
	}

	// Khởi động ManagedNode (bắt đầu các dịch vụ mạng, kết nối peer, v.v.)
	if err := node.Start(); err != nil {
		log.Fatalf("Không thể khởi động ManagedNode: %v", err)
	}

	// Thiết lập channel để lắng nghe tín hiệu OS (Ctrl+C, kill)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Chờ tín hiệu
	recvdSignal := <-sigCh
	log.Printf("Đã nhận tín hiệu: %s. Đang tắt node...", recvdSignal)

	// Dừng ManagedNode một cách an toàn
	if err := node.Stop(); err != nil {
		log.Fatalf("Lỗi khi dừng ManagedNode: %v", err)
	}

	log.Println("ManagedNode đã tắt thành công.")
}
