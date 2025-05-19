package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	// Thêm package time để sử dụng cho Duration parsing
	// !!! QUAN TRỌNG: SỬA ĐƯỜNG DẪN IMPORT NÀY !!!
	// Thay thế "[TÊN_MODULE_CỦA_BẠN]/consensusnode" bằng đường dẫn import đúng của bạn.
	// Tên module thường được định nghĩa trong tệp go.mod ở thư mục gốc của dự án.
	// Ví dụ: nếu module của bạn là "example.com/myproject", và package consensusnode nằm trong thư mục "consensusnode"
	// ở gốc dự án, thì đường dẫn sẽ là "example.com/myproject/consensusnode".
	// Nếu "consensusnode" là một thư mục con trực tiếp trong module của bạn (ví dụ module tên là "mychain")
	// thì đường dẫn có thể là "mychain/consensusnode".
	// Dựa trên cấu trúc file bạn cung cấp, nếu "consensus" là thư mục gốc của module,
	// và module tên là "consensus" (trong go.mod), thì đường dẫn sẽ là:
	// "consensus/consensusnode"
	// Tuy nhiên, tên module thực tế trong go.mod mới là quan trọng.
	// Giả sử tên module của bạn là "github.com/yourusername/yourproject"

	"github.com/blockchain/consensus/consensusnode"
	"gopkg.in/yaml.v3"
)

const defaultConfigFile = "node_config.yaml" // Tên file cấu hình mặc định

// loadConfigFromFile tải cấu hình từ một tệp YAML.
// Hàm này cần được cập nhật để xử lý các trường time.Duration một cách chính xác nếu yaml.v3 không tự động làm điều đó.
// Tuy nhiên, yaml.v3 thường xử lý tốt các chuỗi như "20s", "1m".
func loadConfigFromFile(path string, cfg *consensusnode.NodeConfig) error {
	log.Printf("Đang tải cấu hình từ tệp: %s", path)
	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("không thể đọc tệp cấu hình YAML '%s': %w", path, err)
	}

	// Trước khi unmarshal, có thể thiết lập giá trị mặc định từ DefaultNodeConfig
	// để các trường không có trong YAML vẫn có giá trị.
	// *cfg = consensusnode.DefaultNodeConfig() // Gán giá trị mặc định trước

	// Tuy nhiên, cách tiếp cận tốt hơn là để unmarshal ghi đè lên các giá trị mặc định
	// nếu chúng tồn tại trong YAML, và giữ lại mặc định nếu không.
	// Để làm điều đó, chúng ta sẽ unmarshal vào một config tạm thời rồi merge,
	// hoặc đảm bảo DefaultNodeConfig được gọi trước và unmarshal ghi đè lên nó.
	// Với yaml.v3, việc unmarshal trực tiếp vào cfg thường hoạt động tốt,
	// các trường không có trong YAML sẽ giữ nguyên giá trị zero của chúng (cần DefaultNodeConfig xử lý sau).

	// Để đảm bảo các giá trị mặc định được áp dụng cho các trường không có trong YAML,
	// chúng ta sẽ gán DefaultNodeConfig trước, sau đó Unmarshal sẽ ghi đè các giá trị có trong YAML.
	tempCfg := consensusnode.DefaultNodeConfig() // Lấy giá trị mặc định

	err = yaml.Unmarshal(yamlFile, &tempCfg) // Unmarshal vào cấu hình tạm thời đã có giá trị mặc định
	if err != nil {
		return fmt.Errorf("không thể giải mã (unmarshal) tệp cấu hình YAML '%s': %w", path, err)
	}

	// Gán cấu hình đã được merge (mặc định + YAML) vào cfg chính
	*cfg = tempCfg

	// Xử lý thủ công các trường time.Duration nếu cần thiết (thường yaml.v3 xử lý tốt)
	// Ví dụ, nếu ConnectionGracePeriod trong YAML là một chuỗi "20s", yaml.v3 sẽ cố gắng parse nó.
	// Nếu không, bạn cần đọc nó như một chuỗi rồi dùng time.ParseDuration.
	// Dựa trên log lỗi trước đó, có vẻ yaml.v3 không tự động parse time.Duration khi các trường bị bỏ trống
	// và NodeConfig chưa được khởi tạo với DefaultNodeConfig trước khi unmarshal.
	// Với việc gán DefaultNodeConfig() trước, vấn đề này nên được giải quyết.

	log.Printf("Đã tải cấu hình từ tệp '%s' thành công.", path)
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var configPath string
	if len(os.Args) < 2 {
		log.Printf("Không có tệp cấu hình nào được chỉ định, sử dụng tệp mặc định: %s", defaultConfigFile)
		configPath = defaultConfigFile
	} else {
		configPath = os.Args[1]
		log.Printf("Sử dụng tệp cấu hình được chỉ định: %s", configPath)
	}

	// Khởi tạo cfg với các giá trị mặc định trước khi tải từ file.
	// Điều này quan trọng để các trường không có trong YAML vẫn có giá trị hợp lý.
	cfg := consensusnode.DefaultNodeConfig() // Lấy cấu hình mặc định

	// Tải cấu hình từ tệp YAML, ghi đè lên các giá trị mặc định nếu có.
	if err := loadConfigFromFile(configPath, &cfg); err != nil {
		log.Fatalf("Không thể tải cấu hình từ tệp '%s': %v. Chương trình không thể tiếp tục.", configPath, err)
	}

	log.Printf("Cấu hình đã tải (sau khi merge với mặc định và YAML): %+v", cfg)

	// Kiểm tra các trường cấu hình quan trọng sau khi tải và merge
	if cfg.ListenAddress == "" {
		log.Fatalf("Lỗi cấu hình: 'listenAddress' không được để trống trong tệp %s hoặc giá trị mặc định.", configPath)
	}
	if cfg.NodeType == "" {
		log.Fatalf("Lỗi cấu hình: 'nodeType' không được để trống trong tệp %s hoặc giá trị mặc định.", configPath)
	}
	// Thêm các kiểm tra khác nếu cần, ví dụ:
	if cfg.MasterNodeAddress == "" && cfg.NodeType != "master" { // Ví dụ: node không phải master thì nên có địa chỉ master
		log.Printf("CẢNH BÁO: MasterNodeAddress chưa được cấu hình cho node type '%s'. Node có thể không hoạt động đúng nếu cần kết nối tới Master Node.", cfg.NodeType)
	}

	node, err := consensusnode.NewManagedNode(ctx, cfg)
	if err != nil {
		log.Fatalf("Không thể tạo ManagedNode: %v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("Không thể khởi động ManagedNode: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	recvdSignal := <-sigCh
	log.Printf("Đã nhận tín hiệu: %s. Đang tắt node...", recvdSignal)

	if err := node.Stop(); err != nil {
		log.Fatalf("Lỗi khi dừng ManagedNode: %v", err)
	}

	log.Println("ManagedNode đã tắt thành công.")
}
