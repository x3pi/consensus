package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time" // Thêm package time

	// Đảm bảo đường dẫn import này là chính xác cho dự án của bạn
	"github.com/blockchain/consensus/consensusnode" // Giả sử đây là đường dẫn module của bạn
	"github.com/libp2p/go-libp2p/core/peer"         // Thêm import này
	"gopkg.in/yaml.v3"
)

const defaultConfigFile = "node_config.yaml" // Tên file cấu hình mặc định

// loadConfigFromFile tải cấu hình từ một tệp YAML.
func loadConfigFromFile(path string, cfg *consensusnode.NodeConfig) error {
	log.Printf("Đang tải cấu hình từ tệp: %s", path)
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
	log.Printf("Đã tải cấu hình từ tệp '%s' thành công.", path)
	return nil
}

// sendTransactionRequest định kỳ gửi yêu cầu TransactionsRequestProtocol đến master node.
// Hàm này sẽ chạy cho đến khi `appCtx` bị hủy.
func sendTransactionRequestPeriodically(appCtx context.Context, node *consensusnode.ManagedNode) {
	// Đợi một chút để node có thời gian kết nối hoặc ổn định (tùy chọn)
	// trước khi bắt đầu vòng lặp gửi request.
	select {
	case <-time.After(10 * time.Second): // Đợi 10 giây
		log.Println("CLIENT: Bắt đầu gửi yêu cầu TransactionsRequestProtocol định kỳ...")
	case <-appCtx.Done():
		log.Println("CLIENT: Context ứng dụng đã hủy trước khi bắt đầu gửi yêu cầu định kỳ.")
		return
	}

	// Tạo một ticker để kích hoạt việc gửi request mỗi 30 giây
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop() // Đảm bảo ticker được dừng khi hàm kết thúc

	for {
		select {
		case <-appCtx.Done(): // Nếu context của ứng dụng bị hủy (ví dụ: node đang dừng)
			log.Println("CLIENT: Dừng gửi yêu cầu định kỳ do context ứng dụng đã hủy.")
			return
		case t := <-ticker.C: // Khi ticker kích hoạt
			log.Printf("CLIENT: Ticker kích hoạt lúc %v, chuẩn bị gửi yêu cầu...", t)

			// Chuẩn bị dữ liệu bạn muốn gửi.
			// Bạn có thể thay đổi payload mỗi lần gửi nếu cần.
			requestPayload := []byte(fmt.Sprintf("{\"action\": \"get_pending_transactions\", \"timestamp\": %d, \"request_id\": \"client_periodic_%d\"}", time.Now().Unix(), time.Now().Nanosecond()))

			// Lấy context cho request này, có thể với timeout ngắn hơn.
			// Sử dụng appCtx làm parent để request cũng bị hủy nếu node dừng.
			reqCtx, cancelReq := context.WithTimeout(appCtx, 20*time.Second) // Timeout 20 giây cho mỗi request

			log.Printf("CLIENT: Đang gửi TransactionsRequestProtocol tới Master Node (Payload: %s)...", string(requestPayload))
			responseData, err := node.SendRequestToMasterNode(reqCtx, consensusnode.TransactionsRequestProtocol, requestPayload)
			cancelReq() // Hủy context của request sau khi hoàn tất hoặc lỗi

			if err != nil {
				log.Printf("CLIENT: Lỗi khi gửi TransactionsRequestProtocol đến Master Node: %v", err)
				// Tiếp tục vòng lặp để thử lại ở lần ticker tiếp theo
				continue
			}

			log.Printf("CLIENT: Đã nhận phản hồi từ Master Node: %s", string(responseData))
			// Xử lý responseData nếu cần
		}
	}
}

func main() {
	// Sử dụng node.Context() cho sendTransactionRequestPeriodically
	// để goroutine có thể dừng khi node dừng.
	nodeCtx, nodeCancelFunc := context.WithCancel(context.Background())
	defer nodeCancelFunc()

	var configPath string
	if len(os.Args) < 2 {
		log.Printf("Không có tệp cấu hình nào được chỉ định, sử dụng tệp mặc định: %s", defaultConfigFile)
		configPath = defaultConfigFile
	} else {
		configPath = os.Args[1]
		log.Printf("Sử dụng tệp cấu hình được chỉ định: %s", configPath)
	}

	cfg := consensusnode.DefaultNodeConfig()
	if err := loadConfigFromFile(configPath, &cfg); err != nil {
		log.Fatalf("Không thể tải cấu hình từ tệp '%s': %v. Chương trình không thể tiếp tục.", configPath, err)
	}

	log.Printf("Cấu hình đã tải: Node Type = %s, MasterNodeAddress = %s", cfg.NodeType, cfg.MasterNodeAddress)

	if cfg.ListenAddress == "" {
		log.Fatalf("Lỗi cấu hình: 'listenAddress' không được để trống.")
	}
	if cfg.NodeType == "" {
		log.Fatalf("Lỗi cấu hình: 'nodeType' không được để trống.")
	}

	// Truyền nodeCtx vào NewManagedNode
	node, err := consensusnode.NewManagedNode(nodeCtx, cfg)
	if err != nil {
		log.Fatalf("Không thể tạo ManagedNode: %v", err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("Không thể khởi động ManagedNode: %v", err)
	}

	isMasterItself := false
	if cfg.MasterNodeAddress != "" {
		// Phân tích AddrInfoFromString cần peer.ID, nên cần import "github.com/libp2p/go-libp2p/core/peer"
		masterAddrInfo, err := peer.AddrInfoFromString(cfg.MasterNodeAddress)
		if err != nil {
			log.Printf("Cảnh báo: Không thể phân tích MasterNodeAddress '%s': %v", cfg.MasterNodeAddress, err)
		} else if masterAddrInfo != nil && node.Host().ID() == masterAddrInfo.ID {
			isMasterItself = true
		}
	}

	if cfg.MasterNodeAddress != "" && !isMasterItself {
		log.Printf("Node này (ID: %s, Type: %s) sẽ hoạt động như một client và gửi yêu cầu định kỳ đến Master Node tại %s.", node.Host().ID(), cfg.NodeType, cfg.MasterNodeAddress)
		// Chạy việc gửi request trong một goroutine để không block hàm main
		// Truyền node.Context() (chính là nodeCtx đã tạo ở trên) vào hàm
		go sendTransactionRequestPeriodically(node.Context(), node)
	} else if cfg.MasterNodeAddress == "" {
		log.Printf("Node này (ID: %s, Type: %s) không có MasterNodeAddress được cấu hình, sẽ không gửi yêu cầu TransactionsRequestProtocol tự động.", node.Host().ID(), cfg.NodeType)
	} else if isMasterItself {
		log.Printf("Node này (ID: %s, Type: %s) là Master Node được cấu hình, sẽ không gửi yêu cầu TransactionsRequestProtocol cho chính nó.", node.Host().ID(), cfg.NodeType)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Chờ tín hiệu dừng hoặc context của node bị hủy
	select {
	case recvdSignal := <-sigCh:
		log.Printf("Đã nhận tín hiệu OS: %s. Đang tắt node...", recvdSignal)
	case <-node.Context().Done(): // Nếu context của node bị hủy từ bên trong (ví dụ: lỗi nghiêm trọng)
		log.Printf("Context của node đã hủy. Đang tắt node...")
	}

	// Gọi nodeCancelFunc() để đảm bảo tất cả các goroutine sử dụng nodeCtx (bao gồm cả sendTransactionRequestPeriodically)
	// nhận được tín hiệu dừng. Điều này cũng sẽ được gọi bởi defer ở đầu hàm main.
	log.Println("Gửi tín hiệu hủy context cho các thành phần của node...")
	nodeCancelFunc() // Gửi tín hiệu hủy

	// Đợi một chút để các goroutine có thời gian dọn dẹp (tùy chọn)
	// time.Sleep(1 * time.Second) // Ví dụ

	if err := node.Stop(); err != nil { // node.Stop() cũng sử dụng context của node để dừng các thành phần bên trong
		log.Fatalf("Lỗi khi dừng ManagedNode: %v", err)
	}

	log.Println("ManagedNode đã tắt thành công.")
}
