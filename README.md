# Hướng dẫn Chạy Hệ thống Đồng thuận 4 Node

Tài liệu này hướng dẫn cách thiết lập và chạy một mạng lưới đồng thuận bao gồm **4 node cục bộ**.

---

## Yêu cầu Chung

- Cài đặt **Go** (phiên bản tương thích với các thư viện `libp2p` được sử dụng)
- Mã nguồn dự án đã được **biên dịch thành công**
- **4 tệp cấu hình YAML** riêng biệt cho mỗi node

---

## Chuẩn bị

### 1. Biên dịch Ứng dụng

Đảm bảo bạn đã biên dịch ứng dụng `consensus_app` từ mã nguồn trong thư mục `consensus/cmd/consensus_app/`.

**Lệnh ví dụ (thực hiện trong thư mục `consensus/cmd/consensus_app/`):**

```bash
go build -o consensus_app
```

Sau khi biên dịch, bạn sẽ có tệp thực thi là: `consensus_app`

---

### 2. Chuẩn bị Tệp Cấu hình

Cần có 4 tệp YAML cấu hình riêng biệt:

- `node_config.yaml`
- `node_config_1.yaml`
- `node_config_2.yaml`
- `node_config_3.yaml`

#### Các trường quan trọng cần kiểm tra:

- **`listenAddress`**: mỗi node cần địa chỉ/port lắng nghe riêng.
  - Ví dụ:
    - Node 1: `/ip4/0.0.0.0/udp/8001/quic-v1`
    - Node 2: `/ip4/0.0.0.0/udp/8002/quic-v1`
    - Node 3: `/ip4/0.0.0.0/udp/8003/quic-v1`
    - Node 4: `/ip4/0.0.0.0/udp/8004/quic-v1`

- **`privateKey`**: mỗi node phải có khóa riêng tư **duy nhất**.

- **`rootPath`**: mỗi node dùng thư mục khác nhau, ví dụ: `./node_data_1`, `./node_data_2`, v.v.

- **`bootstrapPeers`**: chỉ định các node khác để kết nối ban đầu.

  - Để lấy `PeerID`, hãy khởi động node một lần, log sẽ hiển thị:
  
    ```
    Node ID: 12D3KooW...
    ```

  - Cập nhật PeerID này trong `bootstrapPeers` của các node khác.

  - Ví dụ (Node 1):

    ```yaml
    bootstrapPeers:
      - "/ip4/127.0.0.1/udp/8002/quic-v1/p2p/<PeerID_Node2>"
      - "/ip4/127.0.0.1/udp/8003/quic-v1/p2p/<PeerID_Node3>"
    ```

- **`allStakers`**: danh sách staker giống nhau trên tất cả node. Mỗi mục gồm `pubKeyHex` và `stake`.

  - Lưu ý: `pubKeyHex` được in ra khi khởi động node từ `privateKey`.

- **`masterNodeAddress`**: nếu có node master riêng, chỉ định tại đây. Nếu không, có thể để trống hoặc trỏ đến một node consensus bất kỳ.

---

## Khởi chạy các Node

Sau khi chuẩn bị cấu hình, mở **4 cửa sổ terminal** (hoặc dùng `tmux`, `screen`).

### Các lệnh khởi chạy:

- Terminal 1:
  ```bash
  ./consensus_app node_config.yaml
  ```

- Terminal 2:
  ```bash
  ./consensus_app node_config_1.yaml
  ```

- Terminal 3:
  ```bash
  ./consensus_app node_config_2.yaml
  ```

- Terminal 4:
  ```bash
  ./consensus_app node_config_3.yaml
  ```

---

## Theo dõi Hoạt động

Quan sát log mỗi node để theo dõi:

- ID node và địa chỉ lắng nghe
- Trạng thái kết nối với peers
- Sự kiện tạo và nhận
- Trạng thái quyết định ClothoStatus
- `lastDecidedFrame`
- Thông tin từ `PrintDagStoreStatus`
- Lỗi hoặc cảnh báo

---

## Khắc phục Sự cố Thường Gặp

### Node không kết nối được:

- Kiểm tra `listenAddress` và `bootstrapPeers` (đúng PeerID chưa)
- Kiểm tra tường lửa và port
- Đảm bảo các node bootstrap đang chạy

### Đồng thuận bị đình trệ:

- Đảm bảo ít nhất **3/4 node** hoạt động tốt (quorum >2/3)
- Kiểm tra log lỗi trong `DecideClotho`

### Lỗi phổ biến:

- **`address already in use`**: port bị trùng
- **`privateKey` hoặc `pubKeyHex`** sai: cần unique và đúng khớp giữa `privateKey` và `allStakers`

---

## Tổng Kết

Bằng cách làm theo các bước trên, bạn có thể thiết lập và chạy một mạng đồng thuận 4 node cục bộ để phát triển và thử nghiệm.
