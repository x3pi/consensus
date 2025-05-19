package consensusnode

import (
	"errors"
	// "fmt" // Uncomment if you need to print for debugging within this file
	"math"
	"math/rand"
	"time"
	// "log" // Uncomment if you need to use log for debugging within this file
)

// NodeID định nghĩa kiểu cho định danh node.
// Trong ngữ cảnh của Lachesis, đây là chuỗi hex của public key của node (creator).
type NodeID string

// SelectReferenceNode triển khai Algorithm 2: Node Selection.
// Thuật toán này chọn một node tham chiếu từ một tập hợp các node ứng cử viên.
//
// Inputs:
//   - heights: Một map đại diện cho Height Vector (H). Key là NodeID (chuỗi hex public key),
//     value là chiều cao tương ứng (ví dụ: event.EventData.Index của event mới nhất từ node đó).
//   - inDegrees: Một map đại diện cho In-degree Vector (I). Key là NodeID,
//     value là bậc vào tương ứng (ví dụ: số lượng OtherParents từ các creator khác nhau
//     trong event mới nhất của node đó).
//   - candidateNodes: Một slice các NodeID đại diện cho tập hợp các node ứng cử viên
//     (KHÔNG bao gồm node hiện tại đang thực hiện lựa chọn).
//
// Output:
//   - NodeID của node tham chiếu được chọn.
//   - Một lỗi nếu không thể chọn node nào (ví dụ: candidateNodes rỗng).
func SelectReferenceNode(heights map[NodeID]uint64, inDegrees map[NodeID]uint64, candidateNodes []NodeID) (NodeID, error) {
	if len(candidateNodes) == 0 {
		return "", errors.New("candidateNodes cannot be empty")
	}

	minCost := math.MaxFloat64
	var sref []NodeID // Slice để lưu các node có chi phí thấp nhất

	for _, k := range candidateNodes {
		Hk, hExists := heights[k]
		Ik, iExists := inDegrees[k]

		var currentCost float64
		if !hExists || !iExists || Hk == 0 {
			// Nếu node k không có trong heights hoặc inDegrees, hoặc Hk = 0,
			// chi phí của nó được coi là vô cực.
			currentCost = math.MaxFloat64
			// log.Printf("Node %s: hExists=%t, iExists=%t, Hk=%d. Cost set to Inf.", k, hExists, iExists, Hk)

			// Xử lý trường hợp tất cả các node đều có chi phí vô cực (ví dụ, tất cả Hk=0)
			if minCost == math.MaxFloat64 {
				// Chỉ thêm vào sref nếu Ik tồn tại (nghĩa là node có một số dữ liệu cơ bản)
				// và Hk là 0. Điều này đảm bảo chúng ta không thêm các node hoàn toàn không có dữ liệu.
				if Hk == 0 && iExists {
					// Kiểm tra trùng lặp trước khi thêm
					isAlreadyInSref := false
					for _, existingNode := range sref {
						if existingNode == k {
							isAlreadyInSref = true
							break
						}
					}
					if !isAlreadyInSref {
						sref = append(sref, k)
					}
				}
			}
		} else {
			currentCost = float64(Ik) / float64(Hk)
			// log.Printf("Node %s: Ik=%d, Hk=%d. Cost = %.2f", k, Ik, Hk, currentCost)
		}

		if currentCost < minCost {
			minCost = currentCost
			sref = []NodeID{k} // Đặt lại sref với chỉ node k
			// log.Printf("New minCost: %.2f for node %s. sref reset.", minCost, k)
		} else if currentCost == minCost {
			// Nếu chi phí bằng nhau, thêm node k vào sref
			// Đảm bảo không thêm node có chi phí vô cực trừ khi minCost cũng là vô cực
			if currentCost != math.MaxFloat64 || minCost == math.MaxFloat64 {
				// Kiểm tra để tránh thêm trùng lặp nếu node k đã có trong sref
				isAlreadyInSref := false
				for _, existingNode := range sref {
					if existingNode == k {
						isAlreadyInSref = true
						break
					}
				}
				if !isAlreadyInSref {
					sref = append(sref, k)
					// log.Printf("Node %s added to sref (cost %.2f equals minCost).", k, currentCost)
				}
			}
		}
	}

	if len(sref) == 0 {
		// Điều này có thể xảy ra nếu candidateNodes không rỗng nhưng tất cả các node
		// đều không có dữ liệu hợp lệ trong heights/inDegrees hoặc tất cả Hk=0
		// và không có node nào được thêm vào sref (ví dụ, tất cả Hk=0 và không có Ik).
		// log.Println("sref is empty after processing candidates.")
		return "", errors.New("no suitable reference node found (sref is empty after processing candidates)")
	}

	// Chọn ngẫu nhiên một node từ sref
	// Sử dụng một nguồn ngẫu nhiên riêng để tránh ảnh hưởng đến global rand state nếu cần.
	// Trong môi trường production, bạn có thể muốn một nguồn ngẫu nhiên tốt hơn
	// hoặc truyền vào một rand.Rand đã được khởi tạo.
	source := rand.NewSource(time.Now().UnixNano())
	randomGenerator := rand.New(source)

	randomIndex := randomGenerator.Intn(len(sref))
	selectedRefNodeID := sref[randomIndex]

	// log.Printf("Node selection details: Candidates=%d, MinCost=%.2f, NodesAtMinCost=%d, Selected=%s",
	//	len(candidateNodes), minCost, len(sref), selectedRefNodeID)

	return selectedRefNodeID, nil
}

// GetNodeIDFromString chuyển đổi một chuỗi thành NodeID.
// Hàm này đơn giản, nhưng có thể hữu ích để tường minh khi làm việc với các chuỗi public key.
func GetNodeIDFromString(s string) NodeID {
	return NodeID(s)
}
