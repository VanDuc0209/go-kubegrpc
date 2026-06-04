# Hướng dẫn tích hợp: `go-kubegrpc`

Tài liệu này hướng dẫn cách cài đặt, cấu hình và tích hợp thư viện `go-kubegrpc` vào các ứng dụng Go chạy trên Kubernetes nhằm tự động khám phá dịch vụ và cân bằng tải L7 (phía client) cho gRPC.

---

## 1. Cài đặt

Thêm thư viện vào phụ thuộc Go module của bạn:

```bash
go get github.com/grpc-k8s-balancer/grpc-k8s-balancer
```

Yêu cầu phiên bản Go tối thiểu là từ `1.22.0` trở lên.

---

## 2. Cấu hình Kubernetes RBAC

Vì thư viện tự động theo dõi (`list` và `watch`) tài nguyên `EndpointSlice` của Kubernetes theo thời gian thực để tìm IP các Pod, ServiceAccount chạy ứng dụng của bạn phải có quyền RBAC để đọc tài nguyên `endpointslices`.

Hãy áp dụng Role và RoleBinding sau vào namespace chạy ứng dụng của bạn:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: <app-namespace-cua-ban>
  name: go-kubegrpc-resolver
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  namespace: <app-namespace-cua-ban>
  name: go-kubegrpc-resolver-binding
subjects:
  - kind: ServiceAccount
    name: default # Thay bằng tên ServiceAccount chạy ứng dụng của bạn
    namespace: <app-namespace-cua-ban>
roleRef:
  kind: Role
  name: go-kubegrpc-resolver
  apiGroup: rbac.authorization.k8s.io
```

---

## 3. Định dạng Target URI

Thư viện sẽ phân tích target string để kích hoạt resolver tương ứng:

1. **Kubernetes Target (Khuyên dùng):**
   * Định dạng: `k8s:///<namespace>/<service-name>:<port>`
   * Lược bỏ namespace: `k8s:///<service-name>:<port>` (sẽ tự động dùng `DefaultNamespace` trong `Config`).
   * Cổng (`port`) có thể là **số** (ví dụ: `8080`) hoặc **tên cổng** (ví dụ: `grpc` được cấu hình trong spec của Service).
   * Ví dụ: `k8s:///prod/billing-service:grpc`

2. **DNS Fallback Target (Dùng ngoài cluster hoặc chạy máy local):**
   * Định dạng: `dns:///<host>:<port>`
   * Ví dụ: `dns:///localhost:50051`

---

## 4. Ví dụ tích hợp nhanh (Quick Start)

Dưới đây là một ví dụ tối giản để khởi tạo client và thực hiện gọi RPC:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	grpck8s "github.com/grpc-k8s-balancer/grpc-k8s-balancer"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Khởi tạo cấu hình mặc định và ghi đè các tham số
	cfg := grpck8s.DefaultConfig()
	cfg.DefaultNamespace = "billing"
	cfg.LoadBalancingPolicy = "least_request" // "round_robin" hoặc "least_request"
	cfg.EnableHealthCheck = true

	// Khởi tạo client gRPC
	// Client sẽ tự động chạy ngầm trình resolver để watch EndpointSlice
	client, err := grpck8s.NewClient(ctx, "k8s:///billing/payment-service:grpc", cfg)
	if err != nil {
		log.Fatalf("Khởi tạo Client thất bại: %v", err)
	}
	defer client.Close() // Giải phóng tài nguyên, dừng watcher và đóng các kết nối khi tắt app

	// Thực hiện gọi RPC
	// Bộ cân bằng tải L7 sẽ tự động phân phối các request tới các Pod khỏe mạnh
	var reply emptypb.Empty
	err = client.Invoke(ctx, "/PaymentService/ProcessPayment", &emptypb.Empty{}, &reply)
	if err != nil {
		log.Printf("Gọi RPC thất bại: %v", err)
	} else {
		fmt.Println("Thanh toán thành công!")
	}
}
```

---

## 5. Các tham số cấu hình (Configuration)

Bạn có thể thay đổi hành vi của client bằng cách ghi đè các trường trong cấu trúc `Config` trả về từ `DefaultConfig()`:

| Trường cấu hình | Kiểu dữ liệu | Mặc định | Mô tả |
|---|---|---|---|
| `DefaultNamespace` | `string` | `""` | Namespace mặc định sử dụng khi target string không khai báo namespace. |
| `Kubeconfig` | `string` | `""` | Đường dẫn file kubeconfig (dành cho môi trường dev). Chạy trong pod thì bỏ trống để tự động lấy token dịch vụ của Pod. |
| `LoadBalancingPolicy`| `string` | `"round_robin"`| Thuật toán cân bằng tải: `"round_robin"` hoặc `"least_request"`. |
| `EnableHealthCheck` | `bool` | `true` | Cho phép chủ động kiểm tra sức khỏe của backend (`grpc.health.v1`). |
| `HealthCheckServiceName`| `string`| `""` | Tên dịch vụ cần kiểm tra sức khỏe gửi kèm trong probe. |
| `HealthCheckInterval`| `time.Duration`| `10s` | Khoảng thời gian giữa các probe Check nếu backend không hỗ trợ Watch stream. |
| `HealthCheckTimeout` | `time.Duration`| `2s` | Thời gian tối đa (timeout) cho mỗi lượt probe. |
| `ResolverGraceWindow`| `time.Duration`| `30s` | Thời gian tiếp tục sử dụng danh sách IP cũ khi kết nối tới Kubernetes API server bị gián đoạn. |
| `MaxSubconnections` | `int` | `0` (không giới hạn)| Giới hạn số lượng subconnection tối đa cho mỗi Client. |
| `DialOptions` | `[]grpc.DialOption`| `nil` | Các tùy chọn dial bổ sung (cấu hình TLS, credentials, tracing interceptors). |

---

## 6. Tính năng nâng cao

### 6.1 Bỏ qua Retry đối với gọi hàm non-idempotent
Theo mặc định, các lệnh gọi bị lỗi có mã trạng thái nằm trong danh sách `RetryPolicy.RetryableStatusCodes` sẽ được tự động gọi lại trên một Pod khác. Bạn có thể vô hiệu hóa cơ chế này cho một cuộc gọi cụ thể bằng cách truyền option `NonIdempotent()`:

```go
client.Invoke(ctx, method, in, out, grpck8sbalancer.NonIdempotent())
```

### 6.2 Đăng ký thuật toán cân bằng tải tùy chỉnh
Bạn có thể tự đăng ký thuật toán chọn pod tùy chỉnh (custom load balancing picker):

```go
import (
	grpck8s "github.com/grpc-k8s-balancer/grpc-k8s-balancer"
	internal "github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/balancer"
)

func init() {
	grpck8s.RegisterPolicy("custom_random", func(healthy []*internal.SubConnEntry) grpcbalancer.Picker {
		return &myRandomPicker{healthy: healthy}
	})
}
```

---

## 7. Các khuyến nghị vận hành (Best Practices)

1. **Hủy Context khi tắt ứng dụng:** Luôn hủy `context.Context` truyền vào `NewClient` hoặc gọi `client.Close()` khi ứng dụng kết thúc. Điều này đảm bảo các Kubernetes Informer ngầm dừng lại lập tức, dọn dẹp các watcher kiểm tra sức khỏe, và đóng kết nối một cách an toàn tránh rò rỉ goroutine.
2. **Cấu hình TLS/mTLS:** Nếu các backend yêu cầu TLS/mTLS, hãy cấu hình transport credentials trong trường `DialOptions` của cấu trúc `Config`. Chúng sẽ tự động ghi đè cơ chế kết nối insecure mặc định của thư viện.
3. **Cơ chế Draining khi Pod tắt:** Khi một Pod bị chấm dứt, resolver thông báo đến client. Thư viện sẽ đưa kết nối tương ứng vào trạng thái "draining" — từ chối nhận request mới nhưng cho phép các request hiện tại có tối đa 5 giây để xử lý nốt. Do đó, hãy cho phép Pod tắt từ từ, tránh kill nóng Pod ngay lập tức.
