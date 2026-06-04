# go-kubegrpc

[English](#english) | [Tiếng Việt](#tiếng-việt)

---

## English

**go-kubegrpc** is a smart, Kubernetes-native client-side load balancer and service resolver for gRPC services written in Go.

It eliminates the dependency on service mesh sidecar proxies (such as Istio or Linkerd) for east-west gRPC communication by implementing real-time service discovery, active connection pooling, health checking, and retry failover directly in your application process.

### Key Features
* **Kubernetes Service Discovery:** Watches `EndpointSlice` API resources in real time to trace backend Pods as they scale up, down, or restart.
* **Smart Load Balancing:** Built-in L7 policies: **Round Robin** and **Least Request** (minimizes concurrent in-flight RPCs per pod).
* **Active Health Probing:** Probes backend endpoints using the gRPC Health Checking Protocol (`grpc.health.v1`) with streaming `Watch` or polling fallback.
* **Failover & Retries:** Automatically retries transient failures on different healthy backend pods with exponential backoff.
* **Concurrency Safe:** Zero global configurations, preventing race conditions when initializing multiple clients concurrently.

### Integration Guide
* **English Guide:** See [docs/en/integration_guide.md](file:///home/duccv/duccv-project/go-kubegrpc/docs/en/integration_guide.md) for full installation, configuration, and setup steps.

---

## Tiếng Việt

**go-kubegrpc** là một thư viện cân bằng tải L7 (phía client) và khám phá dịch vụ (service discovery) nguyên bản cho Kubernetes dành cho các ứng dụng gRPC viết bằng Go.

Thư viện loại bỏ sự phụ thuộc vào các sidecar proxy (như Istio hoặc Linkerd) đối với các kết nối nội bộ bằng cách tự tích hợp trình tìm kiếm dịch vụ thông minh, quản lý nhóm kết nối (connection pool), kiểm tra sức khỏe và cơ chế tự động thử lại khi gặp lỗi trực tiếp ngay bên trong tiến trình ứng dụng của bạn.

### Các tính năng chính
* **Tự khám phá dịch vụ trên K8s:** Theo dõi tài nguyên `EndpointSlice` theo thời gian thực để cập nhật danh sách Pod khi scale-up, scale-down hoặc restart.
* **Cân bằng tải thông minh:** Tích hợp sẵn thuật toán **Round Robin** và **Least Request** (chọn pod đang xử lý ít request đồng thời nhất).
* **Chủ động dò sức khỏe (Health Check):** Gửi các đầu dò kiểm tra sức khỏe bằng giao thức chuẩn gRPC (`grpc.health.v1`) qua stream `Watch` hoặc fallback Check.
* **Tự động thử lại thông minh (Failover):** Khi một RPC lỗi, thư viện tự động thử lại trên một Pod khỏe mạnh khác với cơ chế exponential backoff.
* **An toàn đa luồng:** Thiết kế không dùng biến cấu hình dùng chung toàn cục (global states), an toàn khi chạy ứng dụng đa luồng phức tạp.

### Tài liệu hướng dẫn tích hợp
* **Tài liệu Tiếng Việt:** Xem tại [docs/vi/integration_guide.md](file:///home/duccv/duccv-project/go-kubegrpc/docs/vi/integration_guide.md) để biết chi tiết hướng dẫn cài đặt, cấu hình RBAC và các khuyến nghị vận hành.

---

## Quick Start / Khởi đầu nhanh

```go
package main

import (
	"context"
	"log"

	grpck8s "github.com/grpc-k8s-balancer/grpc-k8s-balancer"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	ctx := context.Background()

	// 1. Get default configs / Lấy cấu hình mặc định
	cfg := grpck8s.DefaultConfig()
	cfg.DefaultNamespace = "default"
	cfg.LoadBalancingPolicy = "least_request"

	// 2. Initialize Client / Khởi tạo Client
	client, err := grpck8s.NewClient(ctx, "k8s:///default/my-service:grpc", cfg)
	if err != nil {
		log.Fatalf("Failed to initialize client: %v", err)
	}
	defer client.Close()

	// 3. Make gRPC calls / Thực hiện các cuộc gọi gRPC
	var reply emptypb.Empty
	err = client.Invoke(ctx, "/MyService/MyMethod", &emptypb.Empty{}, &reply)
	if err != nil {
		log.Printf("Error: %v", err)
	}
}
```
