package grpck8sbalancer

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/balancer"
	k8sresolver "github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver"
	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver/dns"
	"github.com/grpc-k8s-balancer/grpc-k8s-balancer/internal/resolver/k8s"
)

// grpcResolverWrapper wraps our internal resolver.Resolver into a gRPC resolver.Resolver.
type grpcResolverWrapper struct {
	inner  k8sresolver.Resolver
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (r *grpcResolverWrapper) ResolveNow(resolver.ResolveNowOptions) {
	// Our resolvers use active informers/watchers (k8s) or tickers (dns),
	// so ResolveNow is a no-op as updates are pushed automatically.
}

func (r *grpcResolverWrapper) Close() {
	r.cancel()
	_ = r.inner.Close()
	r.wg.Wait()
}

// k8sResolverBuilder implements grpc.resolver.Builder for the "k8s" scheme.
type k8sResolverBuilder struct {
	cfg *Config
	t   Target
}

func (b *k8sResolverBuilder) Scheme() string { return "k8s" }

func (b *k8sResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	kubeClient, err := k8s.NewKubeClient(b.cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("k8s resolver build: failed to construct client: %w", err)
	}

	watcherCfg := k8s.WatcherConfig{
		Namespace:   b.t.Namespace,
		ServiceName: b.t.Service,
		Port: k8s.Port{
			Name:   b.t.Port.Name,
			Number: b.t.Port.Number,
		},
		KubeClient: kubeClient,
		Logger:     b.cfg.Logger,
	}

	watcher := k8s.NewWatcher(watcherCfg)
	sm := k8s.NewStateMachine(watcher, b.cfg.ResolverGraceWindow, b.cfg.Logger)

	ctx, cancel := context.WithCancel(context.Background())
	wrapper := &grpcResolverWrapper{
		inner:  sm,
		cancel: cancel,
	}

	wrapper.wg.Add(1)
	err = sm.Start(ctx, func(snap k8sresolver.Snapshot) {
		addrs := make([]resolver.Address, len(snap.Endpoints))
		for i, ep := range snap.Endpoints {
			addrs[i] = resolver.Address{
				Addr: fmt.Sprintf("%s:%d", ep.Address, ep.Port),
			}
		}

		// Pass the balancer configuration dynamically via gRPC attributes (Task 2).
		// This prevents concurrency race conditions with package-level globals.
		bCfg := &balancer.BuildConfig{
			Namespace:   b.t.Namespace,
			Service:     b.t.Service,
			Policy:      b.cfg.LoadBalancingPolicy,
			MaxSubconns: b.cfg.MaxSubconnections,
			Logger:      b.cfg.Logger,
		}

		cc.UpdateState(resolver.State{
			Addresses:  addrs,
			Attributes: attributes.New(balancer.ConfigKey, bCfg),
		})
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("k8s resolver start: %w", err)
	}

	go func() {
		defer wrapper.wg.Done()
		<-ctx.Done()
	}()

	return wrapper, nil
}

// dnsResolverBuilder implements grpc.resolver.Builder for the "dns" scheme.
type dnsResolverBuilder struct {
	cfg *Config
	t   Target
}

func (b *dnsResolverBuilder) Scheme() string { return "dns" }

func (b *dnsResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	dnsBuilder := &dns.Builder{
		LookupFn: nil,
	}
	port := b.t.Port.Number
	innerResolver := dnsBuilder.NewResolver(b.t.Service, port)

	ctx, cancel := context.WithCancel(context.Background())
	wrapper := &grpcResolverWrapper{
		inner:  innerResolver,
		cancel: cancel,
	}

	wrapper.wg.Add(1)
	err := innerResolver.Start(ctx, func(snap k8sresolver.Snapshot) {
		addrs := make([]resolver.Address, len(snap.Endpoints))
		for i, ep := range snap.Endpoints {
			addrs[i] = resolver.Address{
				Addr: fmt.Sprintf("%s:%d", ep.Address, ep.Port),
			}
		}

		bCfg := &balancer.BuildConfig{
			Namespace:   "",
			Service:     b.t.Service,
			Policy:      b.cfg.LoadBalancingPolicy,
			MaxSubconns: b.cfg.MaxSubconnections,
			Logger:      b.cfg.Logger,
		}

		cc.UpdateState(resolver.State{
			Addresses:  addrs,
			Attributes: attributes.New(balancer.ConfigKey, bCfg),
		})
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("dns resolver start: %w", err)
	}

	go func() {
		defer wrapper.wg.Done()
		<-ctx.Done()
	}()

	return wrapper, nil
}
