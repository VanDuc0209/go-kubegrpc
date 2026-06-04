package k8s

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewKubeClient creates a Kubernetes client.
// If kubeconfig is non-empty, it uses clientcmd.BuildConfigFromFlags("", kubeconfig).
// Otherwise it tries rest.InClusterConfig().
// Returns a typed error wrapping the underlying cause when neither path works.
func NewKubeClient(kubeconfig string) (kubernetes.Interface, error) {
	if kubeconfig != "" {
		restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("building REST config from kubeconfig %q: %w", kubeconfig, err)
		}

		client, err := kubernetes.NewForConfig(restCfg)
		if err != nil {
			return nil, fmt.Errorf("creating Kubernetes client from kubeconfig %q: %w", kubeconfig, err)
		}

		return client, nil
	}

	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("building in-cluster REST config: %w", err)
	}

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kubernetes client from in-cluster config: %w", err)
	}

	return client, nil
}
