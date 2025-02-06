package mc

import (
	"errors"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type LoadBalancer struct {
	clients      []*minio.Client
	failures     []time.Time
	currentIndex int
	mutex        sync.Mutex
}

func New(endpoints []string, accessKey, secretKey string, secure bool, healthCheckDuration time.Duration) (*LoadBalancer, error) {
	var clients []*minio.Client
	failures := make([]time.Time, len(endpoints))

	for i, endpoint := range endpoints {
		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: secure,
		})
		if err != nil {
			return nil, err
		}
		_, err = client.HealthCheck(healthCheckDuration)
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)

		failures[i] = time.Time{}
	}

	return &LoadBalancer{
		clients:      clients,
		failures:     failures,
		currentIndex: 0,
	}, nil
}

func (lb *LoadBalancer) Get() (int, *minio.Client, error) {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	startIdx := lb.currentIndex
	for {
		idx := lb.currentIndex % len(lb.clients)
		originalIdx := idx
		lb.currentIndex = (lb.currentIndex + 1) % len(lb.clients)

		// healthy idx
		if lb.clients[idx].IsOnline() {
			return originalIdx, lb.clients[idx], nil
		}

		// recent unhealthy idx
		if lb.failures[idx].IsZero() {
			// mark as unhealthy
			lb.failures[idx] = time.Now()
			continue
		}

		// unhealthy idx since 10 minutes
		if time.Since(lb.failures[idx]) >= 10*time.Minute {
			if healthy := lb.clients[idx].IsOnline(); healthy {
				lb.failures[idx] = time.Time{}
				return originalIdx, lb.clients[idx], nil
			} else {
				lb.failures[idx] = time.Now()
			}
		}

		if lb.currentIndex == startIdx {
			return -1, nil, errors.New("no servers available")
		}
	}
}
