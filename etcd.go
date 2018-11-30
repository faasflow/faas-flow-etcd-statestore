package EtcdStateStore

import (
	"fmt"
	etcd "go.etcd.io/etcd/client"

	"context"
	faasflow "github.com/s8sg/faas-flow"
	"strconv"
)

type EtcdStateStore struct {
	etcdKeyPath string
	kv          etcd.KeysAPI

	RetryCount int
}

func GetEtcdStateStore(address string) (faasflow.StateStore, error) {

	etcdST := &EtcdStateStore{}

	config := etcd.Config{
		Endpoints: []string{address},
		Transport: etcd.DefaultTransport,
	}

	ec, err := etcd.New(config)
	if err != nil {
		return nil, err
	}
	etcdST.kv = etcd.NewKeysAPI(ec)
	etcdST.RetryCount = 10

	return etcdST, nil
}

// Configure
func (etcdStore *EtcdStateStore) Configure(flowName string, requestId string) {
	etcdStore.etcdKeyPath = fmt.Sprintf("faasflow/%s/%s", flowName, requestId)
}

// Init (Called only once in a request)
func (etcdStore *EtcdStateStore) Init() error {
	return nil
}

// Create
func (etcdStore *EtcdStateStore) Create(vertexs []string) error {

	for _, vertex := range vertexs {
		key := fmt.Sprintf("%s/%s", etcdStore.etcdKeyPath, vertex)
		_, err := etcdStore.kv.Set(context.Background(), key, "0", nil)
		if err != nil {
			return fmt.Errorf("failed to create vertex %s, error %v", vertex, err)
		}
	}
	return nil
}

// IncrementCounter
func (etcdStore *EtcdStateStore) IncrementCounter(vertex string) (int, error) {
	var serr error
	count := 0
	key := fmt.Sprintf("%s/%s", etcdStore.etcdKeyPath, vertex)
	for i := 0; i < etcdStore.RetryCount; i++ {
		resp, err := etcdStore.kv.Get(context.Background(), key, nil)
		if err != nil {
			return 0, fmt.Errorf("failed to get vertex %s, error %v", vertex, err)
		}
		if resp == nil {
			return 0, fmt.Errorf("failed to get vertex %s", vertex)
		}
		modifyIndex := resp.Node.ModifiedIndex

		counter, err := strconv.Atoi(resp.Node.Value)
		if err != nil {
			return 0, fmt.Errorf("failed to update counter for %s, error %v", vertex, err)
		}

		count = counter + 1
		counterStr := fmt.Sprintf("%d", count)

		_, err = etcdStore.kv.Set(context.Background(), key, counterStr, &etcd.SetOptions{PrevIndex: modifyIndex})
		if err == nil {
			return count, nil
		}
		serr = err
	}
	return 0, fmt.Errorf("failed to update counter after max retry for %s, error %v", vertex, serr)
}

// SetState set state of pipeline
func (etcdStore *EtcdStateStore) SetState(state bool) error {
	key := fmt.Sprintf("%s/state", etcdStore.etcdKeyPath)
	stateStr := "false"
	if state {
		stateStr = "true"
	}

	_, err := etcdStore.kv.Set(context.Background(), key, stateStr, nil)
	if err != nil {
		return fmt.Errorf("failed to set state, error %v", err)
	}
	return nil
}

// GetState set state of the pipeline
func (etcdStore *EtcdStateStore) GetState() (bool, error) {
	key := fmt.Sprintf("%s/state", etcdStore.etcdKeyPath)
	resp, err := etcdStore.kv.Get(context.Background(), key, nil)
	if err != nil {
		return false, fmt.Errorf("failed to get state, error %v", err)
	}
	if resp == nil {
		return false, fmt.Errorf("failed to get state")
	}
	state := false
	if resp.Node.Value == "true" {
		state = true
	}
	return state, nil
}

// Cleanup (Called only once in a request)
func (etcdStore *EtcdStateStore) Cleanup() error {
	_, err := etcdStore.kv.Delete(context.Background(), etcdStore.etcdKeyPath, &etcd.DeleteOptions{Recursive: true})
	if err != nil {
		return fmt.Errorf("error removing %s, error %v", etcdStore.etcdKeyPath, err)
	}
	return nil
}
