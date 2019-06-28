package EtcdStateStore

import (
	"context"
	"fmt"
	faasflow "github.com/s8sg/faas-flow"
	etcd "go.etcd.io/etcd/client"
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

// Update Compare and Update a valuer
func (etcdStore *EtcdStateStore) Update(key string, oldValue string, newValue string) error {
	key = fmt.Sprintf("%s/%s", etcdStore.etcdKeyPath, key)

	resp, err := etcdStore.kv.Get(context.Background(), key, nil)
	if err != nil {
		return fmt.Errorf("failed to get key %s, error %v", key, err)
	}
	if resp == nil {
		return fmt.Errorf("failed to get key %s, returned nil", key)
	}
	modifyIndex := resp.Node.ModifiedIndex

	if resp.Node.Value != oldValue {
		return fmt.Errorf("Old value doesn't match for key %s", key, err)
	}

	_, err = etcdStore.kv.Set(context.Background(), key, newValue, &etcd.SetOptions{PrevIndex: modifyIndex})
	if err != nil {
		return fmt.Errorf("failed to update value for key %s, error %v", key, err)
	}
	return nil
}

// Set Sets a value (override existing, or create one)
func (etcdStore *EtcdStateStore) Set(key string, value string) error {
	key = fmt.Sprintf("%s/%s", etcdStore.etcdKeyPath, key)

	_, err := etcdStore.kv.Set(context.Background(), key, value, nil)
	if err != nil {
		return fmt.Errorf("failed to set value for key %s, error %v", key, err)
	}
	return nil
}

// Get Gets a value
func (etcdStore *EtcdStateStore) Get(key string) (string, error) {
	key = fmt.Sprintf("%s/%s", etcdStore.etcdKeyPath, key)

	resp, err := etcdStore.kv.Get(context.Background(), key, nil)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s, error %v", key, err)
	}
	if resp == nil {
		return "", fmt.Errorf("failed to get key %s, returned nil", key)
	}
	return resp.Node.Value, nil
}

// Cleanup (Called only once in a request)
func (etcdStore *EtcdStateStore) Cleanup() error {
	_, err := etcdStore.kv.Delete(context.Background(), etcdStore.etcdKeyPath, &etcd.DeleteOptions{Recursive: true})
	if err != nil {
		return fmt.Errorf("error removing %s, error %v", etcdStore.etcdKeyPath, err)
	}
	return nil
}
