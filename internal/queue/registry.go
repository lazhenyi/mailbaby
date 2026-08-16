package queue

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"mailbaby/internal/config"
)

// FactoryFunc is a function signature for initializing a Queue instance from configuration.
type FactoryFunc func(cfg *config.Config) (Queue, error)

var (
	registryMu sync.RWMutex
	drivers    = make(map[config.QueueDriver]FactoryFunc)
)

// Register registers a queue driver factory.
// It is typically called in the init() function of driver implementation packages.
// Calling Register with a duplicate driver name or a nil factory will panic.
func Register(driver config.QueueDriver, factory FactoryFunc) {
	registryMu.Lock()
	defer registryMu.Unlock()

	normalized := config.QueueDriver(strings.ToLower(strings.TrimSpace(string(driver))))
	if normalized == "" {
		panic("queue: cannot register driver with empty name")
	}
	if factory == nil {
		panic(fmt.Sprintf("queue: Register driver %q factory is nil", driver))
	}
	if _, exists := drivers[normalized]; exists {
		panic(fmt.Sprintf("queue: Register called twice for driver %q", driver))
	}
	drivers[normalized] = factory
}

// Unregister removes a registered driver (primarily used for unit testing).
func Unregister(driver config.QueueDriver) {
	registryMu.Lock()
	defer registryMu.Unlock()
	delete(drivers, config.QueueDriver(strings.ToLower(strings.TrimSpace(string(driver)))))
}

// IsDriverRegistered checks if a specific queue driver has been registered.
func IsDriverRegistered(driver config.QueueDriver) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	normalized := config.QueueDriver(strings.ToLower(strings.TrimSpace(string(driver))))
	_, exists := drivers[normalized]
	return exists
}

// GetRegisteredDrivers returns a sorted list of all currently registered queue drivers.
func GetRegisteredDrivers() []config.QueueDriver {
	registryMu.RLock()
	defer registryMu.RUnlock()

	result := make([]config.QueueDriver, 0, len(drivers))
	for name := range drivers {
		result = append(result, name)
	}
	sort.Slice(result, func(i, j int) bool {
		return string(result[i]) < string(result[j])
	})
	return result
}

// New creates and initializes a Queue instance according to cfg.Queue.Driver.
func New(cfg *config.Config) (Queue, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: config is nil", ErrInvalidConfig)
	}

	driver := config.QueueDriver(strings.ToLower(strings.TrimSpace(string(cfg.Queue.Driver))))
	if driver == "" {
		driver = config.DriverMemory
	}

	registryMu.RLock()
	factory, exists := drivers[driver]
	registryMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: driver %q is not registered (available: %v)",
			ErrDriverNotFound, driver, GetRegisteredDrivers())
	}

	return factory(cfg)
}

// NewQueue creates a Queue instance given only a QueueConfig.
func NewQueue(qCfg *config.QueueConfig) (Queue, error) {
	if qCfg == nil {
		return nil, fmt.Errorf("%w: queue config is nil", ErrInvalidConfig)
	}
	return New(&config.Config{
		Queue: *qCfg,
	})
}
