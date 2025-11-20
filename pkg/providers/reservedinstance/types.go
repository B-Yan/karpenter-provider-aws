/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// package reservedinstance provides types and methods for querying and managing
// EC2 Reserved Instances. This package is designed to abstract the complexities
// of EC2 Reserved Instances (RIs) and provide a simplified interface for
// tracking their availability and usage.
package reservedinstance

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/patrickmn/go-cache"
	"k8s.io/utils/clock"
)

// ReservedInstance is a struct that defines the parameters for an EC2 Reserved Instance.
// In Go, a struct is a composite type that groups together zero or more named
// values of arbitrary types as a single entity. It's similar to a class in Java
// or Python, but it only contains data fields and does not have methods
// associated with it in the same way.
type ReservedInstance struct {
	// ID is the unique identifier for the Reserved Instance.
	ID string
	// InstanceType specifies the EC2 instance type, such as "t2.micro" or "m5.large".
	InstanceType ec2types.InstanceType
	// InstanceCount is the number of instances covered by this reservation.
	InstanceCount int32
	// AvailabilityZone is the AWS Availability Zone where the RI is located, e.g., "us-east-1a".
	AvailabilityZone string
	// State indicates the current status of the Reserved Instance, such as "active" or "retired".
	State ec2types.ReservedInstanceState
}

// availabilityCache provides an in-memory cache for tracking the availability of
// Reserved Instances. It is designed to be thread-safe and uses a time-based
// cache to store data.
//
// Go doesn't have classes in the same way as Java or Python. Instead, it uses
// structs to hold data and methods can be associated with any named type. This
// approach allows for more flexible and decoupled designs.
type availabilityCache struct {
	// mu is a RWMutex (Read-Write Mutex) that protects the cache from concurrent
	// access. This is a common Go pattern for ensuring thread safety. A RWMutex
	// allows multiple readers to access the data simultaneously, but only one
	// writer at a time.
	mu sync.RWMutex
	// cache is a pointer to a go-cache instance, which provides the underlying
	// caching mechanism. Using a pointer here is efficient as it avoids copying
	// the entire cache object.
	cache *cache.Cache
	// clk is a clock instance from the Kubernetes utils package. This allows for
	// easier testing by providing a way to mock time.
	clk clock.Clock
}

// availabilityCacheEntry represents an entry in the availability cache. It stores
// the number of available and total Reserved Instances, along with the last sync time.
type availabilityCacheEntry struct {
	// count is the number of available Reserved Instances of a specific type and zone.
	count int32
	// total is the total number of Reserved Instances of that type and zone.
	total int32
	// syncTime is the timestamp of the last time the entry was synchronized with AWS.
	syncTime time.Time
}

// makeCacheKey creates a unique key for the cache based on the instance type and zone.
// This is a method associated with the availabilityCache struct. In Go, methods
// are defined using a receiver, which is a parameter that appears before the
// function name. In this case, `(c *availabilityCache)` is the receiver.
func (c *availabilityCache) makeCacheKey(instanceType ec2types.InstanceType, zone string) string {
	// fmt.Sprintf is a Go function for formatting strings, similar to Python's f-strings
	// or Java's String.format.
	return fmt.Sprintf("%s/%s", instanceType, zone)
}

// decodeCacheKey reverses the process of makeCacheKey, extracting the instance type
// and zone from a cache key.
func (c *availabilityCache) decodeCacheKey(key string) (ec2types.InstanceType, string) {
	// strings.Split is a standard library function for splitting a string by a separator.
	parts := strings.Split(key, "/")
	// The function returns two values, which is a common pattern in Go for returning
	// a result and an error, or multiple related values.
	return ec2types.InstanceType(parts[0]), parts[1]
}

// syncAvailability atomically updates the cache with the latest availability data.
// It takes a map of cache entries as input, where the key is the cache key.
func (c *availabilityCache) syncAvailability(availability map[string]*availabilityCacheEntry) {
	now := c.clk.Now()
	// c.mu.Lock() acquires an exclusive write lock. While this lock is held, no other goroutine
	// can acquire a read or write lock on this mutex, ensuring that no other process can read
	// the cache in a partially updated state. Any other calls to methods that use this mutex
	// will block until the lock is released.
	c.mu.Lock()
	// defer c.mu.Unlock() schedules the unlock operation to be called right
	// before the function returns. This is a powerful Go feature that guarantees
	// the mutex will be released, even if the function panics.
	defer c.mu.Unlock()
	// c.cache.Flush() clears all existing entries from the cache before repopulating it.
	c.cache.Flush()
	// This is a for-range loop, which is the standard way to iterate over maps,
	// slices, and arrays in Go.
	for key, entry := range availability {
		entry.syncTime = now
		c.cache.Set(key, entry)
	}
}

// MarkLaunched decrements the count of available instances for a given type and zone.
// This method is called when a new instance is launched using a Reserved Instance.
func (c *availabilityCache) MarkLaunched(instanceType ec2types.InstanceType, zone string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.makeCacheKey(instanceType, zone)
	// The if statement here is a common Go idiom. It checks if a key exists in the
	// cache and, if it does, assigns the value to the 'entry' variable.
	if entry, ok := c.cache.Get(key); ok {
		// The value returned from the cache is of type interface{}, so it needs to be
		// type-asserted to its actual type, *availabilityCacheEntry.
		cacheEntry := entry.(*availabilityCacheEntry)
		cacheEntry.count--
	}
}

// MarkTerminated increments the count of available instances for a given type and zone.
// This method is called when an instance that was using a Reserved Instance is terminated.
func (c *availabilityCache) MarkTerminated(instanceType ec2types.InstanceType, zone string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.makeCacheKey(instanceType, zone)
	if entry, ok := c.cache.Get(key); ok {
		cacheEntry := entry.(*availabilityCacheEntry)
		if cacheEntry.count < cacheEntry.total {
			cacheEntry.count++
		}
	}
}
