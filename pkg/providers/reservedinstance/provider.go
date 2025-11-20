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

// package reservedinstance provides a mechanism for tracking and utilizing EC2 Reserved Instances (RIs).
// It's designed to help Karpenter make cost-aware decisions by identifying when it can launch
// an EC2 instance that is covered by an existing reservation, thus avoiding on-demand pricing.
// This package abstracts the AWS API calls and caching logic needed to maintain an up-to-date
// view of available RI capacity.
package reservedinstance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/patrickmn/go-cache"
	"github.com/samber/lo"
	"k8s.io/utils/clock"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	"github.com/aws/karpenter-provider-aws/pkg/aws"
)

const (
	// cacheKey is the key used to store and retrieve the reserved instance data from the cache.
	cacheKey = "reserved-instances"
)

// Provider is an interface for getting reserved instance data.
// In Go, interfaces are defined implicitly. Any type that implements all the methods
// of an interface is considered to be of that interface type. This is known as structural typing,
// which is different from Java's explicit `implements` keyword.
type Provider interface {
	// GetReservedInstances returns all reserved instances that are currently active and have available capacity.
	// The context.Context parameter is a standard Go pattern for passing request-scoped values,
	// cancellation signals, and deadlines across API boundaries and between processes.
	GetReservedInstances(ctx context.Context) ([]*ReservedInstance, error)
	// MarkLaunched decrements the count of available instances for a given instance type and availability zone.
	// This is called when Karpenter launches an instance that is expected to consume an RI.
	MarkLaunched(instanceType ec2types.InstanceType, zone string)
	// MarkTerminated increments the count of available instances for a given instance type and availability zone.
	// This is called when an instance that was consuming an RI is terminated.
	MarkTerminated(instanceType ec2types.InstanceType, zone string)
}

// DefaultProvider is the concrete implementation of the Provider interface.
// It uses the AWS API to get reserved instance information and caches it for performance.
type DefaultProvider struct {
	// ec2 is the AWS EC2 API client.
	ec2 sdk.EC2API
	// cache stores the availability of reserved instances.
	cache *availabilityCache
	// cm is used to monitor and log changes to the reserved instance data.
	cm *pretty.ChangeMonitor
	// mu is a mutex, which stands for "mutual exclusion". It's a locking mechanism used to ensure that
	// only one goroutine (a lightweight thread managed by the Go runtime) can access the critical sections
	// of the code at a time. This prevents race conditions when updating the cache.
	mu sync.Mutex
}

// instanceCounts is a custom type alias for a nested map.
// It maps an EC2 instance type to another map, which in turn maps an availability zone to the count of instances.
// In Go, you can define new types based on existing ones, which can improve code readability.
// map[ec2types.InstanceType]map[string]int32 is equivalent to Map<InstanceType, Map<String, Integer>> in Java.
type instanceCounts map[ec2types.InstanceType]map[string]int32

// NewDefaultProvider constructs a new DefaultProvider.
// This is a common Go pattern for creating instances of a struct, similar to a constructor in Java or Python.
func NewDefaultProvider(ec2 sdk.EC2API, cache *cache.Cache) *DefaultProvider {
	return &DefaultProvider{
		ec2: ec2,
		cm:  pretty.NewChangeMonitor(),
		cache: &availabilityCache{
			cache: cache,
			clk:   clock.RealClock{},
		},
	}
}

// GetReservedInstances gets all reserved instances and adds them to the cache.
// This method implements the double-checked locking pattern to minimize the time the lock is held.
func (p *DefaultProvider) GetReservedInstances(ctx context.Context) ([]*ReservedInstance, error) {
	// First, try to build the reserved instances from the cache without locking.
	// This is a quick check to avoid the overhead of locking if the cache is already populated.
	reservedInstances := p.buildReservedInstancesFromCache()
	if len(reservedInstances) > 0 {
		return reservedInstances, nil
	}

	// If the cache is empty, acquire a lock to ensure only one goroutine repopulates it.
	p.mu.Lock()
	// `defer` is a Go keyword that schedules a function call to be run immediately before the function
	// in which it is called returns. This is a robust way to ensure that resources, like this mutex,
	// are released, regardless of how the function exits (e.g., normal return, panic).
	defer p.mu.Unlock()

	// After acquiring the lock, we need to check the cache again. It's possible that another
	// goroutine acquired the lock and populated the cache while this one was waiting.
	reservedInstances = p.buildReservedInstancesFromCache()
	if len(reservedInstances) > 0 {
		return reservedInstances, nil
	}

	// If the cache is still empty, fetch the data from the AWS API.
	// DescribeReservedInstances gets all RIs owned by the account.
	riOutput, err := p.ec2.DescribeReservedInstances(ctx, &ec2.DescribeReservedInstancesInput{
		Filters: []ec2types.Filter{
			{
				Name:   lo.ToPtr("state"),
				Values: []string{string(ec2types.ReservedInstanceStateActive)},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("describing reserved instances, %w", err)
	}

	// DescribeInstances gets all running instances to determine which RIs are already in use.
	instanceOutput, err := p.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{
				Name:   lo.ToPtr("instance-state-name"),
				Values: []string{string(ec2types.InstanceStateNameRunning)},
			},
			{
				Name:   lo.ToPtr("tenancy"),
				Values: []string{string(ec2types.TenancyDefault)},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("describing instances, %w", err)
	}

	// Update the cache with the fresh data from the API.
	p.updateCache(riOutput.ReservedInstances, instanceOutput.Reservations)

	// Build the result from the now-populated cache.
	reservedInstances = p.buildReservedInstancesFromCache()
	// If the reserved instance data has changed since the last check, log a diff.
	if p.cm.HasChanged(cacheKey, reservedInstances) {
		pretty.FriendlyDiff(p.cm.Previous(cacheKey), p.cm.Current(cacheKey))
	}
	return reservedInstances, nil
}

// MarkLaunched delegates to the underlying availabilityCache to decrement the count for a given RI.
func (p *DefaultProvider) MarkLaunched(instanceType ec2types.InstanceType, zone string) {
	p.cache.MarkLaunched(instanceType, zone)
}

// MarkTerminated delegates to the underlying availabilityCache to increment the count for a given RI.
func (p *DefaultProvider) MarkTerminated(instanceType ec2types.InstanceType, zone string) {
	p.cache.MarkTerminated(instanceType, zone)
}

// updateCache processes the raw data from the AWS API and updates the internal cache.
func (p *DefaultProvider) updateCache(ris []ec2types.ReservedInstances, reservations []ec2types.Reservation) {
	// runningInstances tracks the number of running on-demand instances per type and zone.
	runningInstances := make(instanceCounts)
	for _, res := range reservations {
		for _, inst := range res.Instances {
			// `if _, ok := ...` is a common Go idiom to check if a key exists in a map.
			// It attempts to retrieve the value for the key, and `ok` will be true if the key exists.
			if _, ok := runningInstances[inst.InstanceType]; !ok {
				// Initialize the inner map if this is the first time we've seen this instance type.
				runningInstances[inst.InstanceType] = make(map[string]int32)
			}
			// We only consider Linux/Unix instances for RI matching, as Windows instances have separate RIs.
			if inst.Platform != ec2types.PlatformValuesWindows {
				// The `*` operator here dereferences the pointer `inst.Placement.AvailabilityZone`.
				// In Go, many fields in AWS SDK structs are pointers to distinguish between a zero value (e.g., 0, "")
				// and a value that was not present in the API response.
				runningInstances[inst.InstanceType][*inst.Placement.AvailabilityZone]++
			}
		}
	}

	// purchasedRIs tracks the total number of purchased RIs per type and zone.
	purchasedRIs := make(instanceCounts)
	// Filter out RIs that are expired or not for default tenancy.
	activeRIs := lo.Filter(ris, func(r ec2types.ReservedInstances, _ int) bool {
		return r.End.After(time.Now()) && r.InstanceTenancy == ec2types.TenancyDefault
	})
	for _, ri := range activeRIs {
		if _, ok := purchasedRIs[ri.InstanceType]; !ok {
			purchasedRIs[ri.InstanceType] = make(map[string]int32)
		}
		purchasedRIs[ri.InstanceType][*ri.AvailabilityZone] += *ri.InstanceCount
	}

	// availability calculates the number of available RIs by subtracting running instances from purchased RIs.
	availability := make(map[string]*availabilityCacheEntry)
	// `for instType, zones := range purchasedRIs` is Go's syntax for iterating over a map.
	// It's similar to `for key, value in my_dict.items()` in Python.
	for instType, zones := range purchasedRIs {
		for zone, count := range zones {
			runningCount := int32(0)
			if _, ok := runningInstances[instType]; ok {
				runningCount = runningInstances[instType][zone]
			}
			availableCount := count - runningCount
			key := p.cache.makeCacheKey(instType, zone)
			availability[key] = &availabilityCacheEntry{
				count: availableCount,
				total: count,
			}
		}
	}
	// syncAvailability updates the cache with the newly calculated availability data.
	p.cache.syncAvailability(availability)
}

// buildReservedInstancesFromCache constructs a slice of ReservedInstance objects from the cache.
// It acquires a read lock to prevent a race condition where the cache is read while being updated.
func (p *DefaultProvider) buildReservedInstancesFromCache() []*ReservedInstance {
	p.cache.mu.RLock()
	defer p.cache.mu.RUnlock()
	var reservedInstances []*ReservedInstance
	for key, item := range p.cache.cache.Items() {
		// The type assertion `item.Object.(*availabilityCacheEntry)` is used to convert the
		// generic `interface{}` type stored in the cache back to its concrete type.
		// This is similar to casting in Java. If the assertion fails, it will cause a panic.
		entry := item.Object.(*availabilityCacheEntry)
		if entry.count > 0 {
			instanceType, zone := p.cache.decodeCacheKey(key)
			// `append` is a built-in Go function used to add elements to a slice.
			// If the underlying array of the slice has enough capacity, the element is added in place.
			// If not, a new, larger array is allocated, and the existing elements are copied over.
			reservedInstances = append(reservedInstances, &ReservedInstance{
				ID:               fmt.Sprintf("ri-%s-%s", instanceType, zone),
				InstanceType:     instanceType,
				InstanceCount:    entry.count,
				AvailabilityZone: zone,
				State:            ec2types.ReservedInstanceStateActive,
			})
		}
	}
	return reservedInstances
}