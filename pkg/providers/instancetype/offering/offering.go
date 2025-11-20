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

// Package offering provides logic for retrieving and caching offerings for EC2 instance types.
// An "offering" in this context is a specific purchasing option for an instance type,
// such as on-demand, spot, or a capacity reservation, in a particular availability zone.
// For a developer coming from Java or Python, you can think of this package as a service
// that enriches instance type data with pricing and availability information.
package offering

import (
	"context"
	"fmt"
	"sync"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/patrickmn/go-cache"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	awscache "github.com/aws/karpenter-provider-aws/pkg/cache"
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
	"github.com/aws/karpenter-provider-aws/pkg/providers/pricing"
	"github.com/aws/karpenter-provider-aws/pkg/providers/reservedinstance"
)

// Provider is an interface for injecting offerings into a list of instance types.
// In Go, interfaces are implemented implicitly. Any struct that has a method with
// the same signature as the interface's method is considered to have implemented that interface.
type Provider interface {
	// InjectOfferings enriches a slice of cloudprovider.InstanceType with available offerings.
	InjectOfferings(context.Context, []*cloudprovider.InstanceType, *v1.EC2NodeClass, []string) []*cloudprovider.InstanceType
}

type NodeClass interface {
	CapacityReservations() []v1.CapacityReservation
	ZoneInfo() []v1.ZoneInfo
}

// DefaultProvider is the concrete implementation of the Provider interface.
// It uses various providers and caches to gather and store offering information.
// Think of this as a class in Java/Python that implements the Provider interface.
// The fields are dependencies that are injected during its construction.
type DefaultProvider struct {
	pricingProvider             pricing.Provider
	capacityReservationProvider capacityreservation.Provider
	reservedInstanceProvider    reservedinstance.Provider
	unavailableOfferings        *awscache.UnavailableOfferings
	// lastUnavailableOfferingsSeqNum is used to track changes in unavailable offerings.
	// A sync.Map is a thread-safe map, useful when multiple goroutines (Go's lightweight threads)
	// might access it concurrently.
	lastUnavailableOfferingsSeqNum sync.Map // instance type -> seqNum
	cache                          *cache.Cache
}

// NewDefaultProvider is a constructor function for DefaultProvider.
// In Go, it's idiomatic to have a `New...` function that returns a pointer to a new struct instance.
// This is similar to a constructor in Java or `__init__` in Python.
func NewDefaultProvider(
	pricingProvider pricing.Provider,
	capacityReservationProvider capacityreservation.Provider,
	reservedInstanceProvider reservedinstance.Provider,
	unavailableOfferingsCache *awscache.UnavailableOfferings,
	offeringCache *cache.Cache,
) *DefaultProvider {
	return &DefaultProvider{
		pricingProvider:             pricingProvider,
		capacityReservationProvider: capacityReservationProvider,
		reservedInstanceProvider:    reservedInstanceProvider,
		unavailableOfferings:        unavailableOfferingsCache,
		cache:                       offeringCache,
	}
}

// InjectOfferings enriches a slice of cloudprovider.InstanceType with available offerings.
// It iterates through the provided instance types and, for each one, creates a set of
// offerings based on capacity type, zone, and any available reservations.
//
// A key concept in Go is the `context.Context`. It's used to carry deadlines, cancellation
// signals, and other request-scoped values across API boundaries and between processes.
// It's good practice to accept it as the first argument in a function.
func (p *DefaultProvider) InjectOfferings(
	ctx context.Context,
	instanceTypes []*cloudprovider.InstanceType,
	nodeClass NodeClass,
	allZones sets.Set[string],
) []*cloudprovider.InstanceType {
	// `lo.SliceToMap` is a utility function from the `lo` library that converts a slice to a map.
	// This is a common pattern for creating lookup maps.
	subnetZonesToZoneIDs := lo.SliceToMap(nodeClass.ZoneInfo(), func(info v1.ZoneInfo) (string, string) {
		return info.Zone, info.ZoneID
	})
	var its []*cloudprovider.InstanceType
	// The `for _, it := range instanceTypes` syntax is Go's way of iterating over a slice.
	// The `_` is used to ignore the index of the element.
	for _, it := range instanceTypes {
		offerings := p.createOfferings(
			ctx,
			it,
			nodeClass,
			allZones,
			subnetZonesToZoneIDs,
		)
		// We create a deep copy of the instance type here to avoid mutating the original slice.
		// This is important because the original slice might be a cached result that should not be changed.
		its = append(its, &cloudprovider.InstanceType{
			Name:         it.Name,
			Requirements: it.Requirements,
			Offerings:    offerings,
			Capacity:     it.Capacity,
			Overhead:     it.Overhead,
		})
	}
	return its
}

//nolint:gocyclo
// createOfferings is the core logic for generating offerings for a single instance type.
// It checks the cache first, and if there's a miss, it generates new offerings for
// on-demand and spot capacity types. It then adds offerings for any matching
// capacity reservations and reserved instances.
// The `//nolint:gocyclo` is a directive to the linter to ignore the cyclomatic complexity of this function.
// This is sometimes used for functions that have a lot of branching but are still readable.
//nolint:gocyclo
func (p *DefaultProvider) createOfferings(
	ctx context.Context,
	it *cloudprovider.InstanceType,
	nodeClass NodeClass,
	allZones sets.Set[string],
	subnetZonesToZoneIDs map[string]string,
) cloudprovider.Offerings {
	var offerings []*cloudprovider.Offering
	itZones := sets.New(it.Requirements.Get(corev1.LabelTopologyZone).Values()...)

	// If the sequence number for unavailable offerings has changed, we can't use the cached value.
	// This is a way to invalidate the cache when we know that some offerings have become unavailable.
	lastSeqNum, ok := p.lastUnavailableOfferingsSeqNum.Load(ec2types.InstanceType(it.Name))
	if !ok {
		lastSeqNum = 0
	}
	seqNum := p.unavailableOfferings.SeqNum(ec2types.InstanceType(it.Name))
	// The `if ofs, ok := ...; ok` is a common Go idiom. It declares and initializes `ofs` and `ok`,
	// then checks `ok` in the same line. `ok` is a boolean indicating if the key was found in the cache.
	if ofs, ok := p.cache.Get(p.cacheKeyFromInstanceType(it)); ok && lastSeqNum == seqNum {
		// The `ofs.([]*cloudprovider.Offering)` is a type assertion. It checks if `ofs` is of type `[]*cloudprovider.Offering`.
		offerings = append(offerings, ofs.([]*cloudprovider.Offering)...)
	} else {
		// If not in cache or cache is stale, generate new offerings.
		var cachedOfferings []*cloudprovider.Offering
		for zone := range allZones {
			for _, capacityType := range it.Requirements.Get(karpv1.CapacityTypeLabelKey).Values() {
				// Reserved capacity types are handled separately.
				if capacityType == karpv1.CapacityTypeReserved {
					continue
				}
				isUnavailable := p.unavailableOfferings.IsUnavailable(ec2types.InstanceType(it.Name), zone, capacityType)
				var price float64
				var hasPrice bool
				// The `switch` statement in Go is similar to what you'd find in other languages.
				switch capacityType {
				case karpv1.CapacityTypeOnDemand:
					price, hasPrice = p.pricingProvider.OnDemandPrice(ec2types.InstanceType(it.Name))
				case karpv1.CapacityTypeSpot:
					price, hasPrice = p.pricingProvider.SpotPrice(ec2types.InstanceType(it.Name), zone)
				default:
					// `panic` is used for unrecoverable errors. It stops the program.
					panic(fmt.Sprintf("invalid capacity type %q in requirements for instance type %q", capacityType, it.Name))
				}
				offering := &cloudprovider.Offering{
					Requirements: scheduling.NewRequirements(
						scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType),
						scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
						scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpDoesNotExist),
						scheduling.NewRequirement(v1.LabelCapacityReservationType, corev1.NodeSelectorOpDoesNotExist),
					),
					Price:     price,
					Available: !isUnavailable && hasPrice && itZones.Has(zone),
				}
				if id, ok := subnetZonesToZoneIDs[zone]; ok {
					offering.Requirements.Add(scheduling.NewRequirement(v1.LabelTopologyZoneID, corev1.NodeSelectorOpIn, id))
				}
				cachedOfferings = append(cachedOfferings, offering)
			}
		}
		p.cache.SetDefault(p.cacheKeyFromInstanceType(it), cachedOfferings)
		p.lastUnavailableOfferingsSeqNum.Store(ec2types.InstanceType(it.Name), seqNum)
		offerings = append(offerings, cachedOfferings...)
	}
	if !options.FromContext(ctx).FeatureGates.ReservedCapacity {
		return offerings
	}

	// Add offerings for any matching capacity reservations.
	capacityReservations := nodeClass.CapacityReservations()
	for i := range capacityReservations {
		if capacityReservations[i].InstanceType != it.Name {
			continue
		}
		reservation := &capacityReservations[i]
		price := 0.0
		if odPrice, ok := p.pricingProvider.OnDemandPrice(ec2types.InstanceType(it.Name)); ok {
			// We treat the reservation as "free" by giving it a very low price.
			// This ensures that consolidation logic will prefer to use reserved instances.
			price = odPrice / 10_000_000.0
		}
		reservationCapacity := p.capacityReservationProvider.GetAvailableInstanceCount(reservation.ID)
		offering := &cloudprovider.Offering{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeReserved),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, reservation.AvailabilityZone),
				scheduling.NewRequirement(cloudprovider.ReservationIDLabel, corev1.NodeSelectorOpIn, reservation.ID),
				scheduling.NewRequirement(v1.LabelCapacityReservationType, corev1.NodeSelectorOpIn, string(reservation.ReservationType)),
			),
			Price:               price,
			Available:           reservationCapacity != 0 && itZones.Has(reservation.AvailabilityZone) && reservation.State != v1.CapacityReservationStateExpiring,
			ReservationCapacity: reservationCapacity,
		}
		if id, ok := subnetZonesToZoneIDs[reservation.AvailabilityZone]; ok {
			offering.Requirements.Add(scheduling.NewRequirement(v1.LabelTopologyZoneID, corev1.NodeSelectorOpIn, id))
		}
		offerings = append(offerings, offering)
	}

	// Add offerings for any matching reserved instances.
	if ris, err := p.reservedInstanceProvider.GetReservedInstances(ctx); err == nil {
		for _, ri := range ris {
			if ec2types.InstanceType(ri.InstanceType) != ec2types.InstanceType(it.Name) {
				continue
			}
			price := 0.0
			if odPrice, ok := p.pricingProvider.OnDemandPrice(ec2types.InstanceType(it.Name)); ok {
				price = odPrice / 10_000_000.0
			}
			offering := &cloudprovider.Offering{
				Requirements: scheduling.NewRequirements(
					scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeReserved),
					scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, ri.AvailabilityZone),
				),
				Price:               price,
				Available:           ri.InstanceCount > 0 && itZones.Has(ri.AvailabilityZone) && ri.State == ec2types.ReservedInstanceStateActive,
				ReservationCapacity: int(ri.InstanceCount),
			}
			if id, ok := subnetZonesToZoneIDs[ri.AvailabilityZone]; ok {
				offering.Requirements.Add(scheduling.NewRequirement(v1.LabelTopologyZoneID, corev1.NodeSelectorOpIn, id))
			}
			offerings = append(offerings, offering)
		}
	}
	return offerings
}

// cacheKeyFromInstanceType generates a cache key for an instance type.
// The key is based on the instance type name, the availability zones, and the capacity types.
// This ensures that we have different cache entries for the same instance type if its
// requirements (like zones or capacity types) are different.
func (p *DefaultProvider) cacheKeyFromInstanceType(it *cloudprovider.InstanceType) string {
	// `hashstructure` is a library that generates a hash value for a Go struct.
	// We use it here to create a consistent hash of the zone and capacity type requirements.
	zonesHash, _ := hashstructure.Hash(
		it.Requirements.Get(corev1.LabelTopologyZone).Values(),
		hashstructure.FormatV2,
		&hashstructure.HashOptions{SlicesAsSets: true},
	)
	capacityTypesHash, _ := hashstructure.Hash(
		it.Requirements.Get(karpv1.CapacityTypeLabelKey).Values(),
		hashstructure.FormatV2,
		&hashstructure.HashOptions{SlicesAsSets: true},
	)
	return fmt.Sprintf(
		"%s-%016x-%016x",
		it.Name,
		zonesHash,
		capacityTypesHash,
	)
}
