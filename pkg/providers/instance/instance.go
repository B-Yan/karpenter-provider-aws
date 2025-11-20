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

package instance

// This package provides an abstraction around EC2 instances (virtual machines).
// If you're coming from Java or Python, think of this package as a Data Access Object (DAO)
// or a repository layer for interacting with AWS EC2 instances. It handles the lifecycle
// of instances (creating, getting, listing, deleting) and encapsulates the AWS SDK calls.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/awslabs/operatorpkg/aws/middleware"
	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/serrors"
	"sigs.k8s.io/karpenter/pkg/events"

	sdk "github.com/aws/karpenter-provider-aws/pkg/aws"
	"github.com/aws/karpenter-provider-aws/pkg/utils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/apis/v1alpha1"

	v1 "github.com/aws/karpenter-provider-aws/pkg/apis/v1"
	"github.com/aws/karpenter-provider-aws/pkg/batcher"
	awscache "github.com/aws/karpenter-provider-aws/pkg/cache"
	awserrors "github.com/aws/karpenter-provider-aws/pkg/errors"
	karpopts "github.com/aws/karpenter-provider-aws/pkg/operator/options"
	"github.com/aws/karpenter-provider-aws/pkg/providers/capacityreservation"
	instancefilter "github.com/aws/karpenter-provider-aws/pkg/providers/instance/filter"
	"github.com/aws/karpenter-provider-aws/pkg/providers/launchtemplate"
	"github.com/aws/karpenter-provider-aws/pkg/providers/reservedinstance"
	"github.com/aws/karpenter-provider-aws/pkg/providers/subnet"

	"github.com/patrickmn/go-cache"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

const (
	// falling back to on-demand without flexibility risks insufficient capacity errors
	instanceTypeFlexibilityThreshold = 5
	// The maximum number of instance types to include in a Create request
	maxInstanceTypes = 60
)

var (
	instanceStateFilter = ec2types.Filter{
		Name: aws.String("instance-state-name"),
		Values: []string{
			string(ec2types.InstanceStateNamePending),
			string(ec2types.InstanceStateNameRunning),
			string(ec2types.InstanceStateNameStopping),
			string(ec2types.InstanceStateNameStopped),
			string(ec2types.InstanceStateNameShuttingDown),
		},
	}
)

// Provider is an interface for managing EC2 instances. In Go, interfaces are defined
// implicitly. Any type that implements all the methods of an interface is considered
// to have implemented that interface. This is different from Java where you explicitly
// use the `implements` keyword.
type Provider interface {
	// Create launches a new EC2 instance based on the provided specifications.
	Create(context.Context, *v1.EC2NodeClass, *karpv1.NodeClaim, map[string]string, []*cloudprovider.InstanceType) (*Instance, error)
	// Get retrieves an existing EC2 instance by its ID.
	Get(context.Context, string, ...Options) (*Instance, error)
	// List retrieves all EC2 instances managed by Karpenter.
	List(context.Context) ([]*Instance, error)
	// Delete terminates an EC2 instance by its ID.
	Delete(context.Context, string) error
	// CreateTags adds tags to an existing EC2 instance.
	CreateTags(context.Context, string, map[string]string) error
}

type options struct {
	SkipCache bool
}

type Options = option.Function[options]

var SkipCache = func(opts *options) {
	opts.SkipCache = true
}

// DefaultProvider is the concrete implementation of the Provider interface.
// It holds the necessary clients and caches to interact with AWS services.
// Think of this as a class in Java/Python that implements the Provider interface.
// The fields are dependencies that will be "injected" when a new DefaultProvider is created.
type DefaultProvider struct {
	region                      string
	recorder                    events.Recorder
	ec2api                      sdk.EC2API
	unavailableOfferings        *awscache.UnavailableOfferings
	subnetProvider              subnet.Provider
	launchTemplateProvider      launchtemplate.Provider
	ec2Batcher                  *batcher.EC2API
	capacityReservationProvider capacityreservation.Provider
	reservedInstanceProvider    reservedinstance.Provider
	instanceCache               *cache.Cache
}

// NewDefaultProvider is a constructor function for DefaultProvider. In Go, it's a convention
// to have a `New...` function that returns a pointer to a new instance of a struct.
// This is similar to a constructor in Java or `__init__` in Python.
func NewDefaultProvider(
	ctx context.Context,
	region string,
	recorder events.Recorder,
	ec2api sdk.EC2API,
	unavailableOfferings *awscache.UnavailableOfferings,
	subnetProvider subnet.Provider,
	launchTemplateProvider launchtemplate.Provider,
	capacityReservationProvider capacityreservation.Provider,
	reservedInstanceProvider reservedinstance.Provider,
	instanceCache *cache.Cache,
) *DefaultProvider {
	return &DefaultProvider{
		region:                      region,
		recorder:                    recorder,
		ec2api:                      ec2api,
		unavailableOfferings:        unavailableOfferings,
		subnetProvider:              subnetProvider,
		launchTemplateProvider:      launchTemplateProvider,
		ec2Batcher:                  batcher.EC2(ctx, ec2api),
		capacityReservationProvider: capacityReservationProvider,
		reservedInstanceProvider:    reservedInstanceProvider,
		instanceCache:               instanceCache,
	}
}

// Create launches a new EC2 instance based on the provided specifications.
// It's an asynchronous-like operation from the user's perspective, as it kicks off the
// instance creation in AWS.
//
// A key concept in Go is the `context.Context`. It's used to carry deadlines, cancellation
// signals, and other request-scoped values across API boundaries and between processes.
// It's good practice to accept it as the first argument in a function.
//
// Another Go feature is multiple return values. Here, it returns a pointer to an `Instance`
// and an `error`. If the error is `nil`, the operation was successful. This is the standard
// way Go handles errors, unlike exceptions in Java/Python.
func (p *DefaultProvider) Create(ctx context.Context, nodeClass *v1.EC2NodeClass, nodeClaim *karpv1.NodeClaim, tags map[string]string, instanceTypes []*cloudprovider.InstanceType) (*Instance, error) {
	// First, filter the available instance types based on various constraints.
	instanceTypes, err := p.filterInstanceTypes(ctx, instanceTypes, nodeClaim)
	if err != nil {
		return nil, err // Early return on error
	}
	// Determine the capacity type (e.g., on-demand, spot, reserved) for the launch.
	capacityType := getCapacityType(nodeClaim, instanceTypes)
	tenancyType := getTenancyType(nodeClaim)

	// Launch the instance using a Fleet request. This is an AWS feature that allows launching a fleet of instances.
	fleetInstance, err := p.launchInstance(ctx, nodeClass, nodeClaim, capacityType, instanceTypes, tags, tenancyType)
	if awserrors.IsLaunchTemplateNotFound(err) {
		// In Go, error handling is explicit. Here we check for a specific error type.
		// If the launch template was not found, it might be due to a cache inconsistency.
		// We retry the launch once to handle this case.
		fleetInstance, err = p.launchInstance(ctx, nodeClass, nodeClaim, capacityType, instanceTypes, tags, tenancyType)
	}
	if err != nil {
		return nil, err
	}

	var opts []NewInstanceFromFleetOpts
	if capacityType == karpv1.CapacityTypeReserved {
		launchedInstanceType := string(fleetInstance.InstanceType)
		launchedZone := *fleetInstance.LaunchTemplateAndOverrides.Overrides.AvailabilityZone

		// Determine if this was a Capacity Reservation (CR) or Reserved Instance (RI) launch by inspecting the offerings.
		isCapacityReservation := false
		for _, it := range instanceTypes {
			if it.Name == launchedInstanceType {
				for _, o := range it.Offerings {
					if o.Zone() == launchedZone && o.CapacityType() == karpv1.CapacityTypeReserved && o.ReservationID() != "" {
						isCapacityReservation = true
						break
					}
				}
			}
			if isCapacityReservation {
				break
			}
		}

		if isCapacityReservation {
			id, crt := p.getCapacityReservationDetailsForInstance(launchedInstanceType, launchedZone, instanceTypes)
			opts = append(opts, WithCapacityReservationDetails(id, crt))
		}
		// For any reserved launch, we decrement our internal RI counter. This aligns Karpenter's accounting with AWS's,
		// where an RI discount can be applied even when launching into a targeted Capacity Reservation.
		p.reservedInstanceProvider.MarkLaunched(ec2types.InstanceType(launchedInstanceType), launchedZone)
	}
	// Check if Elastic Fabric Adapter (EFA) is requested and add the corresponding option.
	if lo.Contains(lo.Keys(nodeClaim.Spec.Resources.Requests), v1.ResourceEFA) {
		opts = append(opts, WithEFAEnabled())
	}
	// Create a new Instance object from the fleet instance details.
	return NewInstanceFromFleet(
		fleetInstance,
		capacityType,
		tags,
		tenancyType,
		opts...,
	), nil
}

// Get retrieves an instance by its ID. It uses a cache to avoid unnecessary API calls.
// The `...Options` is a variadic parameter, similar to `*args` in Python. It allows passing
// zero or more `Options` functions.
func (p *DefaultProvider) Get(ctx context.Context, id string, opts ...Options) (*Instance, error) {
	// Resolve options to see if we should skip the cache.
	skipCache := option.Resolve(opts...).SkipCache
	if !skipCache {
		// The `if i, ok := ...; ok` is a common Go idiom. It declares and initializes `i` and `ok`,
		// then checks `ok` in the same line. `ok` is a boolean indicating if the key was found.
		if i, ok := p.instanceCache.Get(id); ok {
			// The `i.(*Instance)` is a type assertion. It checks if `i` is of type `*Instance`.
			// If not, it will panic. A safer way is `i, ok := i.(*Instance)`, which returns a boolean.
			return i.(*Instance), nil
		}
	}
	// DescribeInstances is a batched AWS API call to get instance details.
	out, err := p.ec2Batcher.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{id},
		Filters:     []ec2types.Filter{instanceStateFilter},
	})
	if awserrors.IsNotFound(err) {
		p.instanceCache.Delete(id)
		return nil, cloudprovider.NewNodeClaimNotFoundError(err)
	}
	if err != nil {
		// The `%w` verb in `fmt.Errorf` wraps the error, preserving its context.
		// This is useful for checking the underlying error type higher up the call stack.
		return nil, fmt.Errorf("failed to describe ec2 instances, %w", err)
	}
	instances, err := instancesFromOutput(ctx, out)
	if err != nil {
		return nil, fmt.Errorf("getting instances from output, %w", err)
	}
	if len(instances) != 1 {
		return nil, fmt.Errorf("expected a single instance, %w", err)
	}
	// Store the retrieved instance in the cache.
	p.instanceCache.SetDefault(id, instances[0])
	return instances[0], nil
}

// List retrieves all instances managed by Karpenter in the current cluster.
// It uses pagination to handle a large number of instances.
func (p *DefaultProvider) List(ctx context.Context) ([]*Instance, error) {
	var out = &ec2.DescribeInstancesOutput{}

	// The AWS SDK for Go provides paginators to automatically handle fetching
	// all pages of a paginated API response.
	paginator := ec2.NewDescribeInstancesPaginator(p.ec2api, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			// Filters are used to narrow down the results from the AWS API.
			// Here, we're looking for instances with specific tags that identify them as Karpenter-managed.
			{
				Name:   aws.String("tag-key"),
				Values: []string{v1.NodePoolTagKey},
			},
			{
				Name:   aws.String("tag-key"),
				Values: []string{v1.NodeClassTagKey},
			},
			{
				Name:   aws.String(fmt.Sprintf("tag:%s", v1.EKSClusterNameTagKey)),
				Values: []string{karpopts.FromContext(ctx).ClusterName},
			},
			instanceStateFilter,
		},
		// MaxResults for DescribeInstances is capped at 1000
		MaxResults: lo.ToPtr[int32](1000),
	})

	// Loop through all pages of the result.
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("describing ec2 instances, %w", err)
		}
		out.Reservations = append(out.Reservations, page.Reservations...)
	}
	instances, err := instancesFromOutput(ctx, out)
	// Update the cache with all the retrieved instances.
	for _, it := range instances {
		p.instanceCache.SetDefault(it.ID, it)
	}
	return instances, cloudprovider.IgnoreNodeClaimNotFoundError(err)
}

// Delete terminates an instance by its ID.
func (p *DefaultProvider) Delete(ctx context.Context, id string) error {
	// First, get the instance details to check its state and capacity type.
	out, err := p.Get(ctx, id, SkipCache)
	if err != nil {
		return err
	}
	// If this was a reserved launch, we mark the RI as terminated in our internal inventory to restore the count.
	// This is done for all reserved launches, including those that used a Capacity Reservation,
	// to align with AWS's billing model where an RI discount can apply to a CR instance.
	if out.capacityType == karpv1.CapacityTypeReserved {
		p.reservedInstanceProvider.MarkTerminated(ec2types.InstanceType(out.Type), out.Zone)
	}
	// To avoid unnecessary API calls and handle eventual consistency, we check if the instance is already
	// in the process of shutting down.
	// https://docs.aws.amazon.com/ec2/latest/devguide/eventual-consistency.html
	if out.State != ec2types.InstanceStateNameShuttingDown {
		// TerminateInstances is a batched API call.
		if _, err := p.ec2Batcher.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
			InstanceIds: []string{id},
		}); err != nil {
			return err
		}
	}
	return nil
}

// CreateTags adds tags to an instance. Tags are key-value pairs that help organize AWS resources.
func (p *DefaultProvider) CreateTags(ctx context.Context, id string, tags map[string]string) error {
	// The `lo` package is a utility library providing helpful functions for working with slices and maps,
	// similar to what you might find in Python's standard library or libraries like Lodash in JavaScript.
	// Here, `MapToSlice` converts a map to a slice of `ec2types.Tag`.
	ec2Tags := lo.MapToSlice(tags, func(key, value string) ec2types.Tag {
		return ec2types.Tag{Key: aws.String(key), Value: aws.String(value)}
	})
	if _, err := p.ec2api.CreateTags(ctx, &ec2.CreateTagsInput{
		Resources: []string{id},
		Tags:      ec2Tags,
	}); err != nil {
		if awserrors.IsNotFound(err) {
			return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("tagging instance, %w", err))
		}
		return fmt.Errorf("tagging instance, %w", err)
	}
	return nil
}

// filterInstanceTypes applies a series of filters to the list of instance types to narrow down the choices
// for launching an instance.
func (p *DefaultProvider) filterInstanceTypes(ctx context.Context, instanceTypes []*cloudprovider.InstanceType, nodeClaim *karpv1.NodeClaim) ([]*cloudprovider.InstanceType, error) {
	rejectedInstanceTypes := map[string][]*cloudprovider.InstanceType{}
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)

	// Go allows creating a slice of interfaces. Here, we define a series of filters to apply.
	for _, filter := range []instancefilter.Filter{
		instancefilter.CompatibleAvailableFilter(reqs, nodeClaim.Spec.Resources.Requests),
		instancefilter.CapacityReservationTypeFilter(reqs),
		instancefilter.CapacityBlockFilter(reqs),
		instancefilter.ReservedOfferingFilter(reqs),
		instancefilter.ExoticInstanceTypeFilter(reqs),
		instancefilter.SpotInstanceFilter(reqs),
	} {
		remaining, rejected := filter.FilterReject(instanceTypes)
		if len(remaining) == 0 {
			return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("all requested instance types were unavailable during launch"))
		}
		if len(rejected) != 0 && filter.Name() != "compatible-available-filter" {
			rejectedInstanceTypes[filter.Name()] = rejected
		}
		instanceTypes = remaining
	}
	// Log the instance types that were filtered out for debugging purposes.
	for filterName, its := range rejectedInstanceTypes {
		log.FromContext(ctx).WithValues("filter", filterName, "instance-types", utils.PrettySlice(lo.Map(its, func(i *cloudprovider.InstanceType, _ int) string { return i.Name }), 5)).V(1).Info("filtered out instance types from launch")
	}
	// Truncate the list of instance types to a maximum number to avoid overly large API requests.
	instanceTypes, err := cloudprovider.InstanceTypes(instanceTypes).Truncate(ctx, reqs, maxInstanceTypes)
	if err != nil {
		return nil, cloudprovider.NewCreateError(fmt.Errorf("truncating instance types, %w", err), "InstanceTypeFilteringFailed", "Error truncating instance types based on the passed-in requirements")
	}
	return instanceTypes, nil
}

//nolint:gocyclo
// launchInstance is responsible for the core logic of launching an instance via a CreateFleet request.
// It orchestrates getting subnets, launch templates, and constructing the final API request.
// The `//nolint:gocyclo` is a directive to the linter to ignore the cyclomatic complexity of this function.
// This is sometimes used for functions that have a lot of branching but are still readable.
//nolint:gocyclo
func (p *DefaultProvider) launchInstance(
	ctx context.Context,
	nodeClass *v1.EC2NodeClass,
	nodeClaim *karpv1.NodeClaim,
	capacityType string,
	instanceTypes []*cloudprovider.InstanceType,
	tags map[string]string,
	tenancyType string,
) (ec2types.CreateFleetInstance, error) {
	// Get subnets that are compatible with the selected instance types and capacity type.
	zonalSubnets, err := p.subnetProvider.ZonalSubnetsForLaunch(ctx, nodeClass, instanceTypes, capacityType)
	if err != nil {
		return ec2types.CreateFleetInstance{}, cloudprovider.NewCreateError(fmt.Errorf("getting subnets, %w", err), "SubnetResolutionFailed", "Error getting subnets")
	}

	// Get Launch Template Configs. A launch template is like a blueprint for an EC2 instance.
	// We may need multiple launch templates if the instance types have different requirements (e.g., GPU, architecture).
	launchTemplateConfigs, err := p.getLaunchTemplateConfigs(ctx, nodeClass, nodeClaim, instanceTypes, zonalSubnets, capacityType, tags, tenancyType)
	if err != nil {
		reason, message := awserrors.ToReasonMessage(err)
		return ec2types.CreateFleetInstance{}, cloudprovider.NewCreateError(fmt.Errorf("getting launch template configs, %w", err), reason, fmt.Sprintf("Error getting launch template configs: %s", message))
	}
	if err := p.checkODFallback(nodeClaim, instanceTypes, launchTemplateConfigs); err != nil {
		log.FromContext(ctx).Error(err, "failed while checking on-demand fallback")
	}

	// Use a builder pattern to construct the CreateFleetInput. This improves readability when there are many optional parameters.
	cfiBuilder := NewCreateFleetInputBuilder(capacityType, tags, launchTemplateConfigs)
	if _, ok := nodeClaim.Annotations[v1alpha1.PriceOverlayAppliedAnnotationKey]; ok {
		cfiBuilder.WithOverlay()
	}
	if nodeClass.Spec.Context != nil && nodeClaim.Annotations[karpv1.NodeClaimMinValuesRelaxedAnnotationKey] != "true" {
		cfiBuilder.WithContextID(*nodeClass.Spec.Context)
	}
	if capacityType == karpv1.CapacityTypeReserved {
		crt := getCapacityReservationType(instanceTypes)
		if crt == nil {
			// `panic` is used for unrecoverable errors. It stops the ordinary flow of control and begins panicking.
			// It's generally used when a program reaches an impossible state.
			panic(fmt.Sprintf("%s label isn't set for instance types in reserved launch", v1.LabelCapacityReservationType))
		}
		cfiBuilder.WithCapacityReservationType(*crt)
	}
	createFleetInput := cfiBuilder.Build()

	// Call the batched CreateFleet API.
	createFleetOutput, err := p.ec2Batcher.CreateFleet(ctx, createFleetInput)
	p.subnetProvider.UpdateInflightIPs(createFleetInput, createFleetOutput, instanceTypes, lo.Values(zonalSubnets), capacityType)
	if err != nil {
		reason, message := awserrors.ToReasonMessage(err)
		if awserrors.IsLaunchTemplateNotFound(err) {
			// If launch templates were not found, invalidate the cache for them and return an error.
			for _, lt := range launchTemplateConfigs {
				p.launchTemplateProvider.InvalidateCache(ctx, aws.ToString(lt.LaunchTemplateSpecification.LaunchTemplateName), aws.ToString(lt.LaunchTemplateSpecification.LaunchTemplateId))
			}
			return ec2types.CreateFleetInstance{}, cloudprovider.NewCreateError(fmt.Errorf("launch templates not found when creating fleet request, %w", err), reason, fmt.Sprintf("Launch templates not found when creating fleet request: %s", message))
		}
		return ec2types.CreateFleetInstance{}, cloudprovider.NewCreateError(fmt.Errorf("creating fleet request, %w", err), reason, fmt.Sprintf("Error creating fleet request: %s", message))
	}
	// After the fleet request, update our cache of unavailable offerings based on any errors returned.
	p.updateUnavailableOfferingsCache(ctx, createFleetOutput.Errors, capacityType, nodeClaim, instanceTypes)
	if len(createFleetOutput.Instances) == 0 || len(createFleetOutput.Instances[0].InstanceIds) == 0 {
		// If no instances were launched, combine the errors from the fleet request and return a single error.
		requestID, _ := awsmiddleware.GetRequestIDMetadata(createFleetOutput.ResultMetadata)
		return ec2types.CreateFleetInstance{}, serrors.Wrap(
			combineFleetErrors(createFleetOutput.Errors),
			middleware.AWSRequestIDLogKey, requestID,
			middleware.AWSOperationNameLogKey, "CreateFleet",
			middleware.AWSServiceNameLogKey, "EC2",
			middleware.AWSStatusCodeLogKey, 200,
			middleware.AWSErrorCodeLogKey, "UnfulfillableCapacity",
		)
	}
	// Return the details of the successfully launched instance.
	return createFleetOutput.Instances[0], nil
}

// checkODFallback checks if there is a risk of insufficient capacity when falling back from Spot to On-Demand.
// It warns the user if there are too few instance type options available.
func (p *DefaultProvider) checkODFallback(nodeClaim *karpv1.NodeClaim, instanceTypes []*cloudprovider.InstanceType, launchTemplateConfigs []ec2types.FleetLaunchTemplateConfigRequest) error {
	// Only evaluate for on-demand fallback if the capacity type for the request is OD and both OD and spot are allowed in requirements.
	if getCapacityType(nodeClaim, instanceTypes) != karpv1.CapacityTypeOnDemand || !scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...).Get(karpv1.CapacityTypeLabelKey).Has(karpv1.CapacityTypeSpot) {
		return nil
	}

	// Loop through the LT configs for currently considered instance types to get the flexibility count.
	instanceTypeZones := map[string]struct{}{}
	for _, ltc := range launchTemplateConfigs {
		for _, override := range ltc.Overrides {
			// `struct{}` is an empty struct. It's often used as a value in a map when you only care about the keys,
			// effectively creating a set. It has a size of zero, making it memory efficient.
			instanceTypeZones[string(override.InstanceType)] = struct{}{}
		}
	}
	if len(instanceTypes) < instanceTypeFlexibilityThreshold {
		return fmt.Errorf("at least %d instance types are recommended when flexible to spot but requesting on-demand, "+
			"the current provisioning request only has %d instance type options", instanceTypeFlexibilityThreshold, len(instanceTypes))
	}
	return nil
}

// getLaunchTemplateConfigs ensures that all necessary launch templates exist and returns their configurations.
func (p *DefaultProvider) getLaunchTemplateConfigs(
	ctx context.Context,
	nodeClass *v1.EC2NodeClass,
	nodeClaim *karpv1.NodeClaim,
	instanceTypes []*cloudprovider.InstanceType,
	zonalSubnets map[string]*subnet.Subnet,
	capacityType string,
	tags map[string]string,
	tenancyType string,
) ([]ec2types.FleetLaunchTemplateConfigRequest, error) {
	var launchTemplateConfigs []ec2types.FleetLaunchTemplateConfigRequest
	// Ensure all necessary launch templates are created or updated.
	launchTemplates, err := p.launchTemplateProvider.EnsureAll(ctx, nodeClass, nodeClaim, instanceTypes, capacityType, tags, tenancyType)
	if err != nil {
		return nil, fmt.Errorf("getting launch templates, %w", err)
	}
	requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
	requirements[karpv1.CapacityTypeLabelKey] = scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType)
	// For each launch template, create a FleetLaunchTemplateConfigRequest which includes overrides for each instance type.
	for _, launchTemplate := range launchTemplates {
		launchTemplateConfig := ec2types.FleetLaunchTemplateConfigRequest{
			Overrides: p.getOverrides(launchTemplate.InstanceTypes, zonalSubnets, requirements, launchTemplate.ImageID, launchTemplate.CapacityReservationID),
			LaunchTemplateSpecification: &ec2types.FleetLaunchTemplateSpecificationRequest{
				LaunchTemplateName: aws.String(launchTemplate.Name),
				Version:            aws.String("$Latest"), // Always use the latest version of the launch template.
			},
		}
		if len(launchTemplateConfig.Overrides) > 0 {
			launchTemplateConfigs = append(launchTemplateConfigs, launchTemplateConfig)
		}
	}
	if len(launchTemplateConfigs) == 0 {
		return nil, fmt.Errorf("no capacity offerings are currently available given the constraints")
	}
	return launchTemplateConfigs, nil
}

// getOverrides creates and returns launch template overrides for the cross product of InstanceTypes and subnets (with subnets being constrained by
// zones and the offerings in InstanceTypes)
func (p *DefaultProvider) getOverrides(
	instanceTypes []*cloudprovider.InstanceType,
	zonalSubnets map[string]*subnet.Subnet,
	reqs scheduling.Requirements,
	image, capacityReservationID string,
) []ec2types.FleetLaunchTemplateOverridesRequest {
	// Unwrap all the offerings to a flat slice that includes a pointer
	// to the parent instance type name
	type offeringWithParentName struct {
		*cloudprovider.Offering
		parentInstanceTypeName ec2types.InstanceType
	}
	var filteredOfferings []offeringWithParentName
	for _, it := range instanceTypes {
		ofs := it.Offerings.Available().Compatible(reqs)
		// If we are generating a launch template for a specific capacity reservation, we only want to include the offering
		// for that capacity reservation when generating overrides.
		if capacityReservationID != "" {
			ofs = ofs.Compatible(scheduling.NewRequirements(scheduling.NewRequirement(
				cloudprovider.ReservationIDLabel,
				corev1.NodeSelectorOpIn,
				capacityReservationID,
			)))
		}
		for _, o := range ofs {
			filteredOfferings = append(filteredOfferings, offeringWithParentName{
				Offering:               o,
				parentInstanceTypeName: ec2types.InstanceType(it.Name),
			})
		}
	}
	var overrides []ec2types.FleetLaunchTemplateOverridesRequest
	for _, offering := range filteredOfferings {
		subnet, ok := zonalSubnets[offering.Zone()]
		if !ok {
			continue
		}
		overrides = append(overrides, ec2types.FleetLaunchTemplateOverridesRequest{
			InstanceType: offering.parentInstanceTypeName,
			SubnetId:     lo.ToPtr(subnet.ID),
			ImageId:      lo.ToPtr(image),
			// This is technically redundant, but is useful if we have to parse insufficient capacity errors from
			// CreateFleet so that we can figure out the zone rather than additional API calls to look up the subnet
			AvailabilityZone: lo.ToPtr(subnet.Zone),
			// Priority settings for offerings take effect only when node overlays are defined.
			// Allocation strategies will be applied in the following order:
			// 1. On-demand instances (prioritized)
			// 2. Capacity-optimized Spot instances
			Priority: lo.ToPtr(float64(offering.Price)),
		})
	}
	return overrides
}

// updateUnavailableOfferingsCache is called after a CreateFleet call to update the cache of unavailable offerings
// based on the errors received. This helps avoid retrying to launch capacity that is known to be unavailable.
func (p *DefaultProvider) updateUnavailableOfferingsCache(
	ctx context.Context,
	errs []ec2types.CreateFleetError,
	capacityType string,
	nodeClaim *karpv1.NodeClaim,
	instanceTypes []*cloudprovider.InstanceType,
) {
	for _, err := range errs {
		zone := lo.FromPtr(err.LaunchTemplateAndOverrides.Overrides.AvailabilityZone)
		if awserrors.IsInsufficientFreeAddressesInSubnet(err) && zone != "" {
			p.unavailableOfferings.MarkAZUnavailable(zone)
		}
	}

	if capacityType != karpv1.CapacityTypeReserved {
		for _, err := range errs {
			if awserrors.IsUnfulfillableCapacity(err) {
				p.unavailableOfferings.MarkUnavailableForFleetErr(ctx, err, capacityType)
			}
			if awserrors.IsServiceLinkedRoleCreationNotPermitted(err) {
				p.unavailableOfferings.MarkCapacityTypeUnavailable(karpv1.CapacityTypeSpot)
				p.recorder.Publish(SpotServiceLinkedRoleCreationFailure(nodeClaim))
			}
		}
		return
	}

	reservationIDs := make([]string, 0, len(errs))
	for i := range errs {
		id, _ := p.getCapacityReservationDetailsForInstance(
			string(errs[i].LaunchTemplateAndOverrides.Overrides.InstanceType),
			lo.FromPtr(errs[i].LaunchTemplateAndOverrides.Overrides.AvailabilityZone),
			instanceTypes,
		)
		reservationIDs = append(reservationIDs, id)
		log.FromContext(ctx).WithValues(
			"reason", lo.FromPtr(errs[i].ErrorCode),
			"instance-type", errs[i].LaunchTemplateAndOverrides.Overrides.InstanceType,
			"zone", lo.FromPtr(errs[i].LaunchTemplateAndOverrides.Overrides.AvailabilityZone),
			"capacity-reservation-id", id,
		).V(1).Info("marking capacity reservation unavailable")
	}
	p.capacityReservationProvider.MarkUnavailable(reservationIDs...)
}

// getCapacityReservationDetailsForInstance finds the capacity reservation ID and type for a given instance type and zone.
func (p *DefaultProvider) getCapacityReservationDetailsForInstance(instance, zone string, instanceTypes []*cloudprovider.InstanceType) (id string, crt v1.CapacityReservationType) {
	for _, it := range instanceTypes {
		if it.Name != instance {
			continue
		}
		for _, o := range it.Offerings {
			if o.CapacityType() != karpv1.CapacityTypeReserved || o.Zone() != zone {
				continue
			}
			return o.ReservationID(), v1.CapacityReservationType(o.Requirements.Get(v1.LabelCapacityReservationType).Any())
		}
	}
	// This function should only be called when we know a capacity reservation exists for the given instance and zone.
	// If we can't find one, it indicates a logic error, so we panic.
	panic("reservation ID doesn't exist for reserved launch")
}

// getTenancyType selects the tenancy for the nodeclaim.
// If both default and dedicated are allowed by the claim then it will select default tenancy.
func getTenancyType(nodeClaim *karpv1.NodeClaim) string {
	requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)

	requirement := requirements.Get(v1.LabelInstanceTenancy)

	if requirement == nil {
		return string(ec2types.TenancyDefault)
	}

	for _, tenancyType := range []string{string(ec2types.TenancyDefault), string(ec2types.TenancyDedicated)} {
		if requirement.Has(tenancyType) {
			return tenancyType
		}
	}

	return string(ec2types.TenancyDefault)
}

// getCapacityType selects the capacity type based on the flexibility of the NodeClaim and the available offerings.
// Prioritization is as follows: reserved, spot, on-demand.
func getCapacityType(nodeClaim *karpv1.NodeClaim, instanceTypes []*cloudprovider.InstanceType) string {
	for _, capacityType := range []string{karpv1.CapacityTypeReserved, karpv1.CapacityTypeSpot} {
		requirements := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
		if !requirements.Get(karpv1.CapacityTypeLabelKey).Has(capacityType) {
			continue
		}
		requirements[karpv1.CapacityTypeLabelKey] = scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityType)
		for _, it := range instanceTypes {
			if len(it.Offerings.Available().Compatible(requirements)) != 0 {
				return capacityType
			}
		}
	}
	return karpv1.CapacityTypeOnDemand
}

func getCapacityReservationType(instanceTypes []*cloudprovider.InstanceType) *v1.CapacityReservationType {
	for _, it := range instanceTypes {
		for _, o := range it.Offerings {
			if o.Requirements.Has(v1.LabelCapacityReservationType) {
				return lo.ToPtr(v1.CapacityReservationType(o.Requirements.Get(v1.LabelCapacityReservationType).Any()))
			}
		}
	}
	return nil
}

// instancesFromOutput converts the output of a DescribeInstances API call to a slice of our internal `Instance` struct.
func instancesFromOutput(ctx context.Context, out *ec2.DescribeInstancesOutput) ([]*Instance, error) {
	if len(out.Reservations) == 0 {
		return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("instance not found"))
	}
	// The result of DescribeInstances is a list of reservations, each containing a list of instances.
	// We flatten this into a single slice of instances.
	instances := lo.Flatten(lo.Map(out.Reservations, func(r ec2types.Reservation, _ int) []ec2types.Instance {
		return r.Instances
	}))
	if len(instances) == 0 {
		return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("instance not found"))
	}
	// Sort the instances by ID to ensure a consistent order.
	sort.Slice(instances, func(i, j int) bool {
		return aws.ToString(instances[i].InstanceId) < aws.ToString(instances[j].InstanceId)
	})
	return lo.Map(instances, func(i ec2types.Instance, _ int) *Instance { return NewInstance(ctx, i) }), nil
}

// combineFleetErrors takes a slice of CreateFleetError and combines them into a single error.
// It de-duplicates the errors and wraps them in a higher-level error type (e.g., InsufficientCapacityError).
func combineFleetErrors(fleetErrs []ec2types.CreateFleetError) (errs error) {
	unique := sets.NewString()
	for _, err := range fleetErrs {
		unique.Insert(fmt.Sprintf("%s: %s", aws.ToString(err.ErrorCode), aws.ToString(err.ErrorMessage)))
	}
	// The `multierr` package is used to combine multiple errors into a single error.
	for errorCode := range unique {
		errs = multierr.Append(errs, errors.New(errorCode))
	}
	// If all the Fleet errors are Insufficient Capacity (ICE) errors, then we should wrap the combined error
	// in the generic ICE error type. This helps higher-level code to react appropriately.
	iceErrorCount := lo.CountBy(fleetErrs, func(err ec2types.CreateFleetError) bool {
		return awserrors.IsUnfulfillableCapacity(err) || awserrors.IsServiceLinkedRoleCreationNotPermitted(err)
	})
	if iceErrorCount == len(fleetErrs) {
		return cloudprovider.NewInsufficientCapacityError(fmt.Errorf("with fleet error(s), %w", errs))
	}
	reason, message := awserrors.ToReasonMessage(errs)
	return cloudprovider.NewCreateError(errs, reason, message)
}
