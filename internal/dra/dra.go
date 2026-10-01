/*
Copyright 2026 The Modelplane Authors.

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

// Package dra indexes the devices Dynamic Resource Allocation (DRA) drivers
// publish, and the devices ResourceClaims have allocated.
package dra

import (
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/utils/ptr"
)

// A DeviceID identifies a device. A device's name is unique within its
// driver's pool.
type DeviceID struct {
	Driver string
	Pool   string
	Device string
}

// A Device is a device a DRA driver currently publishes.
type Device struct {
	ID DeviceID

	// NodeName is the node the device is attached to. It's empty for a device
	// that isn't attached to one node, like a network-attached device.
	NodeName string

	// Spec is the device as its driver published it.
	Spec resourcev1.Device
}

// Devices returns the devices the supplied ResourceSlices currently publish.
// A driver can spread a pool over several slices, each stamped with the
// pool's generation. Devices only reads slices of a pool's newest generation,
// because older ones are being replaced. Unlike the scheduler, it reads a pool
// whose newest generation isn't yet complete.
func Devices(slices []resourcev1.ResourceSlice) map[DeviceID]Device {
	type pool struct{ driver, name string }

	newest := map[pool]int64{}
	for _, s := range slices {
		p := pool{driver: s.Spec.Driver, name: s.Spec.Pool.Name}
		newest[p] = max(newest[p], s.Spec.Pool.Generation)
	}

	devices := map[DeviceID]Device{}
	for _, s := range slices {
		if s.Spec.Pool.Generation < newest[pool{driver: s.Spec.Driver, name: s.Spec.Pool.Name}] {
			continue
		}
		for _, d := range s.Spec.Devices {
			id := DeviceID{Driver: s.Spec.Driver, Pool: s.Spec.Pool.Name, Device: d.Name}
			devices[id] = Device{
				ID: id,
				// A slice either names the node all its devices are
				// attached to, or has each device name its own.
				NodeName: ptr.Deref(d.NodeName, ptr.Deref(s.Spec.NodeName, "")),
				Spec:     d,
			}
		}
	}
	return devices
}

// Allocations returns the devices a ResourceClaim has allocated. A device
// allocated for admin access remains free for other claims, so Allocations
// omits it.
func Allocations(c *resourcev1.ResourceClaim) []DeviceID {
	if c.Status.Allocation == nil {
		return nil
	}
	ids := make([]DeviceID, 0, len(c.Status.Allocation.Devices.Results))
	for _, r := range c.Status.Allocation.Devices.Results {
		if ptr.Deref(r.AdminAccess, false) {
			continue
		}
		ids = append(ids, DeviceID{Driver: r.Driver, Pool: r.Pool, Device: r.Device})
	}
	return ids
}

// Tainted returns true if the device has a taint that keeps new claims from
// allocating it.
func (d Device) Tainted() bool {
	for _, t := range d.Spec.Taints {
		if t.Effect == resourcev1.DeviceTaintEffectNoSchedule || t.Effect == resourcev1.DeviceTaintEffectNoExecute {
			return true
		}
	}
	return false
}

// StringAttribute returns the value of the device's string attribute with the
// supplied name. A driver may publish an attribute in its own domain either
// bare or qualified with the driver's name, so StringAttribute checks both.
func (d Device) StringAttribute(name string) (string, bool) {
	for _, n := range []resourcev1.QualifiedName{
		resourcev1.QualifiedName(name),
		resourcev1.QualifiedName(d.ID.Driver + "/" + name),
	} {
		if a, ok := d.Spec.Attributes[n]; ok && a.StringValue != nil {
			return *a.StringValue, true
		}
	}
	return "", false
}
