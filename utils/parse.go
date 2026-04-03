package utils

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	tkev1 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
	asapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	cvmapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
	tkeapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tke/v20180525"
)

var (
	ResourceTypeCluster  = "cluster"
	ResourceTypeInstance = "instance"
)

func Parse(ref string) (namespace string, name string) {
	parts := strings.SplitN(ref, ":", 2)
	if len(parts) == 1 {
		return "", parts[0]
	}
	return parts[0], parts[1]
}

func ParseLabelsString(labels []*tkeapi.Label) []string {
	var expectStrings []string
	for _, label := range labels {
		expectString := fmt.Sprintf("%s=%s", *label.Name, *label.Value)
		expectStrings = append(expectStrings, expectString)
	}
	return expectStrings
}

func ParseStringLabels(labels []string) []*tkeapi.Label {
	var expectLabels []*tkeapi.Label
	for _, label := range labels {
		parts := strings.SplitN(label, "=", 2)
		if len(parts) > 1 {
			expectLabels = append(expectLabels, &tkeapi.Label{
				Name:  &parts[0],
				Value: &parts[1],
			})
		}
	}

	return expectLabels
}

func ParseTaintsString(taints []*tkeapi.Taint) []string {
	var expectStrings []string
	for _, taint := range taints {
		expectString := fmt.Sprintf("%s=%s", *taint.Key, *taint.Value)
		expectStrings = append(expectStrings, expectString)
	}
	return expectStrings
}

func ParseStringTaints(taints []string) []*tkeapi.Taint {
	var expectTaints []*tkeapi.Taint
	for _, taint := range taints {
		parts := strings.SplitN(taint, "=", 2)
		if len(parts) > 1 {
			expectTaints = append(expectTaints, &tkeapi.Taint{
				Key:   &parts[0],
				Value: &parts[1],
			})
		}
	}

	return expectTaints
}

func ParseTagsString(tags []*tkeapi.Tag) []string {
	var expectStrings []string
	for _, tag := range tags {
		expectString := fmt.Sprintf("%s=%s", *tag.Key, *tag.Value)
		expectStrings = append(expectStrings, expectString)
	}
	return expectStrings
}

func ParseStringTags(tags []string) []*tkeapi.Tag {
	var expectTags []*tkeapi.Tag
	for _, tag := range tags {
		parts := strings.SplitN(tag, "=", 2)
		if len(parts) > 1 {
			expectTags = append(expectTags, &tkeapi.Tag{
				Key:   &parts[0],
				Value: &parts[1],
			})
		}
	}

	return expectTags
}

func ParseSystemDiskTo(systemDisk *asapi.SystemDisk) tkev1.DataDisk {
	return tkev1.DataDisk{
		DiskSize: ParseUint64ToInt64(systemDisk.DiskSize),
		DiskType: *systemDisk.DiskType,
	}
}

func ParseToSystemDisk(systemDisk tkev1.DataDisk) *asapi.SystemDisk {
	return &asapi.SystemDisk{
		DiskSize: ParseInt64ToUint64(&systemDisk.DiskSize),
		DiskType: &systemDisk.DiskType,
	}
}

func ParseDataDisksTo(dataDisks []*asapi.DataDisk) []tkev1.DataDisk {
	var expectDisks []tkev1.DataDisk
	for _, dataDisk := range dataDisks {
		expectDisk := tkev1.DataDisk{
			DiskSize: ParseUint64ToInt64(dataDisk.DiskSize),
			DiskType: *dataDisk.DiskType,
		}
		expectDisks = append(expectDisks, expectDisk)
	}
	return expectDisks
}

func ParseToDataDisks(dataDisks []tkev1.DataDisk) []*asapi.DataDisk {
	var expectDisks []*asapi.DataDisk
	for _, dataDisk := range dataDisks {
		expectDisk := &asapi.DataDisk{
			DiskSize: ParseInt64ToUint64(&dataDisk.DiskSize),
			DiskType: &dataDisk.DiskType,
		}
		expectDisks = append(expectDisks, expectDisk)
	}
	return expectDisks
}

func ParseTagSpecificationTo(tagSpecifications []*tkeapi.TagSpecification) []string {
	var expectTags []string
	for _, tagSpecification := range tagSpecifications {
		if *tagSpecification.ResourceType == ResourceTypeCluster {
			for _, tag := range tagSpecification.Tags {
				expectTag := fmt.Sprintf("%s=%s", *tag.Key, *tag.Value)
				expectTags = append(expectTags, expectTag)
			}
		}
	}

	return expectTags
}

func ParseToTagSpecification(tags []string) []*tkeapi.TagSpecification {
	var expectTagSpecification []*tkeapi.TagSpecification
	if len(expectTagSpecification) > 0 {
		expectTagSpecification[0].ResourceType = &ResourceTypeCluster

		for _, tag := range tags {
			parts := strings.SplitN(tag, "=", 2)
			if len(parts) > 1 {
				expectTagSpecification[0].Tags = append(expectTagSpecification[0].Tags,
					&tkeapi.Tag{
						Key:   &parts[0],
						Value: &parts[1],
					},
				)
			}
		}
	}

	return expectTagSpecification
}

func ParseAutoScalingGroupPara(autoScalingGroupPara tkev1.AutoScalingGroupPara) (string, error) {
	autoScalingGroupRequest := &asapi.CreateAutoScalingGroupRequest{
		AutoScalingGroupName: &autoScalingGroupPara.AutoScalingGroupName,
		MaxSize:              ParseInt64ToUint64(&autoScalingGroupPara.MaxSize),
		MinSize:              ParseInt64ToUint64(&autoScalingGroupPara.MinSize),
		DesiredCapacity:      ParseInt64ToUint64(&autoScalingGroupPara.DesiredCapacity),
		VpcId:                &autoScalingGroupPara.VpcID,
		SubnetIds:            ParseStrings(autoScalingGroupPara.SubnetIDs),
	}

	data, err := json.Marshal(autoScalingGroupRequest)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func ParseLaunchConfigurePara(launchConfigurePara tkev1.LaunchConfigurePara) (string, error) {
	autoScalingGroupRequest := &asapi.CreateLaunchConfigurationRequestParams{
		LaunchConfigurationName: &launchConfigurePara.LaunchConfigurationName,
		InstanceType:            &launchConfigurePara.InstanceType,
		SystemDisk:              ParseToSystemDisk(launchConfigurePara.SystemDisk),
		InternetAccessible: &asapi.InternetAccessible{
			InternetChargeType:      &launchConfigurePara.InternetChargeType,
			InternetMaxBandwidthOut: ParseInt64ToUint64(&launchConfigurePara.InternetMaxBandwidthOut),
			PublicIpAssigned:        &launchConfigurePara.PublicIpAssigned,
		},
		DataDisks: ParseToDataDisks(launchConfigurePara.DataDisks),
		LoginSettings: &asapi.LoginSettings{
			KeyIds: ParseStrings(launchConfigurePara.KeyIDs),
		},
		SecurityGroupIds:   ParseStrings(launchConfigurePara.SecurityGroupIDs),
		InstanceChargeType: &launchConfigurePara.InstanceChargeType,
	}

	data, err := json.Marshal(autoScalingGroupRequest)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func StringValue(a *string) string {
	if a == nil {
		return ""
	}
	return *a
}

func ValueString(a string) *string {
	return &a
}

func Uint64Value(a *uint64) uint64 {
	if a == nil {
		return 0
	}
	return *a
}

func int64Value(a *int64) int64 {
	if a == nil {
		return 0
	}
	return *a
}

func ParseUint64ToInt64(a *uint64) int64 {
	return int64(Uint64Value(a))
}

func ParseInt64ToUint64(a *int64) *uint64 {
	b := uint64(int64Value(a))

	return &b
}

func ParseStringsPointer(arr []*string) []string {
	var expectStrings []string
	for _, a := range arr {
		expectStrings = append(expectStrings, *a)
	}

	sort.Strings(expectStrings)
	return expectStrings
}

func ParseStrings(arr []string) []*string {
	var expectStrings []*string
	for _, a := range arr {
		expectStrings = append(expectStrings, &a)
	}

	return expectStrings
}

func ParseToSystemDiskInstance(systemDisk tkev1.DataDisk) *cvmapi.SystemDisk {
	return &cvmapi.SystemDisk{
		DiskSize: &systemDisk.DiskSize,
		DiskType: &systemDisk.DiskType,
	}
}

// VirtualNodePoolModifyFields holds the subset of VirtualNodePoolDetail that
// ModifyClusterVirtualNodePool accepts. Each field is a pointer so that nil means
// “do not send this key” (unchanged in cloud). Slices use pointer-to-slice so an
// empty slice can mean “clear labels/taints/SGs” when that slice pointer is non-nil.
type VirtualNodePoolModifyFields struct {
	SecurityGroupIDs   *[]string
	Labels             *[]tkev1.VirtualNodeLabel
	Taints             *[]tkev1.VirtualNodeTaint
	DeletionProtection *bool
}

// Empty reports whether there is nothing to send to Modify.
func (f *VirtualNodePoolModifyFields) Empty() bool {
	if f == nil {
		return true
	}
	return f.SecurityGroupIDs == nil && f.Labels == nil && f.Taints == nil && f.DeletionProtection == nil
}

// DiffVirtualNodePoolModifyFields compares desired spec to upstream Describe for the only fields
// supported by ModifyClusterVirtualNodePool: SecurityGroupIds, Labels, Taints, DeletionProtection.
// Returns nil when no Modify is needed. If desired.DeletionProtection is nil, deletion
// protection is not compared (leave cloud as-is).
func DiffVirtualNodePoolModifyFields(desired, upstream tkev1.VirtualNodePoolDetail) *VirtualNodePoolModifyFields {
	var f VirtualNodePoolModifyFields
	if !stringSliceEqualUnordered(desired.SecurityGroupIDs, upstream.SecurityGroupIDs) {
		v := append([]string(nil), desired.SecurityGroupIDs...)
		f.SecurityGroupIDs = &v
	}
	if !virtualNodeLabelsEqual(desired.Labels, upstream.Labels) {
		v := append([]tkev1.VirtualNodeLabel(nil), desired.Labels...)
		f.Labels = &v
	}
	if !virtualNodeTaintsEqual(desired.Taints, upstream.Taints) {
		v := append([]tkev1.VirtualNodeTaint(nil), desired.Taints...)
		f.Taints = &v
	}
	if desired.DeletionProtection != nil {
		up := false
		if upstream.DeletionProtection != nil {
			up = *upstream.DeletionProtection
		}
		if *desired.DeletionProtection != up {
			v := *desired.DeletionProtection
			f.DeletionProtection = &v
		}
	}
	if f.Empty() {
		return nil
	}
	return &f
}

func stringSliceEqualUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func virtualNodeLabelsEqual(a, b []tkev1.VirtualNodeLabel) bool {
	if len(a) != len(b) {
		return false
	}
	sa := make([]string, len(a))
	sb := make([]string, len(b))
	for i, l := range a {
		sa[i] = l.Name + "=" + l.Value
	}
	for i, l := range b {
		sb[i] = l.Name + "=" + l.Value
	}
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

func virtualNodeTaintsEqual(a, b []tkev1.VirtualNodeTaint) bool {
	if len(a) != len(b) {
		return false
	}
	sa := make([]string, len(a))
	sb := make([]string, len(b))
	for i, t := range a {
		sa[i] = t.Key + ":" + t.Value + ":" + t.Effect
	}
	for i, t := range b {
		sb[i] = t.Key + ":" + t.Value + ":" + t.Effect
	}
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}
