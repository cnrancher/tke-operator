package utils

import (
	"encoding/json"
	"fmt"
	tkev1 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
	asapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	cvmapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
	tkeapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tke/v20180525"
	"sort"
	"strings"
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
		VpcId:                &autoScalingGroupPara.VpcId,
		SubnetIds:            ParseStrings(autoScalingGroupPara.SubnetIds),
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
			KeyIds: ParseStrings(launchConfigurePara.KeyIds),
		},
		SecurityGroupIds:   ParseStrings(launchConfigurePara.SecurityGroupIds),
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
