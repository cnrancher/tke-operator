/*
Copyright 2017 The Kubernetes Authors.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type TKEClusterConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TKEClusterConfigSpec   `json:"spec"`
	Status TKEClusterConfigStatus `json:"status"`
}

// TKEClusterConfigSpec is the spec for a TKEClusterConfig resource
type TKEClusterConfigSpec struct {
	TKECredentialSecret     string                   `json:"tkeCredentialSecret,omitempty"`
	Imported                bool                     `json:"imported"`
	Region                  string                   `json:"region,omitempty"`
	ClusterId               string                   `json:"clusterId,omitempty"`
	ClusterEndpoint         *ClusterEndpoint         `json:"clusterEndpoint,omitempty"`
	ClusterBasicSettings    *ClusterBasicSettings    `json:"clusterBasicSettings,omitempty"`
	ClusterCIDRSettings     *ClusterCIDRSettings     `json:"clusterCIDRSettings,omitempty"`
	ClusterAdvancedSettings *ClusterAdvancedSettings `json:"clusterAdvancedSettings,omitempty"`
	ExtensionAddon          []ExtensionAddon         `json:"extensionAddon,omitempty"`
	RunInstancesForNode     *RunInstancesForNode     `json:"runInstancesForNode,omitempty"`
	NodePoolList            []NodePoolDetail         `json:"nodePoolList,omitempty"`
}

type ExtensionAddon struct {
	AddonName  string `json:"addonName,omitempty"`
	AddonParam string `json:"addonParam,omitempty"`
}

type ClusterEndpoint struct {
	Enable              bool   `json:"enable,omitempty"`
	Domain              string `json:"domain,omitempty"`
	SubnetId            string `json:"subnetId,omitempty"`
	ExtensiveParameters string `json:"extensiveParameters,omitempty"`
	SecurityGroup       string `json:"securityGroup,omitempty"`
}

type NodePoolDetail struct {
	ClusterId            string               `json:"clusterId,omitempty"`
	NodePoolId           string               `json:"nodePoolId,omitempty"`
	AutoScalingGroupPara AutoScalingGroupPara `json:"autoScalingGroupPara,omitempty"`
	LaunchConfigurePara  LaunchConfigurePara  `json:"launchConfigurePara,omitempty"`
	EnableAutoscale      bool                 `json:"enableAutoscale,omitempty"`
	Name                 string               `json:"name,omitempty"`
	Labels               []string             `json:"labels,omitempty"`
	Taints               []string             `json:"taints,omitempty"`
	NodePoolOs           string               `json:"nodePoolOs,omitempty"`
	OsCustomizeType      string               `json:"osCustomizeType,omitempty"`
	Tags                 []string             `json:"tags,omitempty"`
	DeletionProtection   bool                 `json:"deletionProtection,omitempty"`
}

type AutoScalingGroupPara struct {
	AutoScalingGroupName string   `json:"autoScalingGroupName,omitempty"`
	MaxSize              int64    `json:"maxSize,omitempty"`
	MinSize              int64    `json:"minSize,omitempty"`
	DesiredCapacity      int64    `json:"desiredCapacity,omitempty"`
	VpcId                string   `json:"vpcId,omitempty"`
	SubnetIds            []string `json:"subnetIds,omitempty"`
}

type LaunchConfigurePara struct {
	LaunchConfigurationName string     `json:"launchConfigurationName,omitempty"`
	InstanceType            string     `json:"instanceType,omitempty"`
	SystemDisk              DataDisk   `json:"systemDisk,omitempty"`
	InternetChargeType      string     `json:"internetChargeType,omitempty"`
	InternetMaxBandwidthOut int64      `json:"internetMaxBandwidthOut,omitempty"`
	PublicIpAssigned        bool       `json:"publicIpAssigned,omitempty"`
	DataDisks               []DataDisk `json:"dataDisks,omitempty"`
	KeyIds                  []string   `json:"keyIds,omitempty"`
	SecurityGroupIds        []string   `json:"securityGroupIds,omitempty"`
	InstanceChargeType      string     `json:"instanceChargeType,omitempty"`
}

type TKEClusterConfigStatus struct {
	Phase          string `json:"phase"`
	FailureMessage string `json:"failureMessage"`
}

type ClusterBasicSettings struct {
	ClusterType        string   `json:"clusterType,omitempty"`
	ClusterOs          string   `json:"clusterOs,omitempty"`
	ClusterVersion     string   `json:"clusterVersion,omitempty"`
	ClusterName        string   `json:"clusterName,omitempty"`
	ClusterDescription string   `json:"clusterDescription,omitempty"`
	VpcId              string   `json:"vpcId,omitempty"`
	ProjectId          int64    `json:"projectId,omitempty"`
	Tags               []string `json:"tags,omitempty"`
	ClusterLevel       string   `json:"clusterLevel,omitempty"`
	IsAutoUpgrade      bool     `json:"isAutoUpgrade,omitempty"`
}

type ClusterCIDRSettings struct {
	ClusterCIDR               string   `json:"clusterCIDR,omitempty"`
	IgnoreClusterCIDRConflict bool     `json:"ignoreClusterCIDRConflict,omitempty"`
	MaxNodePodNum             int64    `json:"maxNodePodNum,omitempty"`
	MaxClusterServiceNum      int64    `json:"maxClusterServiceNum,omitempty"`
	ServiceCIDR               string   `json:"serviceCIDR,omitempty"`
	EniSubnetIds              []string `json:"eniSubnetIds,omitempty"`
	ClaimExpiredSeconds       int64    `json:"claimExpiredSeconds,omitempty"`
	IgnoreServiceCIDRConflict bool     `json:"ignoreServiceCIDRConflict,omitempty"`
	OsCustomizeType           string   `json:"osCustomizeType,omitempty"`
	SubnetId                  string   `json:"subnetId,omitempty"`
}

type ClusterAdvancedSettings struct {
	IPVS                    bool     `json:"ipvs,omitempty"`
	AsEnabled               bool     `json:"asEnabled,omitempty"`
	ContainerRuntime        string   `json:"containerRuntime,omitempty"`
	NodeNameType            string   `json:"nodeNameType,omitempty"`
	KubeAPIServer           []string `json:"kubeAPIServer,omitempty"`
	KubeControllerManager   []string `json:"kubeControllerManager,omitempty"`
	KubeScheduler           []string `json:"kubeScheduler,omitempty"`
	Etcd                    []string `json:"etcd,omitempty"`
	NetworkType             string   `json:"networkType,omitempty"`
	IsNonStaticIpMode       bool     `json:"isNonStaticIpMode,omitempty"`
	DeletionProtection      bool     `json:"deletionProtection,omitempty"`
	KubeProxyMode           string   `json:"kubeProxyMode,omitempty"`
	AuditEnabled            bool     `json:"auditEnabled,omitempty"`
	AuditLogsetId           string   `json:"auditLogsetId,omitempty"`
	AuditLogTopicId         string   `json:"auditLogTopicId,omitempty"`
	VpcCniType              string   `json:"vpcCniType,omitempty"`
	RuntimeVersion          string   `json:"runtimeVersion,omitempty"`
	EnableCustomizedPodCIDR bool     `json:"enableCustomizedPodCIDR,omitempty"`
	BasePodNumber           int64    `json:"basePodNumber,omitempty"`
	CiliumMode              string   `json:"ciliumMode,omitempty"`
	IsDualStack             bool     `json:"isDualStack,omitempty"`
	QGPUShareEnable         bool     `json:"qgpuShareEnable,omitempty"`
}

type RunInstancesForNode struct {
	NodeRole                string   `json:"nodeRole,omitempty"`
	InstanceChargeType      string   `json:"instanceChargeType,omitempty"`
	Zone                    string   `json:"zone,omitempty"`
	InstanceCount           int64    `json:"instanceCount,omitempty"`
	ProjectId               int64    `json:"projectId,omitempty"`
	InstanceType            string   `json:"instanceType,omitempty"`
	ImageId                 string   `json:"imageId,omitempty"`
	SystemDisk              DataDisk `json:"systemDisk,omitempty"`
	VpcId                   string   `json:"vpcId,omitempty"`
	SubnetId                string   `json:"subnetId,omitempty"`
	InternetChargeType      string   `json:"internetChargeType,omitempty"`
	InternetMaxBandwidthOut int64    `json:"internetMaxBandwidthOut,omitempty"`
	PublicIpAssigned        bool     `json:"publicIpAssigned,omitempty"`
	InstanceName            string   `json:"instanceName,omitempty"`
	KeyIds                  []string `json:"keyIds,omitempty"`
	SecurityService         bool     `json:"securityService,omitempty"`
	MonitorService          bool     `json:"monitorService,omitempty"`
	UserData                string   `json:"userData,omitempty"`
}

type DataDisk struct {
	DiskSize int64  `json:"diskSize,omitempty"`
	DiskType string `json:"diskType,omitempty"`
}
