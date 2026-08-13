package client

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	tkev1 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
	"github.com/cnrancher/tke-operator/pkg/tkeapifull"
	"github.com/cnrancher/tke-operator/utils"
	"github.com/sirupsen/logrus"
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tcerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	tchttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cvmapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
	tkeapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tke/v20180525"
)

var (
	InstanceDeleteMode = "terminate"
	KeepInstance       = false
)

func normalizeUserScript(script string) string {
	if script == "" {
		return ""
	}
	if _, err := base64.StdEncoding.DecodeString(script); err == nil {
		return script
	}
	return base64.StdEncoding.EncodeToString([]byte(script))
}

type TKEClient struct {
	client *tkeapi.Client
	common *tccommon.Client // same credential/profile as client; used for CommonRequest full JSON responses
}

func GetTKEClient(credential *tccommon.Credential, region, language string) (*TKEClient, error) {
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "tke.tencentcloudapi.com"
	if language == "zh-CN" || language == "en-US" {
		cpf.Language = language
	}
	client, err := tkeapi.NewClient(credential, region, cpf)
	if err != nil {
		return nil, err
	}

	commonClient := tccommon.NewCommonClient(credential, region, cpf)
	return &TKEClient{client: client, common: commonClient}, nil
}

func (t TKEClient) GetCluster(clusterId string) (*tkeapi.Cluster, error) {
	logrus.Infof("client tke action: GetCluster")
	request := tkeapi.NewDescribeClustersRequest()
	request.ClusterIds = []*string{&clusterId}
	response, err := t.client.DescribeClusters(request)
	if err != nil {
		return nil, err
	}

	// DescribeClusters does not return FAILEDOPERATION_CLUSTERNOTFOUND when querying a
	// deleted cluster; it either returns nil Response or an empty Clusters list. Normalize
	// both cases to FAILEDOPERATION_CLUSTERNOTFOUND so all existing callers that already
	// check for that code work correctly without any extra changes.
	if response.Response == nil || len(response.Response.Clusters) == 0 {
		return nil, tcerrors.NewTencentCloudSDKError(tkeapi.FAILEDOPERATION_CLUSTERNOTFOUND, "cluster not found", "")
	}

	return response.Response.Clusters[0], nil
}

func (t TKEClient) GetClusters() (*tkeapi.DescribeClustersResponse, error) {
	logrus.Infof("client tke action: GetClusters")
	request := tkeapi.NewDescribeClustersRequest()
	response, err := t.client.DescribeClusters(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (t TKEClient) GetClusterStatus(clusterId *string) (*tkeapi.ClusterStatus, error) {
	logrus.Infof("client tke action: GetClusterStatus")
	request := tkeapi.NewDescribeClusterStatusRequest()
	request.ClusterIds = []*string{clusterId}
	response, err := t.client.DescribeClusterStatus(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || len(response.Response.ClusterStatusSet) == 0 {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.ClusterStatusSet[0], nil
}

func (t TKEClient) GetClusterNodePools(clusterId string) ([]*tkeapi.NodePool, error) {
	logrus.Infof("client tke action: GetClusterNodePools")
	request := tkeapi.NewDescribeClusterNodePoolsRequest()
	request.ClusterId = &clusterId
	response, err := t.client.DescribeClusterNodePools(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.NodePoolSet, nil
}

func (t TKEClient) CreateClusterNodePool(clusterId string, nodePool tkev1.NodePoolDetail) (*string, error) {
	logrus.Infof("client tke action: CreateClusterNodePool")
	autoScalingGroupPara, err := utils.ParseAutoScalingGroupPara(nodePool.AutoScalingGroupPara)
	if err != nil {
		return nil, err
	}

	launchConfigurePara, err := utils.ParseLaunchConfigurePara(nodePool.LaunchConfigurePara)
	if err != nil {
		return nil, err
	}

	request := tkeapi.NewCreateClusterNodePoolRequest()
	request.ClusterId = &clusterId
	request.AutoScalingGroupPara = &autoScalingGroupPara
	request.LaunchConfigurePara = &launchConfigurePara
	request.EnableAutoscale = &nodePool.EnableAutoscale
	request.Name = &nodePool.Name
	request.NodePoolOs = &nodePool.NodePoolOs
	request.OsCustomizeType = &nodePool.OsCustomizeType
	request.Tags = utils.ParseStringTags(nodePool.Tags)
	request.DeletionProtection = &nodePool.DeletionProtection
	advancedSettings := &tkeapi.InstanceAdvancedSettings{
		Labels: utils.ParseStringLabels(nodePool.Labels),
		Taints: utils.ParseStringTaints(nodePool.Taints),
	}
	if nodePool.UserScript != "" {
		normalized := normalizeUserScript(nodePool.UserScript)
		advancedSettings.UserScript = &normalized
	}
	request.InstanceAdvancedSettings = advancedSettings

	response, err := t.client.CreateClusterNodePool(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || response.Response.NodePoolId == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.NodePoolId, nil
}

func (t TKEClient) DeleteNodePool(clusterId string, nodePoolIds []*string) error {
	logrus.Infof("client tke action: DeleteNodePool")
	request := tkeapi.NewDeleteClusterNodePoolRequest()
	request.ClusterId = &clusterId
	request.NodePoolIds = nodePoolIds
	request.KeepInstance = &KeepInstance

	if _, err := t.client.DeleteClusterNodePool(request); err != nil {
		return err
	}

	return nil
}

func (t TKEClient) ModifyNodePoolInstanceTypes(clusterId, nodePoolId, instanceType string) error {
	logrus.Infof("client tke action: ModifyNodePoolInstanceTypes")
	request := tkeapi.NewModifyNodePoolInstanceTypesRequest()
	request.ClusterId = &clusterId
	request.NodePoolId = &nodePoolId
	request.InstanceTypes = []*string{&instanceType}

	if _, err := t.client.ModifyNodePoolInstanceTypes(request); err != nil {
		return err
	}

	return nil
}

func (t TKEClient) ModifyNodePoolDesiredCapacityAboutAsg(clusterId, nodePoolId string, DesiredCapacity int64) error {
	logrus.Infof("client tke action: ModifyNodePoolDesiredCapacityAboutAsg")
	request := tkeapi.NewModifyNodePoolDesiredCapacityAboutAsgRequest()
	request.ClusterId = &clusterId
	request.NodePoolId = &nodePoolId
	request.DesiredCapacity = &DesiredCapacity

	if _, err := t.client.ModifyNodePoolDesiredCapacityAboutAsg(request); err != nil {
		return err
	}

	return nil
}

func (t TKEClient) ModifyClusterNodePool(clusterId string, nodePool tkev1.NodePoolDetail) error {
	logrus.Infof("client tke action: ModifyClusterNodePool")
	request := tkeapi.NewModifyClusterNodePoolRequest()
	request.ClusterId = &clusterId
	request.NodePoolId = &nodePool.NodePoolID
	request.Name = &nodePool.Name
	request.MaxNodesNum = &nodePool.AutoScalingGroupPara.MaxSize
	request.MinNodesNum = &nodePool.AutoScalingGroupPara.MinSize
	request.Labels = utils.ParseStringLabels(nodePool.Labels)
	request.Taints = utils.ParseStringTaints(nodePool.Taints)
	request.EnableAutoscale = &nodePool.EnableAutoscale
	request.OsName = &nodePool.NodePoolOs
	request.OsCustomizeType = &nodePool.OsCustomizeType
	request.Tags = utils.ParseStringTags(nodePool.Tags)
	request.DeletionProtection = &nodePool.DeletionProtection
	if nodePool.UserScript != "" {
		normalized := normalizeUserScript(nodePool.UserScript)
		request.UserScript = &normalized
	}

	if _, err := t.client.ModifyClusterNodePool(request); err != nil {
		return err
	}

	return nil
}

func (t TKEClient) CreateCluster(spec tkev1.TKEClusterConfigSpec) (*string, error) {
	logrus.Infof("client tke action: CreateCluster")
	request := tkeapi.NewCreateClusterRequest()
	request.ClusterType = &spec.ClusterBasicSettings.ClusterType
	request.ClusterBasicSettings = &tkeapi.ClusterBasicSettings{
		ClusterOs:          &spec.ClusterBasicSettings.ClusterOs,
		ClusterVersion:     &spec.ClusterBasicSettings.ClusterVersion,
		ClusterName:        &spec.ClusterBasicSettings.ClusterName,
		ClusterDescription: &spec.ClusterBasicSettings.ClusterDescription,
		VpcId:              &spec.ClusterBasicSettings.VpcID,
		ProjectId:          &spec.ClusterBasicSettings.ProjectID,
		TagSpecification:   utils.ParseToTagSpecification(spec.ClusterBasicSettings.Tags),
		OsCustomizeType:    &spec.ClusterCIDRSettings.OsCustomizeType,
		SubnetId:           &spec.ClusterCIDRSettings.SubnetID,
		ClusterLevel:       &spec.ClusterBasicSettings.ClusterLevel,
		AutoUpgradeClusterLevel: &tkeapi.AutoUpgradeClusterLevel{
			IsAutoUpgrade: &spec.ClusterBasicSettings.IsAutoUpgrade,
		},
	}

	request.ClusterCIDRSettings = &tkeapi.ClusterCIDRSettings{
		ClusterCIDR:               &spec.ClusterCIDRSettings.ClusterCIDR,
		IgnoreClusterCIDRConflict: &spec.ClusterCIDRSettings.IgnoreClusterCIDRConflict,
		MaxNodePodNum:             utils.ParseInt64ToUint64(&spec.ClusterCIDRSettings.MaxNodePodNum),
		MaxClusterServiceNum:      utils.ParseInt64ToUint64(&spec.ClusterCIDRSettings.MaxClusterServiceNum),
		ServiceCIDR:               &spec.ClusterCIDRSettings.ServiceCIDR,
		EniSubnetIds:              utils.ParseStrings(spec.ClusterCIDRSettings.EniSubnetIDs),
		ClaimExpiredSeconds:       &spec.ClusterCIDRSettings.ClaimExpiredSeconds,
		IgnoreServiceCIDRConflict: &spec.ClusterCIDRSettings.IgnoreServiceCIDRConflict,
	}

	request.ClusterAdvancedSettings = &tkeapi.ClusterAdvancedSettings{
		IPVS:             &spec.ClusterAdvancedSettings.IPVS,
		AsEnabled:        &spec.ClusterAdvancedSettings.AsEnabled,
		ContainerRuntime: &spec.ClusterAdvancedSettings.ContainerRuntime,
		NodeNameType:     &spec.ClusterAdvancedSettings.NodeNameType,
		ExtraArgs: &tkeapi.ClusterExtraArgs{
			KubeAPIServer:         utils.ParseStrings(spec.ClusterAdvancedSettings.KubeAPIServer),
			KubeControllerManager: utils.ParseStrings(spec.ClusterAdvancedSettings.KubeControllerManager),
			KubeScheduler:         utils.ParseStrings(spec.ClusterAdvancedSettings.KubeScheduler),
			Etcd:                  utils.ParseStrings(spec.ClusterAdvancedSettings.Etcd),
		},
		NetworkType:             &spec.ClusterAdvancedSettings.NetworkType,
		IsNonStaticIpMode:       &spec.ClusterAdvancedSettings.IsNonStaticIpMode,
		DeletionProtection:      &spec.ClusterAdvancedSettings.DeletionProtection,
		KubeProxyMode:           &spec.ClusterAdvancedSettings.KubeProxyMode,
		AuditEnabled:            &spec.ClusterAdvancedSettings.AuditEnabled,
		AuditLogsetId:           &spec.ClusterAdvancedSettings.AuditLogsetID,
		AuditLogTopicId:         &spec.ClusterAdvancedSettings.AuditLogTopicID,
		VpcCniType:              &spec.ClusterAdvancedSettings.VpcCniType,
		RuntimeVersion:          &spec.ClusterAdvancedSettings.RuntimeVersion,
		EnableCustomizedPodCIDR: &spec.ClusterAdvancedSettings.EnableCustomizedPodCIDR,
		BasePodNumber:           &spec.ClusterAdvancedSettings.BasePodNumber,
		CiliumMode:              &spec.ClusterAdvancedSettings.CiliumMode,
		IsDualStack:             &spec.ClusterAdvancedSettings.IsDualStack,
		QGPUShareEnable:         &spec.ClusterAdvancedSettings.QGPUShareEnable,
	}

	if spec.RunInstancesForNode != nil {
		var runInstancesForNodes []*tkeapi.RunInstancesForNode
		var runInstancesParas []*string
		runInstancesRequest := &cvmapi.RunInstancesRequest{
			InstanceChargeType: &spec.RunInstancesForNode.InstanceChargeType,
			Placement: &cvmapi.Placement{
				Zone:      &spec.RunInstancesForNode.Zone,
				ProjectId: &spec.RunInstancesForNode.ProjectID,
			},
			InstanceCount: &spec.RunInstancesForNode.InstanceCount,
			InstanceType:  &spec.RunInstancesForNode.InstanceType,
			ImageId:       &spec.RunInstancesForNode.ImageID,
			VirtualPrivateCloud: &cvmapi.VirtualPrivateCloud{
				VpcId:    &spec.RunInstancesForNode.VpcID,
				SubnetId: &spec.RunInstancesForNode.SubnetID,
			},
			InternetAccessible: &cvmapi.InternetAccessible{
				InternetChargeType:      &spec.RunInstancesForNode.InternetChargeType,
				InternetMaxBandwidthOut: &spec.RunInstancesForNode.InternetMaxBandwidthOut,
				PublicIpAssigned:        &spec.RunInstancesForNode.PublicIpAssigned,
			},
			InstanceName: &spec.RunInstancesForNode.InstanceName,
			LoginSettings: &cvmapi.LoginSettings{
				KeyIds: utils.ParseStrings(spec.RunInstancesForNode.KeyIDs),
			},
			EnhancedService: &cvmapi.EnhancedService{
				SecurityService: &cvmapi.RunSecurityServiceEnabled{
					Enabled: &spec.RunInstancesForNode.SecurityService,
				},
				MonitorService: &cvmapi.RunMonitorServiceEnabled{
					Enabled: &spec.RunInstancesForNode.MonitorService,
				},
			},
			UserData: &spec.RunInstancesForNode.UserData,
		}

		runInstancesPara, err := json.Marshal(runInstancesRequest)
		if err != nil {
			return nil, err
		}
		runInstancesParas = append(runInstancesParas, utils.ValueString(string(runInstancesPara)))

		runInstancesForNodes = append(runInstancesForNodes, &tkeapi.RunInstancesForNode{
			NodeRole:         &spec.RunInstancesForNode.NodeRole,
			RunInstancesPara: runInstancesParas,
		})
		request.RunInstancesForNode = runInstancesForNodes
	}
	var specExtensionAddon []*tkeapi.ExtensionAddon
	for _, extensionAddon := range spec.ExtensionAddon {
		specExtensionAddon = append(specExtensionAddon, &tkeapi.ExtensionAddon{
			AddonName:  &extensionAddon.AddonName,
			AddonParam: &extensionAddon.AddonParam,
		})
	}
	request.ExtensionAddons = specExtensionAddon

	response, err := t.client.CreateCluster(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || response.Response.ClusterId == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.ClusterId, nil
}

func (t TKEClient) DeleteCluster(clusterId string) error {
	logrus.Infof("client tke action: DeleteCluster")
	request := tkeapi.NewDeleteClusterRequest()
	request.ClusterId = &clusterId
	request.InstanceDeleteMode = &InstanceDeleteMode

	if _, err := t.client.DeleteCluster(request); err != nil {
		return err
	}

	return nil
}

func (t TKEClient) GetClusterKubeconfig(clusterId string, extranet bool) (*string, error) {
	logrus.Infof("client tke action: GetClusterKubeconfig")
	request := tkeapi.NewDescribeClusterKubeconfigRequest()
	request.IsExtranet = tccommon.BoolPtr(extranet)
	request.ClusterId = &clusterId

	response, err := t.client.DescribeClusterKubeconfig(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || response.Response.Kubeconfig == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.Kubeconfig, nil
}

func (t TKEClient) GetRegions() (*tkeapi.DescribeRegionsResponse, error) {
	logrus.Infof("client tke action: GetRegions")
	request := tkeapi.NewDescribeRegionsRequest()

	response, err := t.client.DescribeRegions(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (t TKEClient) GetVersions() (*tkeapi.DescribeVersionsResponse, error) {
	logrus.Infof("client tke action: GetVersions")
	request := tkeapi.NewDescribeVersionsRequest()

	response, err := t.client.DescribeVersions(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

// GetImages lists TKE node OS images via DescribeOSImages (not DescribeImages, which targets container image instances).
func (t TKEClient) GetImages() (*tkeapi.DescribeOSImagesResponse, error) {
	logrus.Infof("client tke action: GetImages (DescribeOSImages)")
	request := tkeapi.NewDescribeOSImagesRequest()

	response, err := t.client.DescribeOSImages(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (t TKEClient) UpdateClusterVersion(configSpec *tkev1.TKEClusterConfigSpec) (*tkeapi.UpdateClusterVersionResponse, error) {
	logrus.Infof("client tke action: UpdateClusterVersion")
	request := tkeapi.NewUpdateClusterVersionRequest()
	request.ClusterId = &configSpec.ClusterID
	request.DstVersion = &configSpec.ClusterBasicSettings.ClusterVersion
	// Only set ExtraArgs when ClusterAdvancedSettings is present; otherwise leave nil for API default.
	if configSpec.ClusterAdvancedSettings != nil {
		request.ExtraArgs = &tkeapi.ClusterExtraArgs{
			KubeAPIServer:         utils.ParseStrings(configSpec.ClusterAdvancedSettings.KubeAPIServer),
			KubeControllerManager: utils.ParseStrings(configSpec.ClusterAdvancedSettings.KubeControllerManager),
			KubeScheduler:         utils.ParseStrings(configSpec.ClusterAdvancedSettings.KubeScheduler),
			Etcd:                  utils.ParseStrings(configSpec.ClusterAdvancedSettings.Etcd),
		}
	}

	response, err := t.client.UpdateClusterVersion(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

// ModifyClusterAttribute updates mutable cluster attributes.
// upstreamBasicSettings is used to suppress fields that the TKE API rejects when
// the value is identical to the current upstream value (e.g. ClusterLevel).
func (t TKEClient) ModifyClusterAttribute(configSpec *tkev1.TKEClusterConfigSpec, upstreamBasicSettings *tkev1.ClusterBasicSettings) (*tkeapi.ModifyClusterAttributeResponse, error) {
	logrus.Infof("client tke action: ModifyClusterAttribute")
	request := tkeapi.NewModifyClusterAttributeRequest()
	request.ClusterId = &configSpec.ClusterID
	if configSpec.ClusterBasicSettings != nil {
		request.ProjectId = &configSpec.ClusterBasicSettings.ProjectID
		request.ClusterName = &configSpec.ClusterBasicSettings.ClusterName
		request.ClusterDesc = &configSpec.ClusterBasicSettings.ClusterDescription
		// TKE API rejects ClusterLevel / AutoUpgradeClusterLevel when the value is unchanged;
		// send each field independently only when it actually differs from upstream.
		if upstreamBasicSettings == nil || configSpec.ClusterBasicSettings.ClusterLevel != upstreamBasicSettings.ClusterLevel {
			request.ClusterLevel = &configSpec.ClusterBasicSettings.ClusterLevel
		}
		if upstreamBasicSettings == nil || configSpec.ClusterBasicSettings.IsAutoUpgrade != upstreamBasicSettings.IsAutoUpgrade {
			request.AutoUpgradeClusterLevel = &tkeapi.AutoUpgradeClusterLevel{
				IsAutoUpgrade: &configSpec.ClusterBasicSettings.IsAutoUpgrade,
			}
		}
	}
	if configSpec.ClusterAdvancedSettings != nil {
		request.QGPUShareEnable = &configSpec.ClusterAdvancedSettings.QGPUShareEnable
	}

	response, err := t.client.ModifyClusterAttribute(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

// EnableClusterDeletionProtection enables deletion protection for the given cluster.
func (t TKEClient) EnableClusterDeletionProtection(clusterID string) error {
	logrus.Infof("client tke action: EnableClusterDeletionProtection")
	request := tkeapi.NewEnableClusterDeletionProtectionRequest()
	request.ClusterId = &clusterID
	if _, err := t.client.EnableClusterDeletionProtection(request); err != nil {
		return err
	}
	return nil
}

// DisableClusterDeletionProtection disables deletion protection for the given cluster.
func (t TKEClient) DisableClusterDeletionProtection(clusterID string) error {
	logrus.Infof("client tke action: DisableClusterDeletionProtection")
	request := tkeapi.NewDisableClusterDeletionProtectionRequest()
	request.ClusterId = &clusterID
	if _, err := t.client.DisableClusterDeletionProtection(request); err != nil {
		return err
	}
	return nil
}

func (t TKEClient) GetClusterInstances(clusterId string) ([]*tkeapi.Instance, error) {
	logrus.Infof("client tke action: GetClusterInstances")
	request := tkeapi.NewDescribeClusterInstancesRequest()
	request.ClusterId = &clusterId

	response, err := t.client.DescribeClusterInstances(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.InstanceSet, nil
}

func (t TKEClient) CreateClusterInstances() (*tkeapi.CreateClusterInstancesResponse, error) {
	logrus.Infof("client tke action: CreateClusterInstances")
	request := tkeapi.NewCreateClusterInstancesRequest()

	response, err := t.client.CreateClusterInstances(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (t TKEClient) DeleteClusterInstances() (*tkeapi.DeleteClusterInstancesResponse, error) {
	logrus.Infof("client tke action: DeleteClusterInstances")
	request := tkeapi.NewDeleteClusterInstancesRequest()

	response, err := t.client.DeleteClusterInstances(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (t TKEClient) GetClusterEndpoints(clusterId string) (*tkeapi.DescribeClusterEndpointsResponse, error) {
	logrus.Infof("client tke action: GetClusterEndpoints")
	request := tkeapi.NewDescribeClusterEndpointsRequest()
	request.ClusterId = &clusterId

	response, err := t.client.DescribeClusterEndpoints(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

// GetClusterEndpointStatus returns the endpoint status and an optional error message from TKE.
// The errorMsg is non-empty only when status is "CreateFailed".
func (t TKEClient) GetClusterEndpointStatus(clusterId string, extranet bool) (status string, errorMsg string, err error) {
	logrus.Infof("client tke action: GetClusterEndpointStatus")
	request := tkeapi.NewDescribeClusterEndpointStatusRequest()
	request.ClusterId = &clusterId
	request.IsExtranet = tccommon.BoolPtr(extranet)

	response, err := t.client.DescribeClusterEndpointStatus(request)
	if err != nil {
		return "", "", err
	}

	if response.Response == nil || response.Response.Status == nil {
		return "", "", fmt.Errorf("error while getting response")
	}

	if response.Response.ErrorMsg != nil {
		errorMsg = *response.Response.ErrorMsg
	}
	return *response.Response.Status, errorMsg, nil
}

func (t TKEClient) DeleteClusterEndpoints(clusterId string, extranet bool) error {
	logrus.Infof("client tke action: DeleteClusterEndpoints")
	request := tkeapi.NewDeleteClusterEndpointRequest()
	request.ClusterId = &clusterId
	request.IsExtranet = tccommon.BoolPtr(extranet)

	if _, err := t.client.DeleteClusterEndpoint(request); err != nil {
		return err
	}

	return nil
}

func (t TKEClient) CreateClusterEndpoints(spec tkev1.TKEClusterConfigSpec, extranet bool) error {
	logrus.Infof("client tke action: CreateClusterEndpoints")
	request := tkeapi.NewCreateClusterEndpointRequest()
	request.ClusterId = &spec.ClusterID
	request.IsExtranet = tccommon.BoolPtr(extranet)
	request.SubnetId = &spec.ClusterEndpoint.SubnetID
	request.Domain = &spec.ClusterEndpoint.Domain
	request.SecurityGroup = &spec.ClusterEndpoint.SecurityGroup
	request.ExtensiveParameters = &spec.ClusterEndpoint.ExtensiveParameters

	if _, err := t.client.CreateClusterEndpoint(request); err != nil {
		return err
	}

	return nil
}

func (t TKEClient) CreateClusterVirtualNodePool(clusterId string, pool tkev1.VirtualNodePoolDetail) (*string, error) {
	logrus.Infof("client tke action: CreateClusterVirtualNodePool")
	request := tkeapi.NewCreateClusterVirtualNodePoolRequest()
	request.ClusterId = &clusterId
	request.Name = &pool.Name
	request.SecurityGroupIds = utils.ParseStrings(pool.SecurityGroupIDs)

	if len(pool.SubnetIDs) > 0 {
		request.SubnetIds = utils.ParseStrings(pool.SubnetIDs)
	}
	if len(pool.Labels) > 0 {
		for _, l := range pool.Labels {
			lCopy := l
			request.Labels = append(request.Labels, &tkeapi.Label{
				Name:  &lCopy.Name,
				Value: &lCopy.Value,
			})
		}
	}
	if len(pool.Taints) > 0 {
		for _, t := range pool.Taints {
			tCopy := t
			request.Taints = append(request.Taints, &tkeapi.Taint{
				Key:    &tCopy.Key,
				Value:  &tCopy.Value,
				Effect: &tCopy.Effect,
			})
		}
	}
	if len(pool.VirtualNodes) > 0 {
		for _, vn := range pool.VirtualNodes {
			vnCopy := vn
			spec := &tkeapi.VirtualNodeSpec{
				SubnetId: &vnCopy.SubnetId,
			}
			if vnCopy.DisplayName != "" {
				dn := vnCopy.DisplayName
				spec.DisplayName = &dn
			}
			for _, tag := range vnCopy.Tags {
				tagCopy := tag
				spec.Tags = append(spec.Tags, &tkeapi.Tag{
					Key:   &tagCopy.Key,
					Value: &tagCopy.Value,
				})
			}
			request.VirtualNodes = append(request.VirtualNodes, spec)
		}
	}
	if pool.DeletionProtection != nil {
		request.DeletionProtection = pool.DeletionProtection
	}
	if pool.OS != "" {
		request.OS = &pool.OS
	}

	logrus.Debugf("CreateClusterVirtualNodePool request: %s", request.ToJsonString())

	response, err := t.client.CreateClusterVirtualNodePool(request)
	if err != nil {
		return nil, err
	}
	if response.Response == nil || response.Response.NodePoolId == nil {
		return nil, fmt.Errorf("error while getting response")
	}
	return response.Response.NodePoolId, nil
}

// ModifyClusterVirtualNodePool calls the TKE API when fields is non-empty.
// The bool is true when the API accepted a change that may apply asynchronously; false when
// there was nothing to send or the cloud reported no effective change ("nothing is updated").
func (t TKEClient) ModifyClusterVirtualNodePool(clusterId string, nodePoolId string, fields *utils.VirtualNodePoolModifyFields) (appliedChange bool, err error) {
	if fields == nil || fields.Empty() {
		return false, nil
	}
	logrus.Infof("client tke action: ModifyClusterVirtualNodePool")
	request := tkeapi.NewModifyClusterVirtualNodePoolRequest()
	request.ClusterId = &clusterId
	request.NodePoolId = &nodePoolId

	if fields.Name != nil {
		request.Name = fields.Name
	}
	if fields.SecurityGroupIDs != nil {
		request.SecurityGroupIds = utils.ParseStrings(*fields.SecurityGroupIDs)
	}
	if fields.Labels != nil {
		for _, l := range *fields.Labels {
			lCopy := l
			request.Labels = append(request.Labels, &tkeapi.Label{
				Name:  &lCopy.Name,
				Value: &lCopy.Value,
			})
		}
	}
	if fields.Taints != nil {
		for _, tnp := range *fields.Taints {
			tCopy := tnp
			request.Taints = append(request.Taints, &tkeapi.Taint{
				Key:    &tCopy.Key,
				Value:  &tCopy.Value,
				Effect: &tCopy.Effect,
			})
		}
	}
	if fields.DeletionProtection != nil {
		request.DeletionProtection = fields.DeletionProtection
	}

	if _, err := t.client.ModifyClusterVirtualNodePool(request); err != nil {
		// Local diff can disagree with the cloud (normalization, read-after-write, or fields the API ignores).
		// TKE returns InvalidParameter.Param / "nothing is updated" when the payload would not change state.
		if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok {
			if sdkErr.Code == "InvalidParameter.Param" && strings.Contains(sdkErr.Message, "nothing is updated") {
				logrus.Infof("ModifyClusterVirtualNodePool nodePoolId=%s: no effective change on cloud, ok", nodePoolId)
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func (t TKEClient) GetClusterVirtualNodePools(clusterId string) ([]*tkeapi.VirtualNodePool, error) {
	logrus.Infof("client tke action: GetClusterVirtualNodePools")
	request := tkeapi.NewDescribeClusterVirtualNodePoolsRequest()
	request.ClusterId = &clusterId
	response, err := t.client.DescribeClusterVirtualNodePools(request)
	if err != nil {
		return nil, err
	}
	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}
	return response.Response.NodePoolSet, nil
}

// GetClusterVirtualNodePoolsFull calls DescribeClusterVirtualNodePools via CommonRequest and parses
// the full JSON (including SecurityGroupIds, DeletionProtection, OS, etc. missing from generated SDK models).
func (t TKEClient) GetClusterVirtualNodePoolsFull(clusterId string) ([]tkeapifull.VirtualNodePool, error) {
	logrus.Infof("client tke action: GetClusterVirtualNodePoolsFull")
	// 2018-05-25 is api verion, you can find it in tencent API Explorer
	req := tchttp.NewCommonRequest("tke", "2018-05-25", "DescribeClusterVirtualNodePools")
	if err := req.SetActionParameters(map[string]interface{}{"ClusterId": clusterId}); err != nil {
		return nil, err
	}
	resp := tchttp.NewCommonResponse()
	if err := t.common.Send(req, resp); err != nil {
		return nil, err
	}
	raw := resp.GetBody()
	logrus.Infof("DescribeClusterVirtualNodePools raw response: %s", string(raw))
	return tkeapifull.ParseDescribeClusterVirtualNodePoolsResponse(raw)
}

func (t TKEClient) GetClusterVirtualNodes(clusterId string, nodePoolId string) ([]*tkeapi.VirtualNode, error) {
	logrus.Infof("client tke action: GetClusterVirtualNodes clusterId=%s nodePoolId=%s", clusterId, nodePoolId)
	request := tkeapi.NewDescribeClusterVirtualNodeRequest()
	request.ClusterId = &clusterId
	request.NodePoolId = &nodePoolId
	response, err := t.client.DescribeClusterVirtualNode(request)
	if err != nil {
		return nil, err
	}
	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}
	return response.Response.Nodes, nil
}

func (t TKEClient) DeleteClusterVirtualNodePool(clusterId string, nodePoolIds []*string, force bool) error {
	logrus.Infof("client tke action: DeleteClusterVirtualNodePool")
	request := tkeapi.NewDeleteClusterVirtualNodePoolRequest()
	request.ClusterId = &clusterId
	request.NodePoolIds = nodePoolIds
	request.Force = &force
	if _, err := t.client.DeleteClusterVirtualNodePool(request); err != nil {
		return err
	}
	return nil
}

func (t TKEClient) GetClusterLevelAttribute() (*tkeapi.DescribeClusterLevelAttributeResponse, error) {
	logrus.Infof("client tke action: GetClusterLevelAttribute")
	request := tkeapi.NewDescribeClusterLevelAttributeRequest()

	response, err := t.client.DescribeClusterLevelAttribute(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

// CheckInstancesUpgradeAble returns the instance IDs of cluster nodes that can be upgraded
// to match the current master version.
// Returns an empty slice when all nodes are already at the target version.
func (t TKEClient) CheckInstancesUpgradeAble(clusterId string) ([]string, error) {
	logrus.Infof("client tke action: CheckInstancesUpgradeAble clusterId=%s", clusterId)
	request := tkeapi.NewCheckInstancesUpgradeAbleRequest()
	request.ClusterId = &clusterId

	response, err := t.client.CheckInstancesUpgradeAble(request)
	if err != nil {
		return nil, err
	}
	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response from CheckInstancesUpgradeAble")
	}

	if body, marshalErr := json.Marshal(response.Response); marshalErr != nil {
		logrus.Errorf("CheckInstancesUpgradeAble clusterId=%s response=%+v", clusterId, response.Response)
	} else {
		logrus.Infof("CheckInstancesUpgradeAble clusterId=%s response=%s", clusterId, string(body))
	}

	var instanceIds []string
	for _, inst := range response.Response.UpgradeAbleInstances {
		if inst != nil && inst.InstanceId != nil {
			instanceIds = append(instanceIds, *inst.InstanceId)
		}
	}
	return instanceIds, nil
}

// UpgradeClusterInstances starts a node version upgrade task for the given instances.
// upgradeType: "major" for in-place major-version upgrade, "hot" for minor-version hot upgrade.
// Operation is always "create" to initiate a new upgrade task.
func (t TKEClient) UpgradeClusterInstances(clusterId, upgradeType string, instanceIds []string) error {
	logrus.Infof("client tke action: UpgradeClusterInstances clusterId=%s upgradeType=%s instances=%v",
		clusterId, upgradeType, instanceIds)
	request := tkeapi.NewUpgradeClusterInstancesRequest()
	op := "create"
	request.Operation = &op
	request.ClusterId = &clusterId
	request.UpgradeType = &upgradeType
	request.InstanceIds = utils.ParseStrings(instanceIds)

	response, err := t.client.UpgradeClusterInstances(request)
	if err != nil {
		return err
	}
	if response.Response == nil {
		return fmt.Errorf("error while getting response from UpgradeClusterInstances")
	}
	return nil
}

// GetUpgradeInstanceProgress returns the lifeState of the latest node upgrade task for the cluster.
// Possible lifeState values: "pending", "process", "paused", "pauing", "done", "timeout", "aborted".
// Returns ("", err) when the API call fails (e.g., no upgrade task has ever been created).
func (t TKEClient) GetUpgradeInstanceProgress(clusterId string) (string, error) {
	logrus.Infof("client tke action: GetUpgradeInstanceProgress clusterId=%s", clusterId)
	request := tkeapi.NewGetUpgradeInstanceProgressRequest()
	request.ClusterId = &clusterId

	response, err := t.client.GetUpgradeInstanceProgress(request)
	if err != nil {
		return "", err
	}
	if response.Response == nil || response.Response.LifeState == nil {
		return "", fmt.Errorf("error while getting response from GetUpgradeInstanceProgress")
	}
	return *response.Response.LifeState, nil
}

// CheckClusterCIDR checks whether the given CIDR conflicts with the VPC, other clusters in the
// same VPC, or VPC global routes.
// Uses CommonRequest because the tencentcloud-sdk-go version bundled here does not include
// the CheckClusterCIDR method in its generated client.
func (t TKEClient) CheckClusterCIDR(vpcId, clusterCIDR string) (*tkeapifull.CheckClusterCIDRBody, error) {
	logrus.Infof("client tke action: CheckClusterCIDR vpcId=%s clusterCIDR=%s", vpcId, clusterCIDR)
	req := tchttp.NewCommonRequest("tke", "2018-05-25", "CheckClusterCIDR")
	if err := req.SetActionParameters(map[string]interface{}{
		"VpcId":       vpcId,
		"ClusterCIDR": clusterCIDR,
	}); err != nil {
		return nil, err
	}
	resp := tchttp.NewCommonResponse()
	if err := t.common.Send(req, resp); err != nil {
		return nil, err
	}
	logrus.Debugf("CheckClusterCIDR raw response: %s", string(resp.GetBody()))
	return tkeapifull.ParseCheckClusterCIDRResponse(resp.GetBody())
}
