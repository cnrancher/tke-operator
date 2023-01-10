package client

import (
	"encoding/json"
	"fmt"
	tkev1 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
	"github.com/cnrancher/tke-operator/utils"
	"github.com/sirupsen/logrus"
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cvmapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
	tkeapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tke/v20180525"
)

var (
	InstanceDeleteMode = "terminate"
	KeepInstance       = false
)

type TKEClient struct {
	client *tkeapi.Client
}

func GetTKEClient(credential *tccommon.Credential, region string) (*TKEClient, error) {
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "tke.tencentcloudapi.com"
	client, err := tkeapi.NewClient(credential, region, cpf)
	if err != nil {
		return nil, err
	}

	return &TKEClient{client: client}, nil
}

func (t TKEClient) GetCluster(clusterId string) (*tkeapi.Cluster, error) {
	logrus.Infof("client tke action: GetCluster")
	request := tkeapi.NewDescribeClustersRequest()
	request.ClusterIds = []*string{&clusterId}
	response, err := t.client.DescribeClusters(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || len(response.Response.Clusters) == 0 {
		return nil, fmt.Errorf("error while getting response")
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
	request.InstanceAdvancedSettings = &tkeapi.InstanceAdvancedSettings{
		Labels: utils.ParseStringLabels(nodePool.Labels),
		Taints: utils.ParseStringTaints(nodePool.Taints),
	}

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
	request.NodePoolId = &nodePool.NodePoolId
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
		VpcId:              &spec.ClusterBasicSettings.VpcId,
		ProjectId:          &spec.ClusterBasicSettings.ProjectId,
		TagSpecification:   utils.ParseToTagSpecification(spec.ClusterBasicSettings.Tags),
		OsCustomizeType:    &spec.ClusterCIDRSettings.OsCustomizeType,
		SubnetId:           &spec.ClusterCIDRSettings.SubnetId,
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
		EniSubnetIds:              utils.ParseStrings(spec.ClusterCIDRSettings.EniSubnetIds),
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
		AuditLogsetId:           &spec.ClusterAdvancedSettings.AuditLogsetId,
		AuditLogTopicId:         &spec.ClusterAdvancedSettings.AuditLogTopicId,
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
				ProjectId: &spec.RunInstancesForNode.ProjectId,
			},
			InstanceCount: &spec.RunInstancesForNode.InstanceCount,
			InstanceType:  &spec.RunInstancesForNode.InstanceType,
			ImageId:       &spec.RunInstancesForNode.ImageId,
			VirtualPrivateCloud: &cvmapi.VirtualPrivateCloud{
				VpcId:    &spec.RunInstancesForNode.VpcId,
				SubnetId: &spec.RunInstancesForNode.SubnetId,
			},
			InternetAccessible: &cvmapi.InternetAccessible{
				InternetChargeType:      &spec.RunInstancesForNode.InternetChargeType,
				InternetMaxBandwidthOut: &spec.RunInstancesForNode.InternetMaxBandwidthOut,
				PublicIpAssigned:        &spec.RunInstancesForNode.PublicIpAssigned,
			},
			InstanceName: &spec.RunInstancesForNode.InstanceName,
			LoginSettings: &cvmapi.LoginSettings{
				KeyIds: utils.ParseStrings(spec.RunInstancesForNode.KeyIds),
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

func (t TKEClient) GetClusterKubeconfig(clusterId string) (*string, error) {
	logrus.Infof("client tke action: GetClusterKubeconfig")
	request := tkeapi.NewDescribeClusterKubeconfigRequest()
	request.IsExtranet = tccommon.BoolPtr(true)
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

func (t TKEClient) GetImages() (*tkeapi.DescribeImagesResponse, error) {
	logrus.Infof("client tke action: GetImages")
	request := tkeapi.NewDescribeImagesRequest()

	response, err := t.client.DescribeImages(request)
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
	request.ClusterId = &configSpec.ClusterId
	request.DstVersion = &configSpec.ClusterBasicSettings.ClusterVersion
	request.ExtraArgs = &tkeapi.ClusterExtraArgs{
		KubeAPIServer:         utils.ParseStrings(configSpec.ClusterAdvancedSettings.KubeAPIServer),
		KubeControllerManager: utils.ParseStrings(configSpec.ClusterAdvancedSettings.KubeControllerManager),
		KubeScheduler:         utils.ParseStrings(configSpec.ClusterAdvancedSettings.KubeScheduler),
		Etcd:                  utils.ParseStrings(configSpec.ClusterAdvancedSettings.Etcd),
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

func (t TKEClient) ModifyClusterAttribute(configSpec *tkev1.TKEClusterConfigSpec) (*tkeapi.ModifyClusterAttributeResponse, error) {
	logrus.Infof("client tke action: ModifyClusterAttribute")
	request := tkeapi.NewModifyClusterAttributeRequest()
	request.ClusterId = &configSpec.ClusterId
	request.ProjectId = &configSpec.ClusterBasicSettings.ProjectId
	request.ClusterName = &configSpec.ClusterBasicSettings.ClusterName
	request.ClusterDesc = &configSpec.ClusterBasicSettings.ClusterDescription
	request.ClusterLevel = &configSpec.ClusterBasicSettings.ClusterLevel
	request.AutoUpgradeClusterLevel = &tkeapi.AutoUpgradeClusterLevel{
		IsAutoUpgrade: &configSpec.ClusterBasicSettings.IsAutoUpgrade,
	}
	request.QGPUShareEnable = &configSpec.ClusterAdvancedSettings.QGPUShareEnable

	response, err := t.client.ModifyClusterAttribute(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
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

func (t TKEClient) GetClusterEndpointStatus(clusterId string, extranet bool) (*string, error) {
	logrus.Infof("client tke action: GetClusterEndpointStatus")
	request := tkeapi.NewDescribeClusterEndpointStatusRequest()
	request.ClusterId = &clusterId
	request.IsExtranet = tccommon.BoolPtr(extranet)

	response, err := t.client.DescribeClusterEndpointStatus(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || response.Response.Status == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.Status, nil
}

func (t TKEClient) CreateClusterEndpoints(spec tkev1.TKEClusterConfigSpec, extranet bool) error {
	logrus.Infof("client tke action: CreateClusterEndpoints")
	request := tkeapi.NewCreateClusterEndpointRequest()
	request.ClusterId = &spec.ClusterId
	request.IsExtranet = tccommon.BoolPtr(extranet)
	request.SubnetId = &spec.ClusterEndpoint.SubnetId
	request.Domain = &spec.ClusterEndpoint.Domain
	request.SecurityGroup = &spec.ClusterEndpoint.SecurityGroup
	request.ExtensiveParameters = &spec.ClusterEndpoint.ExtensiveParameters

	if _, err := t.client.CreateClusterEndpoint(request); err != nil {
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
