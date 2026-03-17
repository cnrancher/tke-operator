package client

import (
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
	advancedSettings := &tkeapi.InstanceAdvancedSettings{
		Labels: utils.ParseStringLabels(nodePool.Labels),
		Taints: utils.ParseStringTaints(nodePool.Taints),
	}
	if nodePool.UserScript != "" {
		advancedSettings.UserScript = &nodePool.UserScript
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
		request.UserScript = &nodePool.UserScript
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

func (t TKEClient) ModifyClusterAttribute(configSpec *tkev1.TKEClusterConfigSpec) (*tkeapi.ModifyClusterAttributeResponse, error) {
	logrus.Infof("client tke action: ModifyClusterAttribute")
	request := tkeapi.NewModifyClusterAttributeRequest()
	request.ClusterId = &configSpec.ClusterID
	request.ProjectId = &configSpec.ClusterBasicSettings.ProjectID
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
		for i, vn := range pool.VirtualNodes {
			vnCopy := vn
			displayName := vnCopy.DisplayName
			// Tencent Cloud may treat multiple VirtualNodes with the same SubnetId and empty
			// DisplayName as one node. Assign a unique DisplayName so each entry becomes a node.
			if displayName == "" {
				displayName = fmt.Sprintf("node-%d", i)
			}
			spec := &tkeapi.VirtualNodeSpec{
				DisplayName: &displayName,
				SubnetId:    &vnCopy.SubnetId,
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
	logrus.Debugf("DescribeClusterVirtualNodePools raw response: %s", string(raw))
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
