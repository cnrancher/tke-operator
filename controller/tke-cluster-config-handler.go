package controller

import (
	"context"
	"encoding/base64"
	"fmt"
	tcdriver "github.com/cnrancher/tke-operator/driver"
	tkev1 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
	v12 "github.com/cnrancher/tke-operator/pkg/generated/controllers/tke.pandaria.io/v1"
	"github.com/cnrancher/tke-operator/utils"
	wranglerv1 "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/pkg/slice"
	"github.com/sirupsen/logrus"
	tcerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	tkeapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tke/v20180525"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"time"
)

const (
	controllerName           = "tke-controller"
	controllerRemoveName     = "tke-controller-remove"
	tkeConfigCreatingPhase   = "creating"
	tkeConfigNotCreatedPhase = ""
	tkeConfigActivePhase     = "active"
	tkeConfigUpdatingPhase   = "updating"
	tkeConfigImportingPhase  = "importing"
	waitSecond               = 30
	TKEClusterConfigKind     = "TKEClusterConfig"
)

var backoff = wait.Backoff{
	Duration: 30 * time.Second,
	Steps:    12,
}

type Handler struct {
	tkeCC           v12.TKEClusterConfigClient
	tkeEnqueueAfter func(namespace, name string, duration time.Duration)
	tkeEnqueue      func(namespace, name string)
	secrets         wranglerv1.SecretClient
	secretsCache    wranglerv1.SecretCache
}

func Register(
	ctx context.Context,
	secrets wranglerv1.SecretController,
	tke v12.TKEClusterConfigController) {

	controller := &Handler{
		tkeCC:           tke,
		tkeEnqueue:      tke.Enqueue,
		tkeEnqueueAfter: tke.EnqueueAfter,
		secretsCache:    secrets.Cache(),
		secrets:         secrets,
	}

	// Register handlers
	tke.OnChange(ctx, controllerName, controller.recordError(controller.OnTkeConfigChanged))
	tke.OnRemove(ctx, controllerRemoveName, controller.OnTkeConfigRemoved)
}

func (h *Handler) OnTkeConfigChanged(key string, config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	if config == nil {
		return nil, nil
	}
	if config.DeletionTimestamp != nil {
		return nil, nil
	}

	switch config.Status.Phase {
	case tkeConfigImportingPhase:
		return h.importCluster(config)
	case tkeConfigNotCreatedPhase:
		return h.create(config)
	case tkeConfigCreatingPhase:
		return h.waitForCreationComplete(config)
	case tkeConfigActivePhase, tkeConfigUpdatingPhase:
		return h.checkAndUpdate(config)
	}

	return config, nil
}

// recordError writes the error return by onChange to the failureMessage field on status. If there is no error, then
// empty string will be written to status
func (h *Handler) recordError(onChange func(key string, config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error)) func(key string, config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	return func(key string, config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
		var err error
		var message string
		config, err = onChange(key, config)
		if config == nil {
			// TKE config is likely deleting
			return config, err
		}
		if err != nil {
			message = err.Error()
		}

		if config.Status.FailureMessage == message {
			return config, err
		}

		config = config.DeepCopy()

		if message != "" && config.Status.Phase == tkeConfigActivePhase {
			// can assume an update is failing
			config.Status.Phase = tkeConfigUpdatingPhase
		}
		config.Status.FailureMessage = message

		var recordErr error
		config, recordErr = h.tkeCC.UpdateStatus(config)
		if recordErr != nil {
			logrus.Errorf("error recording tkecc [%s] failure message: %s", config.Name, recordErr.Error())
		}
		return config, err
	}
}

func (h *Handler) OnTkeConfigRemoved(key string, config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	logrus.Infof("handler cluster remove...")
	if config.Spec.Imported {
		logrus.Infof("cluster [%s] is imported, will not delete TKE cluster", config.Name)
		return config, nil
	}

	if config.Status.Phase == tkeConfigNotCreatedPhase {
		// The most likely context here is that the cluster already existed in TKE, so we shouldn't delete it
		logrus.Warnf("cluster [%s] never advanced to creating status, will not delete TKE cluster", config.Name)
		return config, nil
	}

	if err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		if config.Spec.ClusterId != "" {
			driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region)
			if err != nil {
				return false, err
			}

			logrus.Infof("removing cluster %v , region %v", config.Name, config.Spec.Region)
			if err := driver.TKEClient.DeleteCluster(config.Spec.ClusterId); err != nil {
				if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok && sdkErr.Code == tkeapi.FAILEDOPERATION_CLUSTERNOTFOUND {
					logrus.Infof("cluster %v , region %v already removed", config.Name, config.Spec.Region)
					return true, nil
				}
				return false, err
			}
		}
		return true, nil
	}); err != nil {
		return config, err
	}

	return config, nil
}

// importCluster returns an active cluster spec containing the given config's clusterName and region/zone
// and creates a Secret containing the cluster's CA and endpoint retrieved from the cluster object.
func (h *Handler) importCluster(config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	logrus.Infof("handler cluster import...")
	if err := h.validateImport(config); err != nil {
		return config, err
	}

	driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region)
	if err != nil {
		return config, err
	}

	cluster, err := driver.TKEClient.GetCluster(config.Spec.ClusterId)
	if err != nil {
		return config, err
	}

	nodePools, err := driver.TKEClient.GetClusterNodePools(config.Spec.ClusterId)
	if err != nil {
		return config, err
	}

	configUpdate := config.DeepCopy()
	configUpdate.Spec = *FixConfig(driver, &config.Spec, cluster, nodePools)
	configUpdate, err = h.tkeCC.Update(configUpdate)
	if err != nil {
		return config, err
	}

	if err = h.createCASecret(driver, config); err != nil {
		return config, err
	}
	configStatus := configUpdate.DeepCopy()
	configStatus.Status.Phase = tkeConfigActivePhase
	return h.tkeCC.UpdateStatus(configStatus)
}

func (h *Handler) create(config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	logrus.Infof("handler cluster create...")
	var err error

	if config.Spec.Imported {
		logrus.Infof("importing cluster [%s]", config.Name)
		config = config.DeepCopy()
		config.Status.Phase = tkeConfigImportingPhase
		return h.tkeCC.UpdateStatus(config)
	}

	if err = h.validate(config); err != nil {
		return config, err
	}

	if config.Spec.ClusterId == "" {
		driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region)
		if err != nil {
			return config, err
		}

		responseClusterId, err := driver.TKEClient.CreateCluster(config.Spec)
		if err != nil {
			return config, err
		}

		configUpdate := config.DeepCopy()
		configUpdate.Spec.ClusterId = *responseClusterId
		configUpdate, err = h.tkeCC.Update(configUpdate)
		if err != nil {
			return config, err
		}

		logrus.Infof("current cluster id: %s", configUpdate.Spec.ClusterId)
		configStatus := configUpdate.DeepCopy()
		configStatus.Status.Phase = tkeConfigCreatingPhase
		config, err = h.tkeCC.UpdateStatus(configStatus)
		if err != nil {
			return config, err
		}
	}

	return config, err
}

func (h *Handler) waitForCreationComplete(config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	logrus.Infof("handler cluster wait for creat complete...")
	if config.Spec.ClusterId != "" {
		driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region)
		if err != nil {
			return nil, err
		}

		cluster, err := driver.TKEClient.GetCluster(config.Spec.ClusterId)
		if err != nil {
			return nil, err
		}

		logrus.Infof("cluster [%s] status [%s] ", *cluster.ClusterName, *cluster.ClusterStatus)
		if *cluster.ClusterStatus == tcdriver.ClusterStatusAbnormal {
			return config, fmt.Errorf("creation failed for cluster %v", config.Name)
		}

		if *cluster.ClusterStatus == tcdriver.ClusterStatusRunning {
			logrus.Infof("cluster %v is running", config.Name)
			config = config.DeepCopy()
			config.Status.Phase = tkeConfigActivePhase
			return h.tkeCC.UpdateStatus(config)
		}
	}
	logrus.Infof("waiting for cluster [%s] to finish creating", config.Name)
	h.tkeEnqueueAfter(config.Namespace, config.Name, waitSecond*time.Second)
	return config, nil
}

func (h *Handler) checkAndUpdate(config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	logrus.Infof("handler cluster update...")
	if err := h.validate(config); err != nil {
		config = config.DeepCopy()
		config.Status.Phase = tkeConfigUpdatingPhase
		config, err = h.tkeCC.UpdateStatus(config)
		if err != nil {
			return config, err
		}
		return config, err
	}

	driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region)
	if err != nil {
		return nil, err
	}

	cluster, err := driver.TKEClient.GetCluster(config.Spec.ClusterId)
	if err != nil {
		return nil, err
	}

	nodePools, err := driver.TKEClient.GetClusterNodePools(config.Spec.ClusterId)
	if err != nil {
		return nil, err
	}

	clusterState := cluster.ClusterStatus
	if *clusterState == tcdriver.ClusterStatusAbnormal {
		logrus.Infof("waiting for cluster [%s] to finish %s", config.Name, *clusterState)
		if config.Status.Phase != tkeConfigUpdatingPhase {
			config = config.DeepCopy()
			config.Status.Phase = tkeConfigUpdatingPhase
			return h.tkeCC.UpdateStatus(config)
		}
		h.tkeEnqueueAfter(config.Namespace, config.Name, 30*time.Second)
		return config, nil
	}

	for _, nodePool := range nodePools {
		status := *nodePool.LifeState
		logrus.Infof("nodePool set state [%s] nodePool name %s", status, *nodePool.Name)
		if status == tcdriver.NodePoolStatusCreating ||
			status == tcdriver.NodePoolStatusDeleting ||
			status == tcdriver.NodePoolStatusUpdating {
			if config.Status.Phase != tkeConfigUpdatingPhase {
				config = config.DeepCopy()
				config.Status.Phase = tkeConfigUpdatingPhase
				config, err = h.tkeCC.UpdateStatus(config)
				if err != nil {
					return config, err
				}
			}
			logrus.Infof("waiting for cluster [%s] to update nodePool set [%s]", config.Name, *nodePool.Name)
			h.tkeEnqueueAfter(config.Namespace, config.Name, 30*time.Second)
			return config, nil
		}
	}

	upstreamSpec, err := BuildUpstreamClusterState(driver, cluster, nodePools)
	if err != nil {
		return config, err
	}

	return h.updateUpstreamClusterState(driver, config, upstreamSpec)
}

// updateUpstreamClusterState sync config to upstream cluster
func (h *Handler) updateUpstreamClusterState(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig, upstreamSpec *tkev1.TKEClusterConfigSpec) (*tkev1.TKEClusterConfig, error) {
	if config.Spec.ClusterBasicSettings.ProjectId != upstreamSpec.ClusterBasicSettings.ProjectId ||
		config.Spec.ClusterBasicSettings.ClusterName != upstreamSpec.ClusterBasicSettings.ClusterName ||
		config.Spec.ClusterBasicSettings.ClusterDescription != upstreamSpec.ClusterBasicSettings.ClusterDescription ||
		config.Spec.ClusterBasicSettings.ClusterLevel != upstreamSpec.ClusterBasicSettings.ClusterLevel ||
		config.Spec.ClusterBasicSettings.IsAutoUpgrade != upstreamSpec.ClusterBasicSettings.IsAutoUpgrade ||
		config.Spec.ClusterAdvancedSettings.QGPUShareEnable != upstreamSpec.ClusterAdvancedSettings.QGPUShareEnable {
		if _, err := driver.TKEClient.ModifyClusterAttribute(&config.Spec); err != nil {
			return config, err
		}
		return h.enqueueUpdate(config)
	}

	if config.Spec.NodePoolList == nil {
		logrus.Infof("cluster [%s] finished updating", config.Name)
		config = config.DeepCopy()
		config.Status.Phase = tkeConfigActivePhase
		return h.tkeCC.UpdateStatus(config)
	}

	var updatingNodePools bool
	var deleteNodePoolIds []*string
	upstreamNodePool := make(map[string]tkev1.NodePoolDetail)
	configNodePool := make(map[string]tkev1.NodePoolDetail)

	var updateNodePoolInstanceTypes, updateNodePoolDesiredCapacity, updateNodePool []tkev1.NodePoolDetail

	for _, np := range upstreamSpec.NodePoolList {
		upstreamNodePool[np.NodePoolId] = np
	}

	var updatingForNodePoolId bool
	for index, np := range config.Spec.NodePoolList {
		if np.NodePoolId == "" {
			responseNodePoolId, err := driver.TKEClient.CreateClusterNodePool(config.Spec.ClusterId, np)
			if err != nil {
				return config, err
			}
			config.Spec.NodePoolList[index].NodePoolId = *responseNodePoolId
			updatingForNodePoolId = true
		} else {
			configNodePool[np.NodePoolId] = np
		}
	}

	if updatingForNodePoolId {
		updateConfig := config.DeepCopy()
		updateConfig.Status.Phase = tkeConfigUpdatingPhase
		return h.tkeCC.Update(updateConfig)
	}

	for _, upstreamNp := range upstreamSpec.NodePoolList {
		if configNp, ok := configNodePool[upstreamNp.NodePoolId]; ok {
			if configNp.LaunchConfigurePara.InstanceType != upstreamNp.LaunchConfigurePara.InstanceType {
				updateNodePoolInstanceTypes = append(updateNodePoolInstanceTypes, configNp)
			}

			if configNp.AutoScalingGroupPara.DesiredCapacity != upstreamNp.AutoScalingGroupPara.DesiredCapacity {
				updateNodePoolDesiredCapacity = append(updateNodePoolDesiredCapacity, configNp)
			}

			if configNp.Name != upstreamNp.Name ||
				configNp.AutoScalingGroupPara.MaxSize != upstreamNp.AutoScalingGroupPara.MaxSize ||
				configNp.AutoScalingGroupPara.MinSize != upstreamNp.AutoScalingGroupPara.MinSize ||
				!slice.StringsEqual(configNp.Labels, upstreamNp.Labels) ||
				!slice.StringsEqual(configNp.Taints, upstreamNp.Taints) ||
				configNp.NodePoolOs != upstreamNp.NodePoolOs ||
				configNp.OsCustomizeType != upstreamNp.OsCustomizeType ||
				!slice.StringsEqual(configNp.Tags, upstreamNp.Tags) ||
				configNp.DeletionProtection != upstreamNp.DeletionProtection {
				updateNodePool = append(updateNodePool, configNp)
			}
		} else {
			deleteNodePoolIds = append(deleteNodePoolIds, &upstreamNp.NodePoolId)
		}
	}

	if len(deleteNodePoolIds) > 0 {
		if err := driver.TKEClient.DeleteNodePool(config.Spec.ClusterId, deleteNodePoolIds); err != nil {
			return config, err
		}
		updatingNodePools = true
	}

	if len(updateNodePool) > 0 {
		for _, np := range updateNodePool {
			if err := driver.TKEClient.ModifyClusterNodePool(config.Spec.ClusterId, np); err != nil {
				return config, err
			}
		}
		updatingNodePools = true
	}

	if len(updateNodePoolInstanceTypes) > 0 {
		for _, np := range updateNodePoolInstanceTypes {
			if err := driver.TKEClient.ModifyNodePoolInstanceTypes(config.Spec.ClusterId, np.NodePoolId, np.LaunchConfigurePara.InstanceType); err != nil {
				return config, err
			}
		}
		updatingNodePools = true
	}

	if len(updateNodePool) > 0 {
		for _, np := range updateNodePool {
			if err := driver.TKEClient.ModifyClusterNodePool(config.Spec.ClusterId, np); err != nil {
				return config, err
			}
		}
		updatingNodePools = true
	}

	if len(updateNodePoolDesiredCapacity) > 0 {
		for _, np := range updateNodePoolDesiredCapacity {
			if err := driver.TKEClient.ModifyNodePoolDesiredCapacityAboutAsg(config.Spec.ClusterId, np.NodePoolId, np.AutoScalingGroupPara.DesiredCapacity); err != nil {
				return config, err
			}
		}
		updatingNodePools = true
	}

	if updatingNodePools {
		return h.enqueueUpdate(config)
	}

	if config.Spec.ClusterEndpoint.Enable {
		logrus.Infof("cluster endpoint enable")
		endpointStatus, err := driver.TKEClient.GetClusterEndpointStatus(config.Spec.ClusterId)
		if err != nil {
			return config, err
		}

		switch *endpointStatus {
		case tcdriver.EndpointStatusCreated:
			if err = h.createCASecret(driver, config); err != nil {
				return config, err
			}
		case tcdriver.EndpointStatusNotFound:
			instances, err := driver.TKEClient.GetClusterInstances(config.Spec.ClusterId)
			if err != nil {
				return config, err
			}

			for _, instance := range instances {
				if *instance.InstanceState == tcdriver.InstanceStatusRunning {
					if err = driver.TKEClient.CreateClusterEndpoints(config.Spec); err != nil {
						return config, err
					}
					break
				}
			}

			h.tkeEnqueueAfter(config.Namespace, config.Name, waitSecond*time.Second)
			return config, nil
		case tcdriver.EndpointStatusCreating:
			logrus.Infof("waiting for cluster [%s] endpoint to finish creating", config.Name)
			h.tkeEnqueueAfter(config.Namespace, config.Name, waitSecond*time.Second)
			return config, nil
		}
	}

	if config.Status.Phase != tkeConfigActivePhase {
		logrus.Infof("cluster [%s] finished updating", config.Name)
		configUpdate := config.DeepCopy()
		configUpdate.Status.Phase = tkeConfigActivePhase
		return h.tkeCC.UpdateStatus(configUpdate)
	}

	logrus.Infof("cluster [%s] is active now", config.Name)
	return config, nil
}

// FixConfig fix fields for clusters
func FixConfig(driver *tcdriver.Driver, configSpec *tkev1.TKEClusterConfigSpec, cluster *tkeapi.Cluster, nodePools []*tkeapi.NodePool) *tkev1.TKEClusterConfigSpec {
	configSpec.ClusterBasicSettings = &tkev1.ClusterBasicSettings{
		ClusterType:        *cluster.ClusterType,
		ClusterOs:          *cluster.ClusterOs,
		ClusterVersion:     *cluster.ClusterVersion,
		ClusterName:        *cluster.ClusterName,
		ClusterDescription: *cluster.ClusterDescription,
		VpcId:              *cluster.ClusterNetworkSettings.VpcId,
		Tags:               utils.ParseTagSpecificationTo(cluster.TagSpecification),
		ClusterLevel:       *cluster.ClusterLevel,
		IsAutoUpgrade:      *cluster.AutoUpgradeClusterLevel,
		ProjectId:          utils.ParseUint64ToInt64(cluster.ProjectId),
	}

	configSpec.ClusterCIDRSettings = &tkev1.ClusterCIDRSettings{
		ClusterCIDR:               *cluster.ClusterNetworkSettings.ClusterCIDR,
		IgnoreClusterCIDRConflict: *cluster.ClusterNetworkSettings.IgnoreClusterCIDRConflict,
		MaxNodePodNum:             utils.ParseUint64ToInt64(cluster.ClusterNetworkSettings.MaxNodePodNum),
		MaxClusterServiceNum:      utils.ParseUint64ToInt64(cluster.ClusterNetworkSettings.MaxClusterServiceNum),
		ServiceCIDR:               *cluster.ClusterNetworkSettings.ServiceCIDR,
		EniSubnetIds:              utils.ParseStringsPointer(cluster.ClusterNetworkSettings.Subnets),
		IgnoreServiceCIDRConflict: *cluster.ClusterNetworkSettings.IgnoreServiceCIDRConflict,
		OsCustomizeType:           *cluster.OsCustomizeType,
	}

	configSpec.ClusterAdvancedSettings = &tkev1.ClusterAdvancedSettings{
		IPVS:             *cluster.ClusterNetworkSettings.Ipvs,
		ContainerRuntime: *cluster.ContainerRuntime,
		RuntimeVersion:   *cluster.RuntimeVersion,
		QGPUShareEnable:  *cluster.QGPUShareEnable,
	}

	var nodePoolList []tkev1.NodePoolDetail
	for _, nodePool := range nodePools {
		autoScalingGroup, err := driver.ASClient.GetAutoScalingGroups(nodePool.AutoscalingGroupId)
		if err != nil {
			logrus.Errorf("error get autoScalingGroup [%s] failure message: %v", *nodePool.AutoscalingGroupId, err)
			continue
		}

		launchConfiguration, err := driver.ASClient.GetLaunchConfigurations(nodePool.LaunchConfigurationId)
		if err != nil {
			logrus.Errorf("error get launchConfiguration [%s] failure message: %v", *nodePool.LaunchConfigurationId, err)
			continue
		}

		nodePoolList = append(nodePoolList, tkev1.NodePoolDetail{
			ClusterId:  *cluster.ClusterId,
			NodePoolId: *nodePool.NodePoolId,
			AutoScalingGroupPara: tkev1.AutoScalingGroupPara{
				AutoScalingGroupName: *autoScalingGroup.AutoScalingGroupName,
				MaxSize:              *autoScalingGroup.MaxSize,
				MinSize:              *autoScalingGroup.MinSize,
				DesiredCapacity:      *autoScalingGroup.DesiredCapacity,
				VpcId:                *autoScalingGroup.VpcId,
				SubnetIds:            utils.ParseStringsPointer(autoScalingGroup.SubnetIdSet),
			},

			LaunchConfigurePara: tkev1.LaunchConfigurePara{
				LaunchConfigurationName: *launchConfiguration.LaunchConfigurationName,
				InstanceType:            *launchConfiguration.InstanceType,
				SystemDisk:              utils.ParseSystemDiskTo(launchConfiguration.SystemDisk),
				InternetChargeType:      *launchConfiguration.InternetAccessible.InternetChargeType,
				InternetMaxBandwidthOut: utils.ParseUint64ToInt64(launchConfiguration.InternetAccessible.InternetMaxBandwidthOut),
				PublicIpAssigned:        *launchConfiguration.InternetAccessible.PublicIpAssigned,
				DataDisks:               utils.ParseDataDisksTo(launchConfiguration.DataDisks),
				KeyIds:                  utils.ParseStringsPointer(launchConfiguration.LoginSettings.KeyIds),
				SecurityGroupIds:        utils.ParseStringsPointer(launchConfiguration.SecurityGroupIds),
				InstanceChargeType:      *launchConfiguration.InstanceChargeType,
			},
			Name:               *nodePool.Name,
			Labels:             utils.ParseLabelsString(nodePool.Labels),
			Taints:             utils.ParseTaintsString(nodePool.Taints),
			NodePoolOs:         *nodePool.NodePoolOs,
			OsCustomizeType:    *nodePool.OsCustomizeType,
			Tags:               utils.ParseTagsString(nodePool.Tags),
			DeletionProtection: *nodePool.DeletionProtection,
		})
	}
	configSpec.NodePoolList = nodePoolList

	return configSpec
}

func BuildUpstreamClusterState(driver *tcdriver.Driver, cluster *tkeapi.Cluster, nodePools []*tkeapi.NodePool) (*tkev1.TKEClusterConfigSpec, error) {
	upstreamSpec := &tkev1.TKEClusterConfigSpec{}
	return FixConfig(driver, upstreamSpec, cluster, nodePools), nil
}

// createCASecret creates a secret containing a CA and endpoint for use in generating a kubeconfig file.
func (h *Handler) createCASecret(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig) error {
	kubeconfig, err := driver.TKEClient.GetClusterKubeconfig(config.Spec.ClusterId)
	if err != nil {
		return err
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig([]byte(*kubeconfig))
	if err != nil {
		return err
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.Name,
			Namespace: config.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: tkev1.SchemeGroupVersion.String(),
					Kind:       TKEClusterConfigKind,
					UID:        config.UID,
					Name:       config.Name,
				},
			},
		},
		Data: map[string][]byte{
			"endpoint": []byte(restConfig.Host),
			"ca":       []byte(base64.StdEncoding.EncodeToString(restConfig.CAData)),
		},
	}

	if _, err = h.secrets.Create(secret); err != nil {
		if errors.IsAlreadyExists(err) {
			logrus.Infof("ca secret [%s] already exists, ignoring", config.Name)
			return nil
		}
	}

	return err
}

// enqueueUpdate enqueues the config if it is already in the updating phase. Otherwise, the
// phase is updated to "updating". This is important because the object needs to reenter the
// onChange handler to start waiting on the update.
func (h *Handler) enqueueUpdate(config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	if config.Status.Phase == tkeConfigUpdatingPhase {
		h.tkeEnqueue(config.Namespace, config.Name)
		return config, nil
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var err error
		config, err = h.tkeCC.Get(config.Namespace, config.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		config = config.DeepCopy()
		config.Status.Phase = tkeConfigUpdatingPhase
		config, err = h.tkeCC.UpdateStatus(config)
		return err
	})
	return config, err
}

func (h *Handler) validateImport(config *tkev1.TKEClusterConfig) error {
	if config.Spec.ClusterId == "" {
		return fmt.Errorf("field [%s] cannot be nil for cluster [%s]", "clusterId", config.Name)
	}

	return h.validate(config)
}

func (h *Handler) validate(config *tkev1.TKEClusterConfig) error {
	if config.Spec.Region == "" {
		return fmt.Errorf("field [%s] cannot be nil for cluster [%s]", "region", config.Name)
	}

	if config.Spec.TKECredentialSecret == "" {
		return fmt.Errorf("field [%s] cannot be nil for cluster [%s]", "tkeCredentialSecret", config.Name)
	}

	return nil
}
