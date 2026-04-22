package controller

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	tcdriver "github.com/cnrancher/tke-operator/driver"
	tkev1 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
	v12 "github.com/cnrancher/tke-operator/pkg/generated/controllers/tke.pandaria.io/v1"
	"github.com/cnrancher/tke-operator/pkg/tkeapifull"
	"github.com/cnrancher/tke-operator/utils"
	wranglerv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"github.com/rancher/wrangler/v3/pkg/slice"
	"github.com/sirupsen/logrus"
	tcerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	tkeapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/tke/v20180525"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
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

	// errCodeVirtualNodePoolInDeletionProtection is the TKE API error code returned when a
	// virtual node pool has deletion protection enabled. The upstream SDK (v1.3.59) does not
	// yet define this constant, so we keep it locally until the SDK is bumped.
	errCodeVirtualNodePoolInDeletionProtection = "OperationDenied.InDeletionProtection"
)

var backoff = wait.Backoff{
	Duration: 30 * time.Second,
	Steps:    12,
}

func normalizeUserScript(script string) string {
	if script == "" {
		return ""
	}
	if _, err := base64.StdEncoding.DecodeString(script); err == nil {
		return script
	}
	return base64.StdEncoding.EncodeToString([]byte(script))
}

type Handler struct {
	tkeCC           v12.TKEClusterConfigClient
	tkeCache        v12.TKEClusterConfigCache
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
		tkeCache:        tke.Cache(),
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
			logrus.Errorf("TKEClusterConfig [%s] sync error: %v", config.Name, err)
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

	if config.Spec.ClusterID == "" {
		logrus.Infof("cluster [%s] has no ClusterID, already deleted", config.Name)
		return config, nil
	}

	driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region, tcdriver.DefaultLanguage)
	if err != nil {
		return config, err
	}

	// Fast-path: if the TKE cluster is already gone, skip all node-pool queries and deletion
	// calls — there is nothing left to clean up on the cloud side.
	cluster, err := driver.TKEClient.GetCluster(config.Spec.ClusterID)
	if err != nil {
		if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok && sdkErr.Code == tkeapi.FAILEDOPERATION_CLUSTERNOTFOUND {
			logrus.Infof("cluster [%s] no longer exists on TKE, skipping deletion steps", config.Name)
			h.recordRemoveError(config, "")
			return config, nil
		}
		return config, err
	}

	// Check deletion protection before attempting any deletion.
	// If the cluster or any node pool has deletion protection enabled, surface a clear error
	// so the user knows they must disable protection first. This also avoids unnecessary
	// cloud API calls and prevents the cluster from being stuck in a silent retry loop.
	// cluster is passed in to avoid a redundant GetCluster call (already fetched above).
	if protectionErr := h.checkDeletionProtection(driver, config, cluster); protectionErr != nil {
		logrus.Errorf("cluster [%s] deletion blocked by deletion protection: %v", config.Name, protectionErr)
		h.recordRemoveError(config, protectionErr.Error())
		return config, protectionErr
	}

	// Protection is not enabled; clear any stale failure message from a previous blocked attempt.
	h.recordRemoveError(config, "")

	if err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region, tcdriver.DefaultLanguage)
		if err != nil {
			return false, err
		}

		// Check and delete node pools first
		done, err := h.ensureNodePoolsDeleted(driver, config)
		if err != nil {
			return false, err
		}
		if !done {
			// node pools still exist, keep waiting
			return false, nil
		}

		// Check and delete virtual node pools before deleting the cluster
		done, err = h.ensureVirtualNodePoolsDeleted(driver, config)
		if err != nil {
			return false, err
		}
		if !done {
			// virtual node pools still exist, keep waiting
			return false, nil
		}

		// All node pools deleted, now delete the cluster
		logrus.Infof("removing cluster %v, region %v", config.Name, config.Spec.Region)
		if err := driver.TKEClient.DeleteCluster(config.Spec.ClusterID); err != nil {
			if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok {
				if sdkErr.Code == tkeapi.FAILEDOPERATION_CLUSTERNOTFOUND {
					logrus.Infof("cluster %v, region %v already removed", config.Name, config.Spec.Region)
					return true, nil
				}
				if sdkErr.Code == tkeapi.OPERATIONDENIED_CLUSTERINDELETIONPROTECTION {
					return false, fmt.Errorf("cluster [%s] has deletion protection enabled, please disable it in tencent console before deleting", config.Spec.ClusterID)
				}
			}
			logrus.Errorf("failed to delete cluster [%v]: %v", config.Name, err)
			return false, err
		}

		logrus.Infof("cluster %v deletion requested successfully", config.Name)
		return true, nil
	}); err != nil {
		return config, err
	}

	return config, nil
}

// recordRemoveError writes the given message to status.FailureMessage so that deletion errors
// are visible in the UI. Unlike recordError (which wraps OnTkeConfigChanged), OnTkeConfigRemoved
// is not wrapped, so this helper must be called explicitly.
func (h *Handler) recordRemoveError(config *tkev1.TKEClusterConfig, message string) {
	if config.Status.FailureMessage == message {
		return
	}
	configCopy := config.DeepCopy()
	configCopy.Status.FailureMessage = message
	if _, err := h.tkeCC.UpdateStatus(configCopy); err != nil {
		logrus.Errorf("error recording tkecc [%s] remove failure message: %s", config.Name, err.Error())
	}
}

// checkDeletionProtection checks whether deletion protection is enabled on the TKE cluster,
// any regular node pool, or any virtual node pool. cluster is passed in by the caller (already
// fetched) to avoid a redundant API call. Returns an error describing which resource is
// protected; returns nil if it is safe to proceed with deletion.
func (h *Handler) checkDeletionProtection(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig, cluster *tkeapi.Cluster) error {
	if cluster.DeletionProtection != nil && *cluster.DeletionProtection {
		return fmt.Errorf("cluster [%s] has deletion protection enabled, please disable it in tencent console before deleting", config.Spec.ClusterID)
	}

	nodePools, err := driver.TKEClient.GetClusterNodePools(config.Spec.ClusterID)
	if err != nil {
		return err
	}

	for _, np := range nodePools {
		if np.DeletionProtection != nil && *np.DeletionProtection {
			name := ""
			if np.Name != nil {
				name = *np.Name
			}
			return fmt.Errorf("node pool [%s] in cluster [%s] has deletion protection enabled, please disable it in tencent console before deleting", name, config.Spec.ClusterID)
		}
	}

	virtualPools, err := driver.TKEClient.GetClusterVirtualNodePoolsFull(config.Spec.ClusterID)
	if err != nil {
		return err
	}

	for _, vp := range virtualPools {
		if vp.DeletionProtection {
			return fmt.Errorf("virtual node pool [%s] in cluster [%s] has deletion protection enabled, please disable it in tencent console before deleting", vp.Name, config.Spec.ClusterID)
		}
	}

	return nil
}

// importCluster returns an active cluster spec containing the given config's clusterName and region/zone
// and creates a Secret containing the cluster's CA and endpoint retrieved from the cluster object.
func (h *Handler) importCluster(config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	logrus.Infof("handler cluster import...")
	if err := h.validateImport(config); err != nil {
		return config, err
	}

	driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region, tcdriver.DefaultLanguage)
	if err != nil {
		return config, err
	}

	cluster, err := driver.TKEClient.GetCluster(config.Spec.ClusterID)
	if err != nil {
		return config, err
	}

	nodePools, err := driver.TKEClient.GetClusterNodePools(config.Spec.ClusterID)
	if err != nil {
		return config, err
	}

	configUpdate := config.DeepCopy()
	configUpdate.Spec = *FixConfig(driver, &config.Spec, cluster, nodePools)

	configUpdate.Spec.VirtualNodePoolList, err = BuildUpstreamVirtualNodePoolList(driver, config.Spec.ClusterID, config.Spec.VirtualNodePoolList)
	if err != nil {
		return config, err
	}
	configUpdate, err = h.tkeCC.Update(configUpdate)
	if err != nil {
		return config, err
	}

	if err = h.createCASecret(driver, configUpdate); err != nil {
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

	if config.Spec.ClusterID == "" {
		driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region, tcdriver.DefaultLanguage)
		if err != nil {
			return config, err
		}

		responseClusterId, err := driver.TKEClient.CreateCluster(config.Spec)
		if err != nil {
			return config, err
		}

		// Use RetryOnConflict to prevent repeated creation when Update fails
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			result, getErr := h.tkeCache.Get(config.Namespace, config.Name)
			if getErr != nil {
				return fmt.Errorf("failed to get tkeConfig from cache: %w", getErr)
			}
			if result.Spec.ClusterID == *responseClusterId {
				config = result
				return nil
			}

			result = result.DeepCopy()
			result.Spec.ClusterID = *responseClusterId
			result, getErr = h.tkeCC.Update(result)
			if getErr != nil {
				return getErr
			}
			config = result
			return nil
		})
		if err != nil {
			return config, err
		}

		logrus.Infof("current cluster id: %s", config.Spec.ClusterID)
		// Update status to creating phase
		err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
			result, getErr := h.tkeCache.Get(config.Namespace, config.Name)
			if getErr != nil {
				return getErr
			}
			if result.Status.Phase == tkeConfigCreatingPhase && result.Status.FailureMessage == "" {
				config = result
				return nil
			}
			result = result.DeepCopy()
			result.Status.Phase = tkeConfigCreatingPhase
			result.Status.FailureMessage = ""
			result, getErr = h.tkeCC.UpdateStatus(result)
			if getErr != nil {
				return getErr
			}
			config = result
			return nil
		})
		if err != nil {
			return config, err
		}
	}

	return config, err
}

func (h *Handler) waitForCreationComplete(config *tkev1.TKEClusterConfig) (*tkev1.TKEClusterConfig, error) {
	logrus.Infof("handler cluster wait for creat complete...")
	if config.Spec.ClusterID != "" {
		driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region, tcdriver.DefaultLanguage)
		if err != nil {
			return nil, err
		}

		cluster, err := driver.TKEClient.GetCluster(config.Spec.ClusterID)
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
	logrus.Infof("cluster [%s] phase=%s clusterID=%s nodePools=%d virtualNodePools=%d",
		config.Name, config.Status.Phase, config.Spec.ClusterID,
		len(config.Spec.NodePoolList), len(config.Spec.VirtualNodePoolList))
	if err := h.validate(config); err != nil {
		config = config.DeepCopy()
		config.Status.Phase = tkeConfigUpdatingPhase
		config, err = h.tkeCC.UpdateStatus(config)
		if err != nil {
			return config, err
		}
		return config, err
	}

	driver, err := tcdriver.GetDriver(h.secretsCache, config.Spec.TKECredentialSecret, config.Spec.Region, tcdriver.DefaultLanguage)
	if err != nil {
		return nil, err
	}

	cluster, err := driver.TKEClient.GetCluster(config.Spec.ClusterID)
	if err != nil {
		return nil, err
	}

	nodePools, err := driver.TKEClient.GetClusterNodePools(config.Spec.ClusterID)
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

	// DescribeClusters does not expose "Upgrading"; use DescribeClusterStatus to detect
	// an in-progress master version upgrade and wait until it finishes.
	clusterDetailStatus, err := driver.TKEClient.GetClusterStatus(&config.Spec.ClusterID)
	if err != nil {
		return nil, err
	}
	if clusterDetailStatus.ClusterState != nil && *clusterDetailStatus.ClusterState == tcdriver.ClusterStatusUpgrading {
		logrus.Infof("waiting for cluster [%s] master upgrade to finish (ClusterState=Upgrading)", config.Name)
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

	if len(config.Spec.VirtualNodePoolList) > 0 {
		virtualNodePools, err := driver.TKEClient.GetClusterVirtualNodePoolsFull(config.Spec.ClusterID)
		if err != nil {
			return config, err
		}
		for _, vp := range virtualNodePools {
			state := vp.LifeState
			if state == tcdriver.NodePoolStatusCreating ||
				state == tcdriver.NodePoolStatusDeleting ||
				state == tcdriver.NodePoolStatusUpdating {
				if config.Status.Phase != tkeConfigUpdatingPhase {
					config = config.DeepCopy()
					config.Status.Phase = tkeConfigUpdatingPhase
					config, err = h.tkeCC.UpdateStatus(config)
					if err != nil {
						return config, err
					}
				}
				logrus.Infof("waiting for cluster [%s] virtual node pool [%s] state [%s]", config.Name, vp.Name, state)
				h.tkeEnqueueAfter(config.Namespace, config.Name, 30*time.Second)
				return config, nil
			}
		}
	}

	upstreamSpec, err := BuildUpstreamClusterState(driver, &config.Spec, cluster, nodePools)
	if err != nil {
		return config, err
	}

	return h.updateUpstreamClusterState(driver, config, upstreamSpec)
}

// updateUpstreamClusterState sync config to upstream cluster
func (h *Handler) updateUpstreamClusterState(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig, upstreamSpec *tkev1.TKEClusterConfigSpec) (*tkev1.TKEClusterConfig, error) {
	// Check kubernetes version for upgrade cluster.
	if config.Spec.ClusterBasicSettings != nil && upstreamSpec.ClusterBasicSettings != nil {
		if config.Spec.ClusterBasicSettings.ClusterVersion != upstreamSpec.ClusterBasicSettings.ClusterVersion {
			logrus.Infof("cluster [%s] version upgrade detected: %s -> %s",
				config.Name,
				upstreamSpec.ClusterBasicSettings.ClusterVersion,
				config.Spec.ClusterBasicSettings.ClusterVersion)
			if _, err := driver.TKEClient.UpdateClusterVersion(&config.Spec); err != nil {
				return config, err
			}
			return h.enqueueUpdate(config)
		}
	}

	// When master upgrade is confirmed complete (versions match), upgrade node instances to the
	// same target version before processing any other spec changes. This mirrors the two-phase
	// upgrade model of TKE: master first, nodes second.
	// waitOrTriggerNodeUpgrade is a no-op when no version change occurred (CheckInstancesUpgradeAble
	// returns empty for clusters already at the target version).
	if config.Spec.ClusterBasicSettings != nil {
		waiting, err := h.waitOrTriggerNodeUpgrade(driver, config)
		if err != nil {
			return config, err
		}
		if waiting {
			h.tkeEnqueueAfter(config.Namespace, config.Name, 30*time.Second)
			return config, nil
		}
	}

	basicChanged := config.Spec.ClusterBasicSettings != nil &&
		(config.Spec.ClusterBasicSettings.ProjectID != upstreamSpec.ClusterBasicSettings.ProjectID ||
			config.Spec.ClusterBasicSettings.ClusterName != upstreamSpec.ClusterBasicSettings.ClusterName ||
			config.Spec.ClusterBasicSettings.ClusterDescription != upstreamSpec.ClusterBasicSettings.ClusterDescription ||
			config.Spec.ClusterBasicSettings.ClusterLevel != upstreamSpec.ClusterBasicSettings.ClusterLevel ||
			config.Spec.ClusterBasicSettings.IsAutoUpgrade != upstreamSpec.ClusterBasicSettings.IsAutoUpgrade)
	advancedChanged := config.Spec.ClusterAdvancedSettings != nil &&
		config.Spec.ClusterAdvancedSettings.QGPUShareEnable != upstreamSpec.ClusterAdvancedSettings.QGPUShareEnable
	if basicChanged || advancedChanged {
		if _, err := driver.TKEClient.ModifyClusterAttribute(&config.Spec, upstreamSpec.ClusterBasicSettings); err != nil {
			return config, err
		}
		return h.enqueueUpdate(config)
	}

	if config.Spec.ClusterAdvancedSettings != nil && upstreamSpec.ClusterAdvancedSettings != nil &&
		config.Spec.ClusterAdvancedSettings.DeletionProtection != upstreamSpec.ClusterAdvancedSettings.DeletionProtection {
		logrus.Infof("cluster [%s] deletion protection change detected: %v -> %v",
			config.Name,
			upstreamSpec.ClusterAdvancedSettings.DeletionProtection,
			config.Spec.ClusterAdvancedSettings.DeletionProtection)
		if config.Spec.ClusterAdvancedSettings.DeletionProtection {
			if err := driver.TKEClient.EnableClusterDeletionProtection(config.Spec.ClusterID); err != nil {
				return config, err
			}
		} else {
			if err := driver.TKEClient.DisableClusterDeletionProtection(config.Spec.ClusterID); err != nil {
				return config, err
			}
		}
		return h.enqueueUpdate(config)
	}

	logrus.Infof("cluster [%s] updateUpstreamClusterState: nodePools=%d virtualNodePools=%d",
		config.Name, len(config.Spec.NodePoolList), len(config.Spec.VirtualNodePoolList))

	if config.Spec.NodePoolList == nil && config.Spec.VirtualNodePoolList == nil {
		logrus.Infof("cluster [%s] finished updating", config.Name)
		config = config.DeepCopy()
		config.Status.Phase = tkeConfigActivePhase
		return h.tkeCC.UpdateStatus(config)
	}

	var updatingNodePools bool
	var deleteNodePoolIds []string
	configNodePool := make(map[string]tkev1.NodePoolDetail)

	var updateNodePoolInstanceTypes, updateNodePoolDesiredCapacity, updateNodePool []tkev1.NodePoolDetail

	var updatingForNodePoolId bool
	for index, np := range config.Spec.NodePoolList {
		if np.NodePoolID == "" {
			responseNodePoolId, err := driver.TKEClient.CreateClusterNodePool(config.Spec.ClusterID, np)
			if err != nil {
				logrus.Errorf("cluster [%s] failed to create node pool [%s]: %v", config.Name, np.Name, err)
				return config, err
			}
			config.Spec.NodePoolList[index].NodePoolID = *responseNodePoolId
			updatingForNodePoolId = true
		} else {
			configNodePool[np.NodePoolID] = np
		}
	}

	if updatingForNodePoolId {
		updateConfig := config.DeepCopy()
		updateConfig.Status.Phase = tkeConfigUpdatingPhase
		return h.tkeCC.Update(updateConfig)
	}

	for _, upstreamNp := range upstreamSpec.NodePoolList {
		if configNp, ok := configNodePool[upstreamNp.NodePoolID]; ok {
			if configNp.LaunchConfigurePara.InstanceType != upstreamNp.LaunchConfigurePara.InstanceType {
				updateNodePoolInstanceTypes = append(updateNodePoolInstanceTypes, configNp)
			}

			if (configNp.AutoScalingGroupPara.DesiredCapacity != upstreamNp.AutoScalingGroupPara.DesiredCapacity) &&
				(configNp.AutoScalingGroupPara.DesiredCapacity <= upstreamNp.AutoScalingGroupPara.MaxSize) {
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
			logrus.Infof("NodePool [%s] will be delete", upstreamNp.NodePoolID)
			deleteNodePoolIds = append(deleteNodePoolIds, upstreamNp.NodePoolID)
		}
	}

	if len(deleteNodePoolIds) > 0 {
		if err := driver.TKEClient.DeleteNodePool(config.Spec.ClusterID, utils.ParseStrings(deleteNodePoolIds)); err != nil {
			return config, err
		}
		updatingNodePools = true
	}

	if len(updateNodePoolDesiredCapacity) > 0 {
		for _, np := range updateNodePoolDesiredCapacity {
			if err := driver.TKEClient.ModifyNodePoolDesiredCapacityAboutAsg(config.Spec.ClusterID, np.NodePoolID, np.AutoScalingGroupPara.DesiredCapacity); err != nil {
				return config, err
			}
		}
		updatingNodePools = true
	}

	if len(updateNodePool) > 0 {
		for _, np := range updateNodePool {
			if err := driver.TKEClient.ModifyClusterNodePool(config.Spec.ClusterID, np); err != nil {
				return config, err
			}
		}
		updatingNodePools = true
	}

	if len(updateNodePoolInstanceTypes) > 0 {
		for _, np := range updateNodePoolInstanceTypes {
			if err := driver.TKEClient.ModifyNodePoolInstanceTypes(config.Spec.ClusterID, np.NodePoolID, np.LaunchConfigurePara.InstanceType); err != nil {
				return config, err
			}
		}
		updatingNodePools = true
	}

	if updatingNodePools {
		return h.enqueueUpdate(config)
	}

	// Reconcile virtual node pools.
	// Match regular node pool flow: create what config wants but upstream doesn't have,
	// modify what differs, delete what upstream has but config doesn't want.
	upstreamList, err := driver.TKEClient.GetClusterVirtualNodePoolsFull(config.Spec.ClusterID)
	if err != nil {
		return config, err
	}

	// Build config set by NodePoolID and Name (config's desired pools)
	configVirtualNodePoolByID := make(map[string]struct{})
	configVirtualNodePoolByName := make(map[string]struct{})
	for _, vp := range config.Spec.VirtualNodePoolList {
		if vp.NodePoolID != "" {
			configVirtualNodePoolByID[vp.NodePoolID] = struct{}{}
		}
		if vp.Name != "" {
			configVirtualNodePoolByName[vp.Name] = struct{}{}
		}
	}

	// Delete virtual node pools that exist upstream but not in config
	var deleteVirtualNodePoolIds []*string
	for i := range upstreamList {
		u := &upstreamList[i]
		if u.NodePoolId == "" {
			continue
		}
		if u.LifeState != "" {
			state := u.LifeState
			if state == tcdriver.NodePoolStatusDeleting || state == tcdriver.NodePoolStatusDeleted {
				continue
			}
		}
		inConfig := false
		if _, ok := configVirtualNodePoolByID[u.NodePoolId]; ok {
			inConfig = true
		}
		if !inConfig && u.Name != "" {
			if _, ok := configVirtualNodePoolByName[u.Name]; ok {
				inConfig = true
			}
		}
		if !inConfig {
			logrus.Infof("Virtual node pool [%s] will be deleted (not in config)", u.NodePoolId)
			poolID := u.NodePoolId
			deleteVirtualNodePoolIds = append(deleteVirtualNodePoolIds, &poolID)
		}
	}
	if len(deleteVirtualNodePoolIds) > 0 {
		// Always use force=true so running pods are evicted. Deletion protection must be
		// disabled beforehand (checkDeletionProtection blocks if any pool is still protected).
		if err := driver.TKEClient.DeleteClusterVirtualNodePool(config.Spec.ClusterID, deleteVirtualNodePoolIds, true); err != nil {
			if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok &&
				sdkErr.Code == errCodeVirtualNodePoolInDeletionProtection {
				return config, fmt.Errorf("virtual node pool in cluster [%s] has deletion protection enabled, please disable it in tencent console before deleting", config.Spec.ClusterID)
			}
			logrus.Errorf("cluster [%s] failed to delete virtual node pool(s): %v", config.Name, err)
			return config, err
		}
		return h.enqueueUpdate(config)
	}

	// Create and modify virtual node pools when config has desired pools
	if len(config.Spec.VirtualNodePoolList) > 0 {
		upstreamByName := make(map[string]*tkeapifull.VirtualNodePool)
		upstreamByID := make(map[string]*tkeapifull.VirtualNodePool)
		for i := range upstreamList {
			u := &upstreamList[i]
			if u.Name != "" {
				upstreamByName[u.Name] = u
			}
			if u.NodePoolId != "" {
				upstreamByID[u.NodePoolId] = u
			}
		}

		// Create missing virtual node pools. If any created, return immediately so we don't
		// mix create and update in the same reconcile. This preserves the creation flow.
		var createdAny bool
		for index, vp := range config.Spec.VirtualNodePoolList {
			existing := upstreamByName[vp.Name]
			if existing == nil && vp.NodePoolID != "" {
				existing = upstreamByID[vp.NodePoolID]
			}
			if existing != nil {
				// Pool already exists upstream. Set nodePoolId locally so downstream logic can use it,
				// but do NOT write back to the CR: writing back triggers a new reconcile which Rancher
				// may overwrite with nodePoolId="" again, causing an infinite adoption loop.
				if existing.NodePoolId != "" {
					config.Spec.VirtualNodePoolList[index].NodePoolID = existing.NodePoolId
				}
				continue
			}
			// existing still nil: new pool (nodePoolId empty) or stale row (nodePoolId left from Rancher spec).
			if vp.NodePoolID != "" {
				logrus.Warnf("cluster [%s] virtual node pool [%s] nodePoolId=%s not found upstream, skip create (stale spec)",
					config.Name, vp.Name, vp.NodePoolID)
				continue
			}
			responsePoolId, err := driver.TKEClient.CreateClusterVirtualNodePool(config.Spec.ClusterID, vp)
			if err != nil {
				logrus.Errorf("cluster [%s] failed to create virtual node pool [%s]: %v", config.Name, vp.Name, err)
				return config, err
			}
			logrus.Infof("cluster [%s] created virtual node pool [%s] with id [%s]", config.Name, vp.Name, *responsePoolId)
			config.Spec.VirtualNodePoolList[index].NodePoolID = *responsePoolId
			createdAny = true
		}

		if createdAny {
			// Write back nodePoolIds for newly created pools so they are visible in the UI.
			filledByName := make(map[string]string)
			for _, vp := range config.Spec.VirtualNodePoolList {
				if vp.NodePoolID != "" {
					filledByName[vp.Name] = vp.NodePoolID
				}
			}
			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				result, getErr := h.tkeCache.Get(config.Namespace, config.Name)
				if getErr != nil {
					return fmt.Errorf("failed to get tkeConfig from cache: %w", getErr)
				}
				result = result.DeepCopy()
				for i := range result.Spec.VirtualNodePoolList {
					vp := &result.Spec.VirtualNodePoolList[i]
					if vp.NodePoolID == "" {
						if id, ok := filledByName[vp.Name]; ok {
							vp.NodePoolID = id
						}
					}
				}
				result.Status.Phase = tkeConfigUpdatingPhase
				result, getErr = h.tkeCC.Update(result)
				if getErr != nil {
					return getErr
				}
				config = result
				return nil
			})
			if err != nil {
				return config, err
			}
			return config, nil
		}

		// Update existing virtual node pools: compare spec to upstream Describe (full JSON) for
		// modifiable fields only; send only differing fields to Modify.
		var updatingVirtualNodePools bool
		for index, vp := range config.Spec.VirtualNodePoolList {
			existing := upstreamByName[vp.Name]
			if existing == nil && vp.NodePoolID != "" {
				existing = upstreamByID[vp.NodePoolID]
			}
			if existing == nil || existing.NodePoolId == "" {
				continue
			}
			upstreamDetail := existing.ToDetail()
			modifyFields := utils.DiffVirtualNodePoolModifyFields(vp, upstreamDetail)
			if modifyFields == nil {
				config.Spec.VirtualNodePoolList[index].NodePoolID = existing.NodePoolId
				continue
			}
			applied, err := driver.TKEClient.ModifyClusterVirtualNodePool(config.Spec.ClusterID, existing.NodePoolId, modifyFields)
			if err != nil {
				logrus.Errorf("cluster [%s] failed to modify virtual node pool [%s]: %v", config.Name, vp.Name, err)
				return config, err
			}
			if applied {
				logrus.Infof("cluster [%s] modified virtual node pool [%s]", config.Name, vp.Name)
				updatingVirtualNodePools = true
			}
			config.Spec.VirtualNodePoolList[index].NodePoolID = existing.NodePoolId
		}

		if updatingVirtualNodePools {
			return h.enqueueUpdate(config)
		}
	}

	if !config.Spec.Imported {
		logrus.Infof("cluster endpoint enable")
		endpointStatus, err := driver.TKEClient.GetClusterEndpointStatus(config.Spec.ClusterID, config.Spec.ClusterEndpoint.Enable)
		if err != nil {
			return config, err
		}

		switch *endpointStatus {
		case tcdriver.EndpointStatusCreated:
			if err = h.createCASecret(driver, config); err != nil {
				return config, err
			}
		case tcdriver.EndpointStatusNotFound:
			// Clusters with only virtual node pools have no traditional VM instances.
			// Use NodePoolList (not VirtualNodePoolList) as the guard: Rancher may overwrite
			// VirtualNodePoolList to nil when syncing, making it unreliable as a condition.
			// If no regular node pools are configured, create the endpoint without waiting for instances.
			logrus.Infof("cluster [%s] endpoint not found: nodePools=%d virtualNodePools=%d",
				config.Name, len(config.Spec.NodePoolList), len(config.Spec.VirtualNodePoolList))
			if len(config.Spec.NodePoolList) == 0 {
				logrus.Infof("cluster [%s] no regular node pools, creating endpoint directly", config.Name)
				if err = driver.TKEClient.CreateClusterEndpoints(config.Spec, config.Spec.ClusterEndpoint.Enable); err != nil {
					return config, err
				}
			} else {
				instances, err := driver.TKEClient.GetClusterInstances(config.Spec.ClusterID)
				if err != nil {
					return config, err
				}
				for _, instance := range instances {
					if *instance.InstanceState == tcdriver.InstanceStatusRunning {
						if err = driver.TKEClient.CreateClusterEndpoints(config.Spec, config.Spec.ClusterEndpoint.Enable); err != nil {
							return config, err
						}
						break
					}
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

// syncClusterEndpointFromDescribeEndpoints merges DescribeClusterEndpoints into clusterEndpoint
func syncClusterEndpointFromDescribeEndpoints(driver *tcdriver.Driver, configSpec *tkev1.TKEClusterConfigSpec, clusterID string) {
	if clusterID == "" {
		return
	}
	endpoints, err := driver.TKEClient.GetClusterEndpoints(clusterID)
	if err != nil {
		logrus.Warnf("syncClusterEndpointFromDescribeEndpoints: failed to get cluster endpoints for [%s]: %v", clusterID, err)
		return
	}
	if endpoints == nil || endpoints.Response == nil {
		return
	}
	resp := endpoints.Response
	ep := configSpec.ClusterEndpoint
	if ep == nil {
		ep = &tkev1.ClusterEndpoint{}
	}
	if resp.SecurityGroup != nil && *resp.SecurityGroup != "" {
		ep.SecurityGroup = *resp.SecurityGroup
	}
	if resp.ClusterIntranetSubnetId != nil && *resp.ClusterIntranetSubnetId != "" {
		ep.SubnetID = *resp.ClusterIntranetSubnetId
	}
	ep.Enable = resp.ClusterExternalEndpoint != nil && *resp.ClusterExternalEndpoint != ""
	configSpec.ClusterEndpoint = ep
}

// clusterPropertyFields: DescribeClusters Cluster.Property JSON (Tencent; partial decode).
type clusterPropertyFields struct {
	NetworkType string `json:"NetworkType"`
}

// parseNetworkTypeFromProperty reads NetworkType from Cluster.Property when present.
func parseNetworkTypeFromProperty(property *string) string {
	if property == nil {
		return ""
	}
	s := strings.TrimSpace(*property)
	if s == "" {
		return ""
	}
	var p clusterPropertyFields
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		logrus.Debugf("parseNetworkTypeFromProperty: %v", err)
		return ""
	}
	return strings.TrimSpace(p.NetworkType)
}

// FixConfig aligns configSpec with the DescribeCluster cluster object and node pools.
func FixConfig(driver *tcdriver.Driver, configSpec *tkev1.TKEClusterConfigSpec, cluster *tkeapi.Cluster, nodePools []*tkeapi.NodePool) *tkev1.TKEClusterConfigSpec {
	configSpec.ClusterBasicSettings = &tkev1.ClusterBasicSettings{
		ClusterType:        *cluster.ClusterType,
		ClusterOs:          *cluster.ClusterOs,
		ClusterVersion:     *cluster.ClusterVersion,
		ClusterName:        *cluster.ClusterName,
		ClusterDescription: *cluster.ClusterDescription,
		VpcID:              *cluster.ClusterNetworkSettings.VpcId,
		Tags:               utils.ParseTagSpecificationTo(cluster.TagSpecification),
		ClusterLevel:       *cluster.ClusterLevel,
		IsAutoUpgrade:      *cluster.AutoUpgradeClusterLevel,
		ProjectID:          utils.ParseUint64ToInt64(cluster.ProjectId),
	}

	configSpec.ClusterCIDRSettings = &tkev1.ClusterCIDRSettings{
		ClusterCIDR:               *cluster.ClusterNetworkSettings.ClusterCIDR,
		IgnoreClusterCIDRConflict: *cluster.ClusterNetworkSettings.IgnoreClusterCIDRConflict,
		MaxNodePodNum:             utils.ParseUint64ToInt64(cluster.ClusterNetworkSettings.MaxNodePodNum),
		MaxClusterServiceNum:      utils.ParseUint64ToInt64(cluster.ClusterNetworkSettings.MaxClusterServiceNum),
		ServiceCIDR:               *cluster.ClusterNetworkSettings.ServiceCIDR,
		EniSubnetIDs:              utils.ParseStringsPointer(cluster.ClusterNetworkSettings.Subnets),
		IgnoreServiceCIDRConflict: *cluster.ClusterNetworkSettings.IgnoreServiceCIDRConflict,
		OsCustomizeType:           *cluster.OsCustomizeType,
		SubnetID:                  utils.StringValue(cluster.ClusterNetworkSettings.SubnetId),
	}

	configSpec.ClusterAdvancedSettings = &tkev1.ClusterAdvancedSettings{
		IPVS:               *cluster.ClusterNetworkSettings.Ipvs,
		ContainerRuntime:   *cluster.ContainerRuntime,
		RuntimeVersion:     *cluster.RuntimeVersion,
		QGPUShareEnable:    *cluster.QGPUShareEnable,
		DeletionProtection: cluster.DeletionProtection != nil && *cluster.DeletionProtection,
		KubeProxyMode:      utils.StringValue(cluster.ClusterNetworkSettings.KubeProxyMode),
		IsDualStack:        cluster.ClusterNetworkSettings.IsDualStack != nil && *cluster.ClusterNetworkSettings.IsDualStack,
		CiliumMode:         utils.StringValue(cluster.ClusterNetworkSettings.CiliumMode),
		NetworkType:        parseNetworkTypeFromProperty(cluster.Property),
	}

	// Index the existing NodePoolList by NodePoolID so we can preserve fields that the
	// DescribeClusterNodePools API does not return (e.g. EnableAutoscale).
	existingNodePoolByID := make(map[string]tkev1.NodePoolDetail, len(configSpec.NodePoolList))
	for _, np := range configSpec.NodePoolList {
		if np.NodePoolID != "" {
			existingNodePoolByID[np.NodePoolID] = np
		}
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

		existing := existingNodePoolByID[*nodePool.NodePoolId]
		nodePoolList = append(nodePoolList, tkev1.NodePoolDetail{
			ClusterID:  *cluster.ClusterId,
			NodePoolID: *nodePool.NodePoolId,
			AutoScalingGroupPara: tkev1.AutoScalingGroupPara{
				AutoScalingGroupName: *autoScalingGroup.AutoScalingGroupName,
				MaxSize:              *autoScalingGroup.MaxSize,
				MinSize:              *autoScalingGroup.MinSize,
				DesiredCapacity:      *autoScalingGroup.DesiredCapacity,
				VpcID:                *autoScalingGroup.VpcId,
				SubnetIDs:            utils.ParseStringsPointer(autoScalingGroup.SubnetIdSet),
			},

			LaunchConfigurePara: tkev1.LaunchConfigurePara{
				LaunchConfigurationName: *launchConfiguration.LaunchConfigurationName,
				InstanceType:            *launchConfiguration.InstanceType,
				SystemDisk:              utils.ParseSystemDiskTo(launchConfiguration.SystemDisk),
				InternetChargeType:      *launchConfiguration.InternetAccessible.InternetChargeType,
				InternetMaxBandwidthOut: utils.ParseUint64ToInt64(launchConfiguration.InternetAccessible.InternetMaxBandwidthOut),
				PublicIpAssigned:        *launchConfiguration.InternetAccessible.PublicIpAssigned,
				DataDisks:               utils.ParseDataDisksTo(launchConfiguration.DataDisks),
				KeyIDs:                  utils.ParseStringsPointer(launchConfiguration.LoginSettings.KeyIds),
				SecurityGroupIDs:        utils.ParseStringsPointer(launchConfiguration.SecurityGroupIds),
				InstanceChargeType:      *launchConfiguration.InstanceChargeType,
			},
			// EnableAutoscale is not returned by DescribeClusterNodePools; preserve from existing spec.
			EnableAutoscale:    existing.EnableAutoscale,
			Name:               *nodePool.Name,
			Labels:             utils.ParseLabelsString(nodePool.Labels),
			Taints:             utils.ParseTaintsString(nodePool.Taints),
			NodePoolOs:         *nodePool.NodePoolOs,
			OsCustomizeType:    *nodePool.OsCustomizeType,
			Tags:               utils.ParseTagsString(nodePool.Tags),
			DeletionProtection: *nodePool.DeletionProtection,
			UserScript:         normalizeUserScript(utils.StringValue(nodePool.UserScript)),
		})
	}
	configSpec.NodePoolList = nodePoolList

	syncClusterEndpointFromDescribeEndpoints(driver, configSpec, *cluster.ClusterId)

	return configSpec
}

// BuildUpstreamClusterState builds the upstream cluster state by using existingSpec as the base
// so that fields the TKE API does not return (e.g. ExtensiveParameters, RunInstancesForNode,
// ExtensionAddon) are naturally preserved. Fields that can be fetched from the API are
// overwritten by FixConfig.
func BuildUpstreamClusterState(driver *tcdriver.Driver, existingSpec *tkev1.TKEClusterConfigSpec, cluster *tkeapi.Cluster, nodePools []*tkeapi.NodePool) (*tkev1.TKEClusterConfigSpec, error) {
	base := existingSpec.DeepCopy()
	return FixConfig(driver, base, cluster, nodePools), nil
}

// BuildUpstreamVirtualNodePoolList fetches virtual node pools from TKE API and converts to
// VirtualNodePoolDetail using upstream Describe fields (full JSON). SubnetIds, security groups,
// labels, taints, OS, deletion protection come from the cloud. VirtualNodes are filled via
// DescribeClusterVirtualNode; on error, preserves VirtualNodes from existing.
func BuildUpstreamVirtualNodePoolList(driver *tcdriver.Driver, clusterID string, existing []tkev1.VirtualNodePoolDetail) ([]tkev1.VirtualNodePoolDetail, error) {
	upstream, err := driver.TKEClient.GetClusterVirtualNodePoolsFull(clusterID)
	if err != nil {
		return nil, err
	}
	return convertUpstreamVirtualNodePoolList(driver, clusterID, upstream, existing), nil
}

func convertUpstreamVirtualNodePoolList(driver *tcdriver.Driver, clusterID string, upstream []tkeapifull.VirtualNodePool, existing []tkev1.VirtualNodePoolDetail) []tkev1.VirtualNodePoolDetail {
	if len(upstream) == 0 {
		return []tkev1.VirtualNodePoolDetail{}
	}

	existingByID := make(map[string]tkev1.VirtualNodePoolDetail, len(existing))
	for _, vp := range existing {
		if vp.NodePoolID != "" {
			existingByID[vp.NodePoolID] = vp
		}
	}

	result := make([]tkev1.VirtualNodePoolDetail, 0, len(upstream))
	for i := range upstream {
		vp := &upstream[i]
		if vp.NodePoolId == "" {
			continue
		}

		item := vp.ToDetail()

		// VirtualNodes: DescribeClusterVirtualNode; on error keep prior spec only for this sub-resource.
		nodes, err := driver.TKEClient.GetClusterVirtualNodes(clusterID, item.NodePoolID)
		if err != nil {
			logrus.Warnf("GetClusterVirtualNodes clusterId=%s nodePoolId=%s: %v, using existing VirtualNodes", clusterID, item.NodePoolID, err)
			if old, ok := existingByID[item.NodePoolID]; ok {
				item.VirtualNodes = old.VirtualNodes
			}
		} else {
			item.VirtualNodes = toVirtualNodeSpecs(nodes)
		}

		result = append(result, item)
	}

	return result
}

func toVirtualNodeSpecs(nodes []*tkeapi.VirtualNode) []tkev1.VirtualNodeSpec {
	out := make([]tkev1.VirtualNodeSpec, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		out = append(out, tkev1.VirtualNodeSpec{
			DisplayName: utils.StringValue(n.Name),
			SubnetId:    utils.StringValue(n.SubnetId),
		})
	}
	return out
}

// createCASecret creates a secret containing a CA and endpoint for use in generating a kubeconfig file.
func (h *Handler) createCASecret(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig) error {
	kubeconfig, err := driver.TKEClient.GetClusterKubeconfig(config.Spec.ClusterID, config.Spec.ClusterEndpoint.Enable)
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

// waitOrTriggerNodeUpgrade ensures that all cluster nodes are upgraded to match the master version
// after a Kubernetes version upgrade. It must only be called when the master upgrade has already
// completed (i.e., upstream ClusterVersion == spec ClusterVersion).
//
// Return semantics:
//   - (true,  nil) – a node upgrade is in progress or was just triggered; caller should re-enqueue.
//   - (false, nil) – all nodes are already at the target version; caller may proceed.
//   - (false, err) – an unrecoverable error occurred (e.g. upgrade task failed/timed-out).
func (h *Handler) waitOrTriggerNodeUpgrade(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig) (bool, error) {
	clusterId := config.Spec.ClusterID

	// Step 1: Query the progress of any existing node upgrade task.
	// Only "process" and "pending" mean a task is active — wait. The API returns an error
	// when no node-upgrade task exists (e.g. "task not found"); the client then returns
	// empty lifeState, so we fall through. Do not gate on err==nil: errors are non-blocking.
	lifeState, err := driver.TKEClient.GetUpgradeInstanceProgress(clusterId)
	logrus.Infof("cluster [%s] node upgrade lifeState=%q err=%v", config.Name, lifeState, err)
	if lifeState == "process" || lifeState == "pending" {
		return true, nil
	}

	// Step 2: Determine which nodes are behind the master version.
	// Only "major" (in-place major-version upgrade) is used, as Rancher only supports
	// major Kubernetes version upgrades for TKE clusters.
	instanceIds, err := driver.TKEClient.CheckInstancesUpgradeAble(clusterId, "major")
	if err != nil {
		return false, fmt.Errorf("cluster [%s] failed to check upgradeable instances: %v", config.Name, err)
	}

	if len(instanceIds) == 0 {
		// All nodes are already at the target version; nothing to do.
		logrus.Infof("cluster [%s] all nodes are at the target version, no node upgrade required", config.Name)
		return false, nil
	}

	// Step 3: Trigger node upgrade for all eligible instances in a single task.
	logrus.Infof("cluster [%s] triggering node upgrade (major) for %d instance(s): %v",
		config.Name, len(instanceIds), instanceIds)
	if err := driver.TKEClient.UpgradeClusterInstances(clusterId, "major", instanceIds); err != nil {
		return false, fmt.Errorf("cluster [%s] failed to trigger node upgrade: %v", config.Name, err)
	}

	return true, nil
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
	if config.Spec.ClusterID == "" {
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

// ensureNodePoolsDeleted ensures all node pools for the cluster are deleted
func (h *Handler) ensureNodePoolsDeleted(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig) (bool, error) {
	nodePools, err := driver.TKEClient.GetClusterNodePools(config.Spec.ClusterID)
	if err != nil {
		// If cluster not found, it means cluster is already deleted
		if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok {
			if sdkErr.Code == tkeapi.FAILEDOPERATION_CLUSTERNOTFOUND {
				logrus.Infof("cluster [%s] not found, already removed", config.Name)
				return true, nil
			}
		}
		logrus.Warnf("failed to get node pools for cluster [%s]: %v", config.Name, err)
		return false, err
	}

	if len(nodePools) == 0 {
		return true, nil
	}

	var needDeletePoolIds []*string
	for _, np := range nodePools {
		if np.NodePoolId == nil || *np.NodePoolId == "" {
			continue
		}
		// Only delete node pools that are not being deleted yet
		if np.LifeState != nil {
			state := *np.LifeState
			if state == tcdriver.NodePoolStatusDeleting || state == tcdriver.NodePoolStatusDeleted {
				// Already being deleted, skip
				continue
			}
		}
		needDeletePoolIds = append(needDeletePoolIds, np.NodePoolId)
	}

	// Request deletion for node pools that need it
	if len(needDeletePoolIds) > 0 {
		logrus.Infof("requesting deletion of %d node pool(s) for cluster [%s]", len(needDeletePoolIds), config.Name)
		if err := driver.TKEClient.DeleteNodePool(config.Spec.ClusterID, needDeletePoolIds); err != nil {
			if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok {
				if sdkErr.Code == "ResourceNotFound.NodePoolNotFound" {
					// Already deleted, continue waiting
					logrus.Infof("node pools already deleted for cluster [%s]", config.Name)
				} else if sdkErr.Code == tkeapi.FAILEDOPERATION_CVMDELETIONPROTECTION {
					return false, fmt.Errorf("one or more node pools in cluster [%s] have deletion protection enabled, please disable it in tencent console before deleting", config.Name)
				} else {
					logrus.Errorf("failed to delete node pools for cluster [%s]: %v", config.Name, err)
					return false, err
				}
			} else {
				logrus.Errorf("failed to delete node pools for cluster [%s]: %v", config.Name, err)
				return false, err
			}
		}
	}

	// Wait for all node pools to be deleted (regardless of whether we just requested deletion)
	logrus.Infof("cluster [%s] still has %d node pool(s), waiting for deletion to complete", config.Name, len(nodePools))
	return false, nil
}

func (h *Handler) ensureVirtualNodePoolsDeleted(driver *tcdriver.Driver, config *tkev1.TKEClusterConfig) (bool, error) {
	virtualNodePools, err := driver.TKEClient.GetClusterVirtualNodePoolsFull(config.Spec.ClusterID)
	if err != nil {
		if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok {
			if sdkErr.Code == tkeapi.FAILEDOPERATION_CLUSTERNOTFOUND {
				logrus.Infof("cluster [%s] not found while querying virtual node pools, already removed", config.Name)
				return true, nil
			}
		}
		logrus.Warnf("failed to get virtual node pools for cluster [%s]: %v", config.Name, err)
		return false, err
	}

	if len(virtualNodePools) == 0 {
		// nothing to delete, allow cluster deletion to proceed
		return true, nil
	}

	var needDeleteVirtualPoolIds []*string
	for i := range virtualNodePools {
		vp := &virtualNodePools[i]
		if vp.NodePoolId == "" {
			continue
		}
		if vp.LifeState != "" {
			state := vp.LifeState
			if state == tcdriver.NodePoolStatusDeleting || state == tcdriver.NodePoolStatusDeleted {
				// already being deleted, skip
				continue
			}
		}
		poolID := vp.NodePoolId
		needDeleteVirtualPoolIds = append(needDeleteVirtualPoolIds, &poolID)
	}

	if len(needDeleteVirtualPoolIds) > 0 {
		logrus.Infof("requesting deletion of %d virtual node pool(s) for cluster [%s]", len(needDeleteVirtualPoolIds), config.Name)
		// Always use force=true: deletion protection is already verified clean by
		// checkDeletionProtection; force evicts any running pods on virtual nodes.
		if err := driver.TKEClient.DeleteClusterVirtualNodePool(config.Spec.ClusterID, needDeleteVirtualPoolIds, true); err != nil {
			if sdkErr, ok := err.(*tcerrors.TencentCloudSDKError); ok {
				switch sdkErr.Code {
				case "ResourceNotFound.NodePoolNotFound":
					logrus.Infof("virtual node pools already deleted for cluster [%s]", config.Name)
				case errCodeVirtualNodePoolInDeletionProtection:
					// Protection re-enabled after our pre-check (race) — surface a clear message.
					return false, fmt.Errorf("virtual node pool in cluster [%s] has deletion protection enabled, please disable it in tencent console before deleting", config.Spec.ClusterID)
				default:
					logrus.Errorf("failed to delete virtual node pools for cluster [%s]: %v", config.Name, err)
					return false, err
				}
			} else {
				logrus.Errorf("failed to delete virtual node pools for cluster [%s]: %v", config.Name, err)
				return false, err
			}
		}
	}

	logrus.Infof("cluster [%s] still has %d virtual node pool(s), waiting for deletion to complete", config.Name, len(virtualNodePools))
	return false, nil
}
