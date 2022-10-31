package driver

import (
	"fmt"
	"github.com/cnrancher/tke-operator/driver/client"
	"github.com/cnrancher/tke-operator/utils"
	wranglerv1 "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

// state of cluster
const (
	ClusterStatusRunning  = "Running"
	ClusterStatusCreating = "Creating"
	ClusterStatusIdling   = "Idling"
	ClusterStatusAbnormal = "Abnormal"
)

// state of node pool
const (
	NodePoolStatusCreating = "creating"
	NodePoolStatusNormal   = "normal"
	NodePoolStatusUpdating = "updating"
	NodePoolStatusDeleting = "deleting"
	NodePoolStatusDeleted  = "deleted"
)

// state of endpoint
const (
	EndpointStatusCreated  = "Created"
	EndpointStatusCreating = "Creating"
	EndpointStatusNotFound = "NotFound"
)

// state of instance
const (
	InstanceStatusRunning      = "running"
	InstanceStatusInitializing = "initializing"
	InstanceStatusFailed       = "failed"
)

type Driver struct {
	TKEClient *client.TKEClient
	CVMClient *client.CVMClient
	VPCClient *client.VPCClient
	CBSClient *client.CBSClient
	ASClient  *client.ASClient
}

func GetDriver(secretsCache wranglerv1.SecretCache, tkeCredentialSecret, region string) (*Driver, error) {
	if region == "" {
		region = "ap-guangzhou"
	}

	credential, err := GetCredential(secretsCache, tkeCredentialSecret)
	if err != nil {
		return nil, err
	}

	tkeClient, err := client.GetTKEClient(credential, region)
	if err != nil {
		return nil, err
	}

	cvmClient, err := client.GetCVMClient(credential, region)
	if err != nil {
		return nil, err
	}

	vpcClient, err := client.GetVPCClient(credential, region)
	if err != nil {
		return nil, err
	}

	cbsClient, err := client.GetCBSClient(credential, region)
	if err != nil {
		return nil, err
	}

	asClient, err := client.GetASClient(credential, region)
	if err != nil {
		return nil, err
	}

	return &Driver{
		TKEClient: tkeClient,
		CVMClient: cvmClient,
		VPCClient: vpcClient,
		CBSClient: cbsClient,
		ASClient:  asClient,
	}, nil
}

func GetCredential(secretsCache wranglerv1.SecretCache, tkeCredentialSecret string) (*tccommon.Credential, error) {
	if tkeCredentialSecret == "" {
		return nil, fmt.Errorf("error while getting tkeCredentialSecret")
	}

	ns, name := utils.Parse(tkeCredentialSecret)
	secret, err := secretsCache.Get(ns, name)
	if err != nil {
		return nil, err
	}

	accessKeyBytes := secret.Data["tkecredentialConfig-accessKeyId"]
	secretKeyBytes := secret.Data["tkecredentialConfig-accessKeySecret"]
	if accessKeyBytes == nil || secretKeyBytes == nil {
		return nil, fmt.Errorf("invalid tke credential")
	}

	return tccommon.NewCredential(string(accessKeyBytes), string(secretKeyBytes)), nil
}
