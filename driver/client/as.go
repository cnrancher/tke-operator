package client

import (
	"fmt"
	"github.com/sirupsen/logrus"
	asapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
)

type ASClient struct {
	client *asapi.Client
}

func GetASClient(credential *tccommon.Credential, region string) (*ASClient, error) {
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "as.tencentcloudapi.com"
	client, err := asapi.NewClient(credential, region, cpf)
	if err != nil {
		return nil, err
	}

	return &ASClient{client: client}, nil
}

func (a ASClient) GetAutoScalingGroups(scalingGroupId *string) (*asapi.AutoScalingGroup, error) {
	logrus.Infof("client as action: GetAutoScalingGroups")
	request := asapi.NewDescribeAutoScalingGroupsRequest()
	request.AutoScalingGroupIds = []*string{scalingGroupId}
	response, err := a.client.DescribeAutoScalingGroups(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || len(response.Response.AutoScalingGroupSet) == 0 {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.AutoScalingGroupSet[0], nil
}

func (a ASClient) GetLaunchConfigurations(launchConfigurationId *string) (*asapi.LaunchConfiguration, error) {
	logrus.Infof("client as action: GetLaunchConfigurations")
	request := asapi.NewDescribeLaunchConfigurationsRequest()
	request.LaunchConfigurationIds = []*string{launchConfigurationId}
	response, err := a.client.DescribeLaunchConfigurations(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil || len(response.Response.LaunchConfigurationSet) == 0 {
		return nil, fmt.Errorf("error while getting response")
	}

	return response.Response.LaunchConfigurationSet[0], nil
}
