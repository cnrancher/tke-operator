package client

import (
	"fmt"
	"github.com/sirupsen/logrus"
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cvmapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
)

var clusterIdFilter = "dedicated-cluster-id"

type CVMClient struct {
	client *cvmapi.Client
}

func GetCVMClient(credential *tccommon.Credential, region string) (*CVMClient, error) {
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "cvm.tencentcloudapi.com"
	client, err := cvmapi.NewClient(credential, region, cpf)
	if err != nil {
		return nil, err
	}

	return &CVMClient{client: client}, nil
}

func (c CVMClient) GetInstances(clusterId string) (*cvmapi.DescribeInstancesResponse, error) {
	logrus.Infof("client cvm action: GetInstances")
	request := cvmapi.NewDescribeInstancesRequest()
	request.Filters = []*cvmapi.Filter{
		{
			Name:   &clusterIdFilter,
			Values: []*string{&clusterId},
		},
	}

	response, err := c.client.DescribeInstances(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (c CVMClient) GetInstanceTypeConfigs() (*cvmapi.DescribeInstanceTypeConfigsResponse, error) {
	logrus.Infof("client cvm action: GetInstanceTypeConfigs")
	request := cvmapi.NewDescribeInstanceTypeConfigsRequest()

	response, err := c.client.DescribeInstanceTypeConfigs(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (c CVMClient) GetKeyPairs() (*cvmapi.DescribeKeyPairsResponse, error) {
	logrus.Infof("client cvm action: GetKeyPairs")
	request := cvmapi.NewDescribeKeyPairsRequest()

	response, err := c.client.DescribeKeyPairs(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}

func (c CVMClient) GetZones() (*cvmapi.DescribeZonesResponse, error) {
	logrus.Infof("client cvm action: GetZones")
	request := cvmapi.NewDescribeZonesRequest()

	response, err := c.client.DescribeZones(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}
