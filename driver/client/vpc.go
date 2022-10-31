package client

import (
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	vpcapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/vpc/v20170312"
)

type VPCClient struct {
	client *vpcapi.Client
}

func GetVPCClient(credential *tccommon.Credential, region string) (*VPCClient, error) {
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "vpc.tencentcloudapi.com"
	client, err := vpcapi.NewClient(credential, region, cpf)
	if err != nil {
		return nil, err
	}

	return &VPCClient{client: client}, nil
}

func (v VPCClient) GetVPCs() (*vpcapi.DescribeVpcsResponse, error) {
	request := vpcapi.NewDescribeVpcsRequest()

	response, err := v.client.DescribeVpcs(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (v VPCClient) GetSubnets() (*vpcapi.DescribeSubnetsResponse, error) {
	request := vpcapi.NewDescribeSubnetsRequest()

	response, err := v.client.DescribeSubnets(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (v VPCClient) GetSecurityGroups() (*vpcapi.DescribeSecurityGroupsResponse, error) {
	request := vpcapi.NewDescribeSecurityGroupsRequest()

	response, err := v.client.DescribeSecurityGroups(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}
