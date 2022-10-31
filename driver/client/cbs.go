package client

import (
	"fmt"
	"github.com/sirupsen/logrus"
	cbsapi "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cbs/v20170312"
	tccommon "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
)

var inquiryType = "INQUIRY_CVM_CONFIG"

type CBSClient struct {
	client *cbsapi.Client
}

func GetCBSClient(credential *tccommon.Credential, region string) (*CBSClient, error) {
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "cbs.tencentcloudapi.com"
	client, err := cbsapi.NewClient(credential, region, cpf)
	if err != nil {
		return nil, err
	}

	return &CBSClient{client: client}, nil
}

func (c CBSClient) GetDiskConfigQuota() (*cbsapi.DescribeDiskConfigQuotaResponse, error) {
	logrus.Infof("client cbs action: GetDiskConfigQuota")
	request := cbsapi.NewDescribeDiskConfigQuotaRequest()
	request.InquiryType = &inquiryType

	response, err := c.client.DescribeDiskConfigQuota(request)
	if err != nil {
		return nil, err
	}

	if response.Response == nil {
		return nil, fmt.Errorf("error while getting response")
	}

	return response, nil
}
