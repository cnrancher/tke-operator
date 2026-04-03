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

func GetCBSClient(credential *tccommon.Credential, region, language string) (*CBSClient, error) {
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = "cbs.tencentcloudapi.com"
	if language == "zh-CN" || language == "en-US" {
		cpf.Language = language
	}
	client, err := cbsapi.NewClient(credential, region, cpf)
	if err != nil {
		return nil, err
	}

	return &CBSClient{client: client}, nil
}

// GetDiskConfigQuota resolves optional instanceType to an instance family via cvm, then queries CBS disk config quota for zoneId.
// zoneId is required. When instanceType is set but not found in the zone (e.g. sold out), returns an empty DiskConfigSet.
func (c CBSClient) GetDiskConfigQuota(cvm *CVMClient, zoneId, instanceType string) (*cbsapi.DescribeDiskConfigQuotaResponse, error) {
	if zoneId == "" {
		return nil, fmt.Errorf("zoneId is required")
	}
	logrus.Infof("[tke-cbs] GetDiskConfigQuota zoneId=%q instanceType=%q", zoneId, instanceType)

	var instanceFamilies []string
	if instanceType != "" {
		if cvm == nil {
			return nil, fmt.Errorf("cvm client is required when instanceType is set")
		}
		instanceFamily, err := cvm.GetInstanceFamilyForZoneAndInstanceType(zoneId, instanceType)
		if err != nil {
			logrus.Errorf("[tke-cbs] GetDiskConfigQuota GetInstanceFamilyForZoneAndInstanceType failed: %v", err)
			return nil, err
		}
		if instanceFamily == "" {
			logrus.Warnf("[tke-cbs] GetDiskConfigQuota instanceType=%q not found in zone %q (e.g. sold out), returning empty DiskConfigSet", instanceType, zoneId)
			return emptyDiskConfigQuotaResponse(), nil
		}
		instanceFamilies = []string{instanceFamily}
		logrus.Infof("[tke-cbs] GetDiskConfigQuota instanceType=%q maps to InstanceFamily=%q", instanceType, instanceFamily)
	}

	resp, err := c.GetDiskConfigQuotaWithOptions(zoneId, instanceFamilies)
	if err != nil {
		logrus.Errorf("[tke-cbs] GetDiskConfigQuota zoneId=%q failed: %v", zoneId, err)
		return nil, err
	}
	diskCount := 0
	if resp.Response != nil && resp.Response.DiskConfigSet != nil {
		diskCount = len(resp.Response.DiskConfigSet)
	}
	logrus.Infof("[tke-cbs] GetDiskConfigQuota zoneId=%q instanceType=%q DiskConfigSet count=%d", zoneId, instanceType, diskCount)
	return resp, nil
}

func emptyDiskConfigQuotaResponse() *cbsapi.DescribeDiskConfigQuotaResponse {
	rid := ""
	return &cbsapi.DescribeDiskConfigQuotaResponse{
		Response: &cbsapi.DescribeDiskConfigQuotaResponseParams{
			DiskConfigSet: []*cbsapi.DiskConfig{},
			RequestId:     &rid,
		},
	}
}

// GetDiskConfigQuotaWithOptions returns disk configs for the given zone and optionally filtered by instance families.
// When instanceFamilies is nil or empty, returns all disk configs in the zone.
// When instanceFamilies is non-empty, returns only disk configs compatible with those instance families
func (c CBSClient) GetDiskConfigQuotaWithOptions(zoneId string, instanceFamilies []string) (*cbsapi.DescribeDiskConfigQuotaResponse, error) {
	logrus.Infof("[tke-cbs] GetDiskConfigQuotaWithOptions zoneId=%q instanceFamilies=%v", zoneId, instanceFamilies)
	request := cbsapi.NewDescribeDiskConfigQuotaRequest()
	request.InquiryType = &inquiryType
	request.Zones = []*string{&zoneId}
	if len(instanceFamilies) > 0 {
		families := make([]*string, len(instanceFamilies))
		for i := range instanceFamilies {
			families[i] = &instanceFamilies[i]
		}
		request.InstanceFamilies = families
	}

	response, err := c.client.DescribeDiskConfigQuota(request)
	if err != nil {
		logrus.Errorf("[tke-cbs] GetDiskConfigQuotaWithOptions zoneId=%q failed: %v", zoneId, err)
		return nil, err
	}
	if response.Response == nil {
		return nil, fmt.Errorf("DescribeDiskConfigQuota response is nil")
	}
	diskCount := 0
	if response.Response.DiskConfigSet != nil {
		diskCount = len(response.Response.DiskConfigSet)
	}
	logrus.Infof("[tke-cbs] GetDiskConfigQuotaWithOptions zoneId=%q success, DiskConfigSet count=%d", zoneId, diskCount)
	return response, nil
}
