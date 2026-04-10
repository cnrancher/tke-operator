// Package tkeapifull holds response types for TKE APIs whose fields are not fully covered
// by the generated tencentcloud-sdk-go models (e.g. SecurityGroupIds, CheckClusterCIDR).
package tkeapifull

// DescribeClusterVirtualNodePoolsEnvelope matches the top-level API JSON.
type DescribeClusterVirtualNodePoolsEnvelope struct {
	Response DescribeClusterVirtualNodePoolsBody `json:"Response"`
}

// DescribeClusterVirtualNodePoolsBody is the inner Response object.
type DescribeClusterVirtualNodePoolsBody struct {
	TotalCount  uint64            `json:"TotalCount"`
	NodePoolSet []VirtualNodePool `json:"NodePoolSet"`
	RequestId   string            `json:"RequestId"`
	Error       *DescribeAPIError `json:"Error,omitempty"`
}

// DescribeAPIError is returned on business errors.
type DescribeAPIError struct {
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// VirtualNodePool matches the live DescribeClusterVirtualNodePools NodePoolSet item JSON.
type VirtualNodePool struct {
	NodePoolId         string   `json:"NodePoolId"`
	Name               string   `json:"Name"`
	Status             string   `json:"Status"`
	LifeState          string   `json:"LifeState"`
	SubnetIds          []string `json:"SubnetIds"`
	SecurityGroupIds   []string `json:"SecurityGroupIds"`
	Labels             []Label  `json:"Labels"`
	Taints             []Taint  `json:"Taints"`
	DeletionProtection bool     `json:"DeletionProtection"`
	OS                 string   `json:"OS"`
	CreatedAt          string   `json:"CreatedAt"`
	UpdatedAt          string   `json:"UpdatedAt"`
}

// Label matches TKE API label objects in virtual node pool responses.
type Label struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// Taint matches TKE API taint objects.
type Taint struct {
	Key    string `json:"Key"`
	Value  string `json:"Value"`
	Effect string `json:"Effect"`
}

// CheckClusterCIDREnvelope matches the top-level JSON returned by TKE CheckClusterCIDR.
type CheckClusterCIDREnvelope struct {
	Response CheckClusterCIDRBody `json:"Response"`
}

// CheckClusterCIDRBody is the inner Response object for CheckClusterCIDR.
type CheckClusterCIDRBody struct {
	IsConflict   bool              `json:"IsConflict"`
	ConflictType string            `json:"ConflictType"`
	ConflictMsg  string            `json:"ConflictMsg"`
	RequestId    string            `json:"RequestId"`
	Error        *DescribeAPIError `json:"Error,omitempty"`
}
