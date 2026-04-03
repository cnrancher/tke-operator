package tkeapifull

import (
	"encoding/json"
	"fmt"

	tkev1 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
)

// ParseDescribeClusterVirtualNodePoolsResponse unmarshals the raw JSON from CommonResponse.GetBody()
// or the full HTTP body (both are shaped as {"Response":{...}}).
func ParseDescribeClusterVirtualNodePoolsResponse(raw []byte) ([]VirtualNodePool, error) {
	var env DescribeClusterVirtualNodePoolsEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("unmarshal DescribeClusterVirtualNodePools: %w", err)
	}
	if env.Response.Error != nil && env.Response.Error.Code != "" {
		return nil, fmt.Errorf("api error: code=%s message=%s requestId=%s",
			env.Response.Error.Code, env.Response.Error.Message, env.Response.RequestId)
	}
	return env.Response.NodePoolSet, nil
}

// ToDetail maps Describe JSON fields to VirtualNodePoolDetail for modifiable-field comparison
// and upstream sync. Does not include VirtualNodes (filled separately via DescribeClusterVirtualNode).
func (vp *VirtualNodePool) ToDetail() tkev1.VirtualNodePoolDetail {
	dp := vp.DeletionProtection
	labels := make([]tkev1.VirtualNodeLabel, 0, len(vp.Labels))
	for _, l := range vp.Labels {
		labels = append(labels, tkev1.VirtualNodeLabel{Name: l.Name, Value: l.Value})
	}
	taints := make([]tkev1.VirtualNodeTaint, 0, len(vp.Taints))
	for _, t := range vp.Taints {
		taints = append(taints, tkev1.VirtualNodeTaint{Key: t.Key, Value: t.Value, Effect: t.Effect})
	}
	return tkev1.VirtualNodePoolDetail{
		NodePoolID:         vp.NodePoolId,
		Name:               vp.Name,
		SubnetIDs:          append([]string(nil), vp.SubnetIds...),
		SecurityGroupIDs:   append([]string(nil), vp.SecurityGroupIds...),
		Labels:             labels,
		Taints:             taints,
		OS:                 vp.OS,
		DeletionProtection: &dp,
	}
}
