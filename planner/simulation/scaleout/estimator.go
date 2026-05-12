package scaleout

import (
	"fmt"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	plannerapi "github.com/gardener/scaling-advisor/api/planner"
)

var (
	_ plannerapi.NodeEstimator = (*defaultNodeEstimator)(nil)
)

type defaultNodeEstimator struct {
	strategy commontypes.SimulatorStrategy
}

func NewNodeEstimator(strategy commontypes.SimulatorStrategy) plannerapi.NodeEstimator {
	return &defaultNodeEstimator{strategy: strategy}
}

func (e *defaultNodeEstimator) InitialTemplateCounts(templates []plannerapi.ScaleOutNodeTemplate, podInfos []plannerapi.PodInfo) ([]plannerapi.ScaleOutNodeTemplateCount, error) {
	if e.strategy.IsSingleNode() {
		if len(templates) > 1 {
			return nil, fmt.Errorf("%w: strategy %q should not have %d (>1) scale-out nodetemplate", plannerapi.ErrCreateSimNodes, e.strategy, len(templates))
		}
		return []plannerapi.ScaleOutNodeTemplateCount{
			{
				ScaleOutNodeTemplate: templates[0],
				Count:                1,
			},
		}, nil
	}
	return nil, nil
}
