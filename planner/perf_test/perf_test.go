package perf_test

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"sync"
	"testing"
	"time"

	commontypes "github.com/gardener/scaling-advisor/api/common/types"
	sacorev1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	"github.com/gardener/scaling-advisor/api/minkapi"
	"github.com/gardener/scaling-advisor/api/minkapi/typeinfo"
	plannerapi "github.com/gardener/scaling-advisor/api/planner"
	"github.com/gardener/scaling-advisor/common/objutil"
	"github.com/gardener/scaling-advisor/common/podutil"
	commontestutil "github.com/gardener/scaling-advisor/common/testutil"
	"github.com/gardener/scaling-advisor/minkapi/view"
	"github.com/gardener/scaling-advisor/planner"
	"github.com/gardener/scaling-advisor/planner/scheduler"
	pricingtestutil "github.com/gardener/scaling-advisor/pricing/testutil"
	"github.com/gardener/scaling-advisor/samples"
	corev1 "k8s.io/api/core/v1"
)

func TestGreekPoolPods_SingleNodeMultiSim_LeastCost(t *testing.T) {
	monitor := startResourceMonitor(t)
	defer monitor.Stop()
	var request = plannerapi.Request{
		CreationTime:            time.Now(),
		Constraint:              loadScalingConstraint(t, "greek"),
		SimulatorStrategy:       commontypes.SimulatorStrategySingleNodeMultiSim,
		ScoringStrategy:         commontypes.NodeScoringStrategyLeastCost,
		AdviceGenerationMode:    commontypes.ScalingAdviceGenerationModeAllAtOnce,
		AdviceGenerationTimeout: 10 * time.Minute,
		DiagnosticVerbosity:     uint32(4),
	}
	request.ID = t.Name()
	numPods := 6 // TODO: take from env variable or go test parameter
	request.Snapshot.Pods = genPodInfos(t, numPods, "alpha", "beta", "gamma")
	testGenDir, _ := commontestutil.CreateTestGenDir(t)

	runCtx, scalingPlanner := CreateScalingPlanner(t, testGenDir, request.AdviceGenerationTimeout, request.DiagnosticVerbosity)
	_ = obtainLogPlannerResponse(t, testGenDir, runCtx, scalingPlanner, request)
}

func TestAlphaBetaPods_MultiNodeSingleSim_Basic(t *testing.T) {
	monitor := startResourceMonitor(t)
	defer monitor.Stop()
	var request = plannerapi.Request{
		CreationTime:            time.Now(),
		Constraint:              loadScalingConstraint(t, "greek"),
		SimulatorStrategy:       commontypes.SimulatorStrategyMultiNodeSingleSim,
		ScoringStrategy:         commontypes.NodeScoringStrategyLeastCost,
		AdviceGenerationMode:    commontypes.ScalingAdviceGenerationModeAllAtOnce,
		AdviceGenerationTimeout: 10 * time.Minute,
		DiagnosticVerbosity:     uint32(1),
	}
	request.ID = t.Name()
	//numPods := 60D
	numPods := 5000
	request.Snapshot.Pods = genPodInfos(t, numPods, "alpha", "beta")
	testGenDir, _ := commontestutil.CreateTestGenDir(t)

	runCtx, scalingPlanner := CreateScalingPlanner(t, testGenDir, request.AdviceGenerationTimeout, request.DiagnosticVerbosity)
	_ = obtainLogPlannerResponse(t, testGenDir, runCtx, scalingPlanner, request)

}

func TestAlphaBetaGammaPods_MultiNodeSingleSim_Basic(t *testing.T) {
	monitor := startResourceMonitor(t)
	defer monitor.Stop()
	var request = plannerapi.Request{
		CreationTime:            time.Now(),
		Constraint:              loadScalingConstraint(t, "greek"),
		SimulatorStrategy:       commontypes.SimulatorStrategyMultiNodeSingleSim,
		ScoringStrategy:         commontypes.NodeScoringStrategyLeastCost,
		AdviceGenerationMode:    commontypes.ScalingAdviceGenerationModeAllAtOnce,
		AdviceGenerationTimeout: 10 * time.Minute,
		DiagnosticVerbosity:     uint32(1),
	}
	request.ID = t.Name()
	numPods := 10
	request.Snapshot.Pods = genPodInfos(t, numPods, "alpha", "beta", "gamma")
	testGenDir, _ := commontestutil.CreateTestGenDir(t)

	runCtx, scalingPlanner := CreateScalingPlanner(t, testGenDir, request.AdviceGenerationTimeout, request.DiagnosticVerbosity)
	_ = obtainLogPlannerResponse(t, testGenDir, runCtx, scalingPlanner, request)
}

func TestRedPodOnRedMaroonPools_SingleNodeMultiSim(t *testing.T) {
	monitor := startResourceMonitor(t)
	defer monitor.Stop()
	var request = plannerapi.Request{
		CreationTime:            time.Now(),
		Constraint:              loadScalingConstraint(t, "redmaroon"),
		SimulatorStrategy:       commontypes.SimulatorStrategySingleNodeMultiSim,
		ScoringStrategy:         commontypes.NodeScoringStrategyLeastCost,
		AdviceGenerationMode:    commontypes.ScalingAdviceGenerationModeAllAtOnce,
		AdviceGenerationTimeout: 2 * time.Minute,
		DiagnosticVerbosity:     uint32(5),
	}
	request.ID = t.Name()
	numPods := 3 // TODO: take from env variable or go test parameter
	request.Snapshot.Pods = genPodInfos(t, numPods, "red")
	testGenDir, _ := commontestutil.CreateTestGenDir(t)

	runCtx, scalingPlanner := CreateScalingPlanner(t, testGenDir, request.AdviceGenerationTimeout, request.DiagnosticVerbosity)
	_ = obtainLogPlannerResponse(t, testGenDir, runCtx, scalingPlanner, request)
}

func TestRedPodOnRedMaroonPools_MultiNodeSingleSim(t *testing.T) {
	monitor := startResourceMonitor(t)
	defer monitor.Stop()
	var request = plannerapi.Request{
		CreationTime:            time.Now(),
		Constraint:              loadScalingConstraint(t, "redmaroon"),
		SimulatorStrategy:       commontypes.SimulatorStrategyMultiNodeSingleSim,
		ScoringStrategy:         commontypes.NodeScoringStrategyLeastCost,
		AdviceGenerationMode:    commontypes.ScalingAdviceGenerationModeAllAtOnce,
		AdviceGenerationTimeout: 2 * time.Minute,
		DiagnosticVerbosity:     uint32(5),
	}
	request.ID = t.Name()
	numPods := 340 // TODO: take from env variable or go test parameter
	request.Snapshot.Pods = genPodInfos(t, numPods, "red")
	testGenDir, _ := commontestutil.CreateTestGenDir(t)

	runCtx, scalingPlanner := CreateScalingPlanner(t, testGenDir, request.AdviceGenerationTimeout, request.DiagnosticVerbosity)
	_ = obtainLogPlannerResponse(t, testGenDir, runCtx, scalingPlanner, request)
}

func obtainLogPlannerResponse(t *testing.T, genDir string, runContext context.Context, planner plannerapi.ScalingPlanner, request plannerapi.Request) (response plannerapi.Response) {
	responseCh := planner.Plan(runContext, request)
	response = <-responseCh
	if response.Error != nil {
		t.Fatalf("failed to generate scale-out plan: %v", response.Error)
		return
	}
	planResultJson, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal Response: %v", err)
	}
	t.Logf("Obtained plannerapi.Response %s", planResultJson)
	respJsonPath := path.Join(genDir, "response.json")
	if err = os.WriteFile(respJsonPath, planResultJson, 0600); err != nil {
		t.Fatal("failed to write response.json", err)
	}
	return
}

func loadScalingConstraint(t *testing.T, name string) *sacorev1alpha1.ScalingConstraint {
	var (
		err        error
		constraint sacorev1alpha1.ScalingConstraint
	)
	if err = objutil.LoadIntoRuntimeObj(testDataFS, "testdata/constraints/"+name+".yaml", &constraint); err != nil {
		err = fmt.Errorf("failed to load scaling constraints named %q: %w", name, err)
		t.Fatal(err)
	}
	return &constraint
}

func genPodInfos(t *testing.T, num int, names ...string) []plannerapi.PodInfo {
	var podInfos = make([]plannerapi.PodInfo, 0, num*len(names))
	for _, n := range names {
		pod := loadPod(t, n)
		podInfo := podutil.AsPodInfo(&pod)
		for i := range num {
			podInfo.Name = fmt.Sprintf("%s-%d", n, i+1)
			podInfos = append(podInfos, podInfo)
		}
	}
	return podInfos
}

func loadPod(t *testing.T, name string) corev1.Pod {
	var (
		err error
		pod corev1.Pod
	)
	if err = objutil.LoadIntoRuntimeObj(testDataFS, "testdata/pods/"+name+".yaml", &pod); err != nil {
		err = fmt.Errorf("failed to load pod named %q: %w", name, err)
		t.Fatal(err)
	}
	return pod
}

var (
	//go:embed testdata
	testDataFS embed.FS
)

// CreateTestScalingPlanner creates a ScalingPlanner for perf-tests
func CreateScalingPlanner(t *testing.T, traceDir string, timeout time.Duration, verbosity uint32) (runCtx context.Context, planr plannerapi.ScalingPlanner) {
	var (
		err       error
		factories = planner.NewFactories()
	)
	defer func() {
		if err != nil {
			t.Fatalf("failed to create test planner for test %q: %v", t.Name(), err)
			return
		}
	}()
	runCtx = commontestutil.NewTestContext(t, timeout, int(verbosity))
	pricingAccess, err := pricingtestutil.GetInstancePricingAccessForTop20AWSInstanceTypes()
	if err != nil {
		t.Fatalf("failed to get instance pricing access: %v", err)
		return
	}
	viewAccess, err := view.NewAccess(runCtx, &minkapi.ViewArgs{
		Name:   minkapi.DefaultBasePrefix,
		Scheme: typeinfo.SupportedScheme,
		WatchConfig: minkapi.WatchConfig{
			QueueSize: minkapi.DefaultWatchQueueSize,
			Timeout:   minkapi.DefaultWatchTimeout,
		},
	})
	if err != nil {
		err = fmt.Errorf("cannot create ViewAccess: %w", err)
		return
	}
	schedulerConfigBytes, err := samples.LoadBinPackingSchedulerConfig()
	if err != nil {
		err = fmt.Errorf("cannot load scheduler config: %w", err)
		return
	}
	var simulatorConfig = plannerapi.SimulatorConfig{
		MaxParallelSimulations:    plannerapi.DefaultMaxParallelSimulations,
		TrackPollInterval:         plannerapi.DefaultTrackPollInterval,
		MaxUnchangedTrackAttempts: 20,
	}
	simulatorConfig.BindVolumeClaimsForImmediateMode = true
	schedulerLauncher, err := scheduler.NewLauncherFromConfig(schedulerConfigBytes, simulatorConfig.MaxParallelSimulations)
	if err != nil {
		err = fmt.Errorf("cannot create SchedulerLauncher: %w", err)
		return
	}
	storageMetaAccess := samples.GetStorageMetaAccess(commontypes.CloudProviderAWS)
	scalePlannerArgs := plannerapi.ScalingPlannerArgs{
		ViewAccess:        viewAccess,
		ResourceWeigher:   factories.ResourceWeigher,
		PricingAccess:     pricingAccess,
		SchedulerLauncher: schedulerLauncher,
		StorageMetaAccess: storageMetaAccess,
		SimulatorConfig:   simulatorConfig,
		SimulatorFactory:  factories.Simulator,
		SimulationFactory: factories.Simulation,
		TraceDir:          traceDir,
	}
	planr, err = factories.Planner.NewPlanner(scalePlannerArgs)
	return
}

type ResourceMonitor struct {
	t        *testing.T
	begin    time.Time
	name     string
	done     chan struct{}
	wg       sync.WaitGroup
	PeakHeap uint64 // Go heap (most relevant for Go code)
	PeakSys  uint64 // Total memory claimed from OS (m.Sys)
}

// startResourceMonitor starts background mem monitoring
func startResourceMonitor(t *testing.T) *ResourceMonitor {
	t.Helper()

	m := &ResourceMonitor{
		t:     t,
		begin: time.Now(),
		name:  t.Name(),
		done:  make(chan struct{}),
	}

	m.wg.Add(1)
	go m.monitorLoop()

	return m
}

func (m *ResourceMonitor) monitorLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond) // Adjust interval if needed
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)

			if stats.Alloc > m.PeakHeap {
				m.PeakHeap = stats.Alloc
			}
			if stats.Sys > m.PeakSys {
				m.PeakSys = stats.Sys
			}
		}
	}
}

// Stop stops monitoring and logs the peaks.
func (m *ResourceMonitor) Stop() {
	close(m.done)
	duration := time.Since(m.begin)
	m.wg.Wait() // ensure monitorLoop finished

	heapMiB := float64(m.PeakHeap) / (1024 * 1024)
	sysMiB := float64(m.PeakSys) / (1024 * 1024)
	m.t.Logf("[%s] Duration     : %9s", m.name, duration.Round(time.Second))
	m.t.Logf("[%s] Peak Go Heap : %8.2f MiB", m.name, heapMiB)
	m.t.Logf("[%s] Peak Sys     : %8.2f MiB", m.name, sysMiB)
}
