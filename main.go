//go:generate go run pkg/codegen/cleanup/main.go
//go:generate go run pkg/codegen/main.go

package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/cnrancher/tke-operator/controller"
	tkev1 "github.com/cnrancher/tke-operator/pkg/generated/controllers/tke.pandaria.io"
	"github.com/google/uuid"
	"github.com/rancher/wrangler-api/pkg/generated/controllers/apps"
	core3 "github.com/rancher/wrangler/pkg/generated/controllers/core"
	"github.com/rancher/wrangler/pkg/kubeconfig"
	"github.com/rancher/wrangler/pkg/signals"
	"github.com/rancher/wrangler/pkg/start"
	"github.com/sirupsen/logrus"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var (
	masterURL      string
	kubeconfigFile string
	leaderElect    bool
	lockName       string
	lockNamespace  string
	id             string
	qps            float64
	burst          int
)

func init() {
	flag.StringVar(&kubeconfigFile, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&id, "id", uuid.New().String(), "The holder identity name.")
	flag.BoolVar(&leaderElect, "leader_elect", false, "If open leader election,default true.")
	flag.StringVar(&lockName, "lock_name", "tke-operator-pandaria-lock", "The lease lock resource name.")
	flag.StringVar(&lockNamespace, "lock_namespace", "cattle-system", "The lease lock resource namespace.")
	flag.Float64Var(&qps, "qps", float64(rest.DefaultQPS), "Param qps.")
	flag.IntVar(&burst, "burst", rest.DefaultBurst, "Param burst")
	flag.Parse()
}

func main() {
	// set up signals so we handle the first shutdown signal gracefully
	ctx := signals.SetupSignalContext()

	// This will load the kubeconfig file in a style the same as kubectl
	cfg, err := kubeconfig.GetNonInteractiveClientConfig(kubeconfigFile).ClientConfig()
	if err != nil {
		logrus.Fatalf("Error building kubeconfig: %s", err.Error())
	}
	cfg.Burst = burst
	cfg.QPS = float32(qps)

	client := clientset.NewForConfigOrDie(cfg)

	run := func(ctx context.Context) {
		// Generated apps controller
		apps := apps.NewFactoryFromConfigOrDie(cfg)
		// core
		core, err := core3.NewFactoryFromConfig(cfg)
		if err != nil {
			logrus.Fatalf("Error building core factory: %s", err.Error())
		}

		//Generated sample controller
		tke, err := tkev1.NewFactoryFromConfig(cfg)
		if err != nil {
			logrus.Fatalf("Error building tke factory: %s", err.Error())
		}

		//The typical pattern is to build all your controller/clients then just pass to each handler
		//the bare minimum of what they need.  This will eventually help with writing tests.  So
		//don't pass in something like kubeClient, apps, or sample
		controller.Register(ctx,
			core.Core().V1().Secret(),
			tke.Tke().V1().TKEClusterConfig())

		// Start all the controllers
		if err := start.All(ctx, 3, apps, tke, core); err != nil {
			logrus.Fatalf("error starting: %s", err.Error())
		}

		logrus.Infof("controller start.")

		<-ctx.Done()
	}

	if leaderElect {
		logrus.Infof("leader election is ON.")

		lock, lockErr := resourcelock.New(
			resourcelock.ConfigMapsResourceLock,
			lockNamespace,
			lockName,
			client.CoreV1(),
			client.CoordinationV1(),
			resourcelock.ResourceLockConfig{
				Identity: id,
			})
		if lockErr != nil {
			logrus.Fatalf("Error create lock: %s", lockErr.Error())
		}

		// start the leader election code loop
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock: lock,
			// IMPORTANT: you MUST ensure that any code you have that
			// is protected by the lease must terminate **before**
			// you call cancel. Otherwise, you could have a background
			// loop still running and another process could
			// get elected before your background loop finished, violating
			// the stated goal of the lease.
			ReleaseOnCancel: true,
			LeaseDuration:   60 * time.Second,
			RenewDeadline:   15 * time.Second,
			RetryPeriod:     5 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					run(ctx)
				},
				OnStoppedLeading: func() {
					logrus.Infof("leader lost: %s", id)
					os.Exit(0)
				},
				OnNewLeader: func(identity string) {
					// we're notified when new leader elected
					if identity == id {
						// I just got the lock
						return
					}
					logrus.Infof("leader elected: %s", identity)
				},
			},
		})
	} else {
		logrus.Infof("leader election is OFF.")
		run(ctx)
	}
}
