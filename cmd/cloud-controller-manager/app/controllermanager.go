/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/healthz"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	clientv1 "k8s.io/client-go/pkg/api/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/cmd/cloud-controller-manager/app/options"
	"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	"k8s.io/kubernetes/pkg/client/leaderelection"
	"k8s.io/kubernetes/pkg/client/leaderelection/resourcelock"
	"k8s.io/kubernetes/pkg/cloudprovider"
	"k8s.io/kubernetes/pkg/controller"
	nodecontroller "k8s.io/kubernetes/pkg/controller/cloud"
	"k8s.io/kubernetes/pkg/controller/informers"
	routecontroller "k8s.io/kubernetes/pkg/controller/route"
	servicecontroller "k8s.io/kubernetes/pkg/controller/service"
	"k8s.io/kubernetes/pkg/util/configz"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	// Jitter used when starting controller managers
	ControllerStartJitter = 1.0
)

// NewCloudControllerManagerCommand creates a *cobra.Command object with default parameters
func NewCloudControllerManagerCommand() *cobra.Command {
	s := options.NewCloudControllerManagerServer()
	s.AddFlags(pflag.CommandLine)
	cmd := &cobra.Command{
		Use: "cloud-controller-manager",
		Long: `The Cloud controller manager is a daemon that embeds
the cloud specific control loops shipped with Kubernetes.`,
		Run: func(cmd *cobra.Command, args []string) {
		},
	}

	return cmd
}

// resyncPeriod computes the time interval a shared informer waits before resyncing with the api server
func resyncPeriod(s *options.CloudControllerManagerServer) func() time.Duration {
	return func() time.Duration {
		factor := rand.Float64() + 1
		return time.Duration(float64(s.MinResyncPeriod.Nanoseconds()) * factor)
	}
}

// Run runs the ExternalCMServer.  This should never exit.
func Run(s *options.CloudControllerManagerServer, cloud cloudprovider.Interface) error {
	if c, err := configz.New("componentconfig"); err == nil {
		c.Set(s.KubeControllerManagerConfiguration)
	} else {
		glog.Errorf("unable to register configz: %s", err)
	}
	kubeconfig, err := clientcmd.BuildConfigFromFlags(s.Master, s.Kubeconfig)
	if err != nil {
		return err
	}

	// Set the ContentType of the requests from kube client
	kubeconfig.ContentConfig.ContentType = s.ContentType
	// Override kubeconfig qps/burst settings from flags
	kubeconfig.QPS = s.KubeAPIQPS
	kubeconfig.Burst = int(s.KubeAPIBurst)
	kubeClient, err := clientset.NewForConfig(restclient.AddUserAgent(kubeconfig, "cloud-controller-manager"))
	if err != nil {
		glog.Fatalf("Invalid API configuration: %v", err)
	}
	leaderElectionClient := clientset.NewForConfigOrDie(restclient.AddUserAgent(kubeconfig, "leader-election"))

	// Start the external controller manager server
	go func() {
		mux := http.NewServeMux()
		healthz.InstallHandler(mux)
		if s.EnableProfiling {
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		}
		configz.InstallHandler(mux)
		mux.Handle("/metrics", prometheus.Handler())

		server := &http.Server{
			Addr:    net.JoinHostPort(s.Address, strconv.Itoa(int(s.Port))),
			Handler: mux,
		}
		glog.Fatal(server.ListenAndServe())
	}()

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.Core().RESTClient()).Events("")})
	recorder := eventBroadcaster.NewRecorder(api.Scheme, clientv1.EventSource{Component: "cloud-controller-manager"})

	run := func(stop <-chan struct{}) {
		rootClientBuilder := controller.SimpleControllerClientBuilder{
			ClientConfig: kubeconfig,
		}
		var clientBuilder controller.ControllerClientBuilder
		if len(s.ServiceAccountKeyFile) > 0 && s.UseServiceAccountCredentials {
			clientBuilder = controller.SAControllerClientBuilder{
				ClientConfig: restclient.AnonymousClientConfig(kubeconfig),
				CoreClient:   kubeClient.Core(),
				Namespace:    "kube-system",
			}
		} else {
			clientBuilder = rootClientBuilder
		}

		err := StartControllers(s, kubeconfig, rootClientBuilder, clientBuilder, stop, recorder, cloud)
		glog.Fatalf("error running controllers: %v", err)
		panic("unreachable")
	}

	if !s.LeaderElection.LeaderElect {
		run(nil)
		panic("unreachable")
	}

	// Identity used to distinguish between multiple cloud controller manager instances
	id, err := os.Hostname()
	if err != nil {
		return err
	}

	// Lock required for leader election
	rl := resourcelock.EndpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "cloud-controller-manager",
		},
		Client: leaderElectionClient,
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      id + "-external-cloud-controller",
			EventRecorder: recorder,
		},
	}

	// Try and become the leader and start cloud controller manager loops
	leaderelection.RunOrDie(leaderelection.LeaderElectionConfig{
		Lock:          &rl,
		LeaseDuration: s.LeaderElection.LeaseDuration.Duration,
		RenewDeadline: s.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:   s.LeaderElection.RetryPeriod.Duration,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				glog.Fatalf("leaderelection lost")
			},
		},
	})
	panic("unreachable")
}

// StartControllers starts the cloud specific controller loops.
func StartControllers(s *options.CloudControllerManagerServer, kubeconfig *restclient.Config, rootClientBuilder, clientBuilder controller.ControllerClientBuilder, stop <-chan struct{}, recorder record.EventRecorder, cloud cloudprovider.Interface) error {
	// Function to build the kube client object
	client := func(serviceAccountName string) clientset.Interface {
		return rootClientBuilder.ClientOrDie(serviceAccountName)
	}
	sharedInformers := informers.NewSharedInformerFactory(client("shared-informers"), nil, resyncPeriod(s)())

	_, clusterCIDR, err := net.ParseCIDR(s.ClusterCIDR)
	if err != nil {
		glog.Warningf("Unsuccessful parsing of cluster CIDR %v: %v", s.ClusterCIDR, err)
	}

	// Start the CloudNodeController
	nodeController, err := nodecontroller.NewCloudNodeController(
		sharedInformers.Nodes(),
		client("cloud-node-controller"), cloud,
		s.NodeMonitorPeriod.Duration)
	if err != nil {
		glog.Fatalf("Failed to initialize nodecontroller: %v", err)
	}
	nodeController.Run()
	time.Sleep(wait.Jitter(s.ControllerStartInterval.Duration, ControllerStartJitter))

	// Start the service controller
	serviceController, err := servicecontroller.New(cloud, client("service-controller"), s.ClusterName)
	if err != nil {
		glog.Errorf("Failed to start service controller: %v", err)
	} else {
		serviceController.Run(int(s.ConcurrentServiceSyncs))
	}
	time.Sleep(wait.Jitter(s.ControllerStartInterval.Duration, ControllerStartJitter))

	// If CIDRs should be allocated for pods and set on the CloudProvider, then start the route controller
	if s.AllocateNodeCIDRs && s.ConfigureCloudRoutes {
		if routes, ok := cloud.Routes(); !ok {
			glog.Warning("configure-cloud-routes is set, but cloud provider does not support routes. Will not configure cloud provider routes.")
		} else {
			routeController := routecontroller.New(routes, client("route-controller"), s.ClusterName, clusterCIDR)
			routeController.Run(s.RouteReconciliationPeriod.Duration)
			time.Sleep(wait.Jitter(s.ControllerStartInterval.Duration, ControllerStartJitter))
		}
	} else {
		glog.Infof("Will not configure cloud provider routes for allocate-node-cidrs: %v, configure-cloud-routes: %v.", s.AllocateNodeCIDRs, s.ConfigureCloudRoutes)
	}

	// If apiserver is not running we should wait for some time and fail only then. This is particularly
	// important when we start apiserver and controller manager at the same time.
	var versionStrings []string
	err = wait.PollImmediate(time.Second, 10*time.Second, func() (bool, error) {
		if versionStrings, err = restclient.ServerAPIVersions(kubeconfig); err == nil {
			return true, nil
		}
		glog.Errorf("Failed to get api versions from server: %v", err)
		return false, nil
	})
	if err != nil {
		glog.Fatalf("Failed to get api versions from server: %v", err)
	}

	sharedInformers.Start(stop)

	select {}
}