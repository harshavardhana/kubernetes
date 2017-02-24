/*
Copyright 2017 The Kubernetes Authors.

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

package minfs

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path"
	"runtime"
	"strconv"
	dstrings "strings"
	"sync"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/api/v1"
	storageutil "k8s.io/kubernetes/pkg/apis/storage/v1beta1/util"
	"k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	"k8s.io/kubernetes/pkg/util/exec"
	"k8s.io/kubernetes/pkg/util/mount"
	"k8s.io/kubernetes/pkg/util/strings"
	"k8s.io/kubernetes/pkg/volume"
	volutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/volumehelper"
)

// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&minfsPlugin{host: nil, exe: exec.New(), gidTable: make(map[string]*MinMaxAllocator)}}
}

type minfsPlugin struct {
	host         volume.VolumeHost
	exe          exec.Interface
	gidTable     map[string]*MinMaxAllocator
	gidTableLock sync.Mutex
}

var _ volume.VolumePlugin = &minfsPlugin{}
var _ volume.PersistentVolumePlugin = &minfsPlugin{}
var _ volume.DeletableVolumePlugin = &minfsPlugin{}
var _ volume.ProvisionableVolumePlugin = &minfsPlugin{}
var _ volume.Provisioner = &minfsVolumeProvisioner{}
var _ volume.Deleter = &minfsVolumeDeleter{}

const (
	minfsPluginName           = "kubernetes.io/minfs"
	volPrefix                 = "vol_"
	dynamicEpSvcPrefix        = "minfs-dynamic-"
	replicaCount              = 3
	durabilityType            = "replicate"
	secretKeyName             = "key" // key name used in secret
	gciMinfsMountBinariesPath = "/sbin/mount.minfs"
	defaultGidMin             = 2000
	defaultGidMax             = math.MaxInt32
	// absoluteGidMin/Max are currently the same as the
	// default values, but they play a different role and
	// could take a different value. Only thing we need is:
	// absGidMin <= defGidMin <= defGidMax <= absGidMax
	absoluteGidMin = 2000
	absoluteGidMax = math.MaxInt32
)

func (plugin *minfsPlugin) Init(host volume.VolumeHost) error {
	plugin.host = host
	return nil
}

func (plugin *minfsPlugin) GetPluginName() string {
	return minfsPluginName
}

func (plugin *minfsPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
	volumeSource, _, err := getVolumeSource(spec)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%v:%v",
		volumeSource.EndpointsName,
		volumeSource.Path), nil
}

func (plugin *minfsPlugin) CanSupport(spec *volume.Spec) bool {
	if (spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Minfs == nil) ||
		(spec.Volume != nil && spec.Volume.Minfs == nil) {
		return false
	}

	return true
}

func (plugin *minfsPlugin) RequiresRemount() bool {
	return false
}

func (plugin *minfsPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {
	return []v1.PersistentVolumeAccessMode{
		v1.ReadWriteOnce,
		v1.ReadOnlyMany,
		v1.ReadWriteMany,
	}
}

func (plugin *minfsPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {
	source, _ := plugin.getMinfsVolumeSource(spec)
	epName := source.EndpointsName
	// PVC/POD is in same ns.
	ns := pod.Namespace
	kubeClient := plugin.host.GetKubeClient()
	if kubeClient == nil {
		return nil, fmt.Errorf("minfs: failed to get kube client to initialize mounter")
	}
	ep, err := kubeClient.Core().Endpoints(ns).Get(epName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("minfs: failed to get endpoints %s[%v]", epName, err)
		return nil, err
	}
	glog.V(1).Infof("minfs: endpoints %v", ep)
	return plugin.newMounterInternal(spec, ep, pod, plugin.host.GetMounter(), exec.New())
}

func (plugin *minfsPlugin) getMinfsVolumeSource(spec *volume.Spec) (*v1.MinfsVolumeSource, bool) {
	// Minfs volumes used directly in a pod have a ReadOnly flag set by the pod author.
	// Minfs volumes used as a PersistentVolume gets the ReadOnly flag indirectly through the persistent-claim volume used to mount the PV
	if spec.Volume != nil && spec.Volume.Minfs != nil {
		return spec.Volume.Minfs, spec.Volume.Minfs.ReadOnly
	} // else {
	return spec.PersistentVolume.Spec.Minfs, spec.ReadOnly
}

func (plugin *minfsPlugin) newMounterInternal(spec *volume.Spec, ep *v1.Endpoints, pod *v1.Pod, mounter mount.Interface, exe exec.Interface) (volume.Mounter, error) {
	source, readOnly := plugin.getMinfsVolumeSource(spec)
	return &minfsMounter{
		minfs: &minfs{
			volName: spec.Name(),
			mounter: mounter,
			pod:     pod,
			plugin:  plugin,
		},
		hosts:    ep,
		path:     source.Path,
		readOnly: readOnly,
		exe:      exe}, nil
}

func (plugin *minfsPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {
	return plugin.newUnmounterInternal(volName, podUID, plugin.host.GetMounter())
}

func (plugin *minfsPlugin) newUnmounterInternal(volName string, podUID types.UID, mounter mount.Interface) (volume.Unmounter, error) {
	return &minfsUnmounter{&minfs{
		volName: volName,
		mounter: mounter,
		pod:     &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: podUID}},
		plugin:  plugin,
	}}, nil
}

func (plugin *minfsPlugin) execCommand(command string, args []string) ([]byte, error) {
	cmd := plugin.exe.Command(command, args...)
	return cmd.CombinedOutput()
}

func (plugin *minfsPlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {
	minfsVolume := &v1.Volume{
		Name: volumeName,
		VolumeSource: v1.VolumeSource{
			Minfs: &v1.MinfsVolumeSource{
				EndpointsName: volumeName,
				Path:          volumeName,
			},
		},
	}
	return volume.NewSpecFromVolume(minfsVolume), nil
}

// Minfs volumes represent a bare host file or directory mount of an Minfs export.
type minfs struct {
	volName string
	pod     *v1.Pod
	mounter mount.Interface
	plugin  *minfsPlugin
	volume.MetricsNil
}

type minfsMounter struct {
	*minfs
	hosts    *v1.Endpoints
	path     string
	readOnly bool
	exe      exec.Interface
}

var _ volume.Mounter = &minfsMounter{}

func (b *minfsMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{
		ReadOnly:        b.readOnly,
		Managed:         false,
		SupportsSELinux: false,
	}
}

// Checks prior to mount operations to verify that the required components (binaries, etc.)
// to mount the volume are available on the underlying node.
// If not, it returns an error
func (b *minfsMounter) CanMount() error {
	exe := exec.New()
	switch runtime.GOOS {
	case "linux":
		if _, err := exe.Command("/bin/ls", gciMinfsMountBinariesPath).CombinedOutput(); err != nil {
			return fmt.Errorf("Required binary %s is missing", gciMinfsMountBinariesPath)
		}
	}
	return nil
}

// SetUp attaches the disk and bind mounts to the volume path.
func (b *minfsMounter) SetUp(fsGroup *int64) error {
	return b.SetUpAt(b.GetPath(), fsGroup)
}

func (b *minfsMounter) SetUpAt(dir string, fsGroup *int64) error {
	notMnt, err := b.mounter.IsLikelyNotMountPoint(dir)
	glog.V(4).Infof("minfs: mount set up: %s %v %v", dir, !notMnt, err)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if !notMnt {
		return nil
	}

	os.MkdirAll(dir, 0750)
	err = b.setUpAtInternal(dir)
	if err == nil {
		return nil
	}

	// Cleanup upon failure.
	volutil.UnmountPath(dir, b.mounter)
	return err
}

func (minfsVolume *minfs) GetPath() string {
	name := minfsPluginName
	return minfsVolume.plugin.host.GetPodVolumeDir(minfsVolume.pod.UID, strings.EscapeQualifiedNameForDisk(name), minfsVolume.volName)
}

type minfsUnmounter struct {
	*minfs
}

var _ volume.Unmounter = &minfsUnmounter{}

func (c *minfsUnmounter) TearDown() error {
	return c.TearDownAt(c.GetPath())
}

func (c *minfsUnmounter) TearDownAt(dir string) error {
	return volutil.UnmountPath(dir, c.mounter)
}

func (b *minfsMounter) setUpAtInternal(dir string) error {
	var errs error

	options := []string{}
	if b.readOnly {
		options = append(options, "ro")
	}

	p := path.Join(b.minfs.plugin.host.GetPluginDir(minfsPluginName), b.minfs.volName)
	if err := os.MkdirAll(p, 0750); err != nil {
		return fmt.Errorf("minfs: mkdir failed: %v", err)
	}

	var addrlist []string
	if b.hosts == nil {
		return fmt.Errorf("minfs: endpoint is nil")
	}
	addr := make(map[string]struct{})
	if b.hosts.Subsets != nil {
		for _, s := range b.hosts.Subsets {
			for _, a := range s.Addresses {
				addr[a.IP] = struct{}{}
				addrlist = append(addrlist, a.IP)
			}
		}

	}

	// Avoid mount storm, pick a host randomly.
	// Iterate all hosts until mount succeeds.
	for _, ip := range addrlist {
		errs = b.mounter.Mount(ip+":"+b.path, dir, "minfs", options)
		if errs == nil {
			glog.Infof("minfs: successfully mounted %s", dir)
			return nil
		}
	}

	// Failed mount scenario.
	// Since minfs does not return error text
	// it all goes in a log file, we will read the log file
	logerror := readMinfsLog(b.pod.Name)
	if logerror != nil {
		// return fmt.Errorf("minfs: mount failed: %v", logerror)
		return fmt.Errorf("minfs: mount failed: %v the following error information was pulled from the minfs log to help diagnose this issue: %v", errs, logerror)
	}

	return fmt.Errorf("minfs: mount failed: %v", errs)
}

// readMinfsLog will take the last 2 lines of the log file
// on failure of Minfs SetUp and return those so kubelet can
// properly expose them. return nil on any failure
func readMinfsLog(podName string) error {

	var line1 string
	var line2 string
	linecount := 0

	glog.Infof("minfs: failure, now attempting to read the minfs log for pod %s", podName)

	// open the log file
	file, err := os.Open("/var/log/minfs.log")
	if err != nil {
		return fmt.Errorf("minfs: could not open log file for pod: %s", podName)
	}
	defer file.Close()

	// read in and scan the file using scanner
	// from stdlib
	fscan := bufio.NewScanner(file)

	// rather than guessing on bytes or using Seek
	// going to scan entire file and take the last two lines
	// generally the file should be small since it is pod specific
	for fscan.Scan() {
		if linecount > 0 {
			line1 = line2
		}
		line2 = "\n" + fscan.Text()

		linecount++
	}

	if linecount > 0 {
		return fmt.Errorf("%v", line1+line2+"\n")
	}
	return nil
}

func getVolumeSource(
	spec *volume.Spec) (*v1.MinfsVolumeSource, bool, error) {
	if spec.Volume != nil && spec.Volume.Minfs != nil {
		return spec.Volume.Minfs, spec.Volume.Minfs.ReadOnly, nil
	} else if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.Minfs != nil {
		return spec.PersistentVolume.Spec.Minfs, spec.ReadOnly, nil
	}

	return nil, false, fmt.Errorf("Spec does not reference a Minfs volume type")
}

func (plugin *minfsPlugin) NewProvisioner(options volume.VolumeOptions) (volume.Provisioner, error) {
	return plugin.newProvisionerInternal(options)
}

func (plugin *minfsPlugin) newProvisionerInternal(options volume.VolumeOptions) (volume.Provisioner, error) {
	return &minfsVolumeProvisioner{
		minfsMounter: &minfsMounter{
			minfs: &minfs{
				plugin: plugin,
			},
		},
		options: options,
	}, nil
}

type provisionerConfig struct {
	url             string
	user            string
	userKey         string
	secretNamespace string
	secretName      string
	secretValue     string
	clusterId       string
	gidMin          int
	gidMax          int
	volumeType      gapi.VolumeDurabilityInfo
}

type minfsVolumeProvisioner struct {
	*minfsMounter
	provisionerConfig
	options volume.VolumeOptions
}

func convertVolumeParam(volumeString string) (int, error) {
	count, err := strconv.Atoi(volumeString)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %q", volumeString)
	}

	if count < 0 {
		return 0, fmt.Errorf("negative values are not allowed")
	}
	return count, nil
}

func (plugin *minfsPlugin) NewDeleter(spec *volume.Spec) (volume.Deleter, error) {
	return plugin.newDeleterInternal(spec)
}

func (plugin *minfsPlugin) newDeleterInternal(spec *volume.Spec) (volume.Deleter, error) {
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.Minfs == nil {
		return nil, fmt.Errorf("spec.PersistentVolumeSource.Spec.Minfs is nil")
	}
	return &minfsVolumeDeleter{
		minfsMounter: &minfsMounter{
			minfs: &minfs{
				volName: spec.Name(),
				plugin:  plugin,
			},
			path: spec.PersistentVolume.Spec.Minfs.Path,
		},
		spec: spec.PersistentVolume,
	}, nil
}

type minfsVolumeDeleter struct {
	*minfsMounter
	provisionerConfig
	spec *v1.PersistentVolume
}

func (d *minfsVolumeDeleter) GetPath() string {
	name := minfsPluginName
	return d.plugin.host.GetPodVolumeDir(d.minfsMounter.minfs.pod.UID, strings.EscapeQualifiedNameForDisk(name), d.minfsMounter.minfs.volName)
}

//
// Traverse the PVs, fetching all the GIDs from those
// in a given storage class, and mark them in the table.
//
func (r *minfsPlugin) collectGids(className string, gidTable *MinMaxAllocator) error {
	kubeClient := r.host.GetKubeClient()
	if kubeClient == nil {
		return fmt.Errorf("minfs: failed to get kube client when collecting gids")
	}
	pvList, err := kubeClient.Core().PersistentVolumes().List(metav1.ListOptions{LabelSelector: labels.Everything().String()})
	if err != nil {
		glog.Errorf("minfs: failed to get existing persistent volumes")
		return err
	}

	for _, pv := range pvList.Items {
		if storageutil.GetVolumeStorageClass(&pv) != className {
			continue
		}

		pvName := pv.ObjectMeta.Name

		gidStr, ok := pv.Annotations[volumehelper.VolumeGidAnnotationKey]

		if !ok {
			glog.Warningf("minfs: no gid found in pv '%v'", pvName)
			continue
		}

		gid, err := convertGid(gidStr)
		if err != nil {
			glog.Error(err)
			continue
		}

		_, err = gidTable.Allocate(gid)
		if err == ErrConflict {
			glog.Warningf("minfs: gid %v found in pv %v was already allocated", gid)
		} else if err != nil {
			glog.Errorf("minfs: failed to store gid %v found in pv '%v': %v", gid, pvName, err)
			return err
		}
	}

	return nil
}

//
// Return the gid table for a storage class.
// - If this is the first time, fill it with all the gids
//   used in PVs of this storage class by traversing the PVs.
// - Adapt the range of the table to the current range of the SC.
//
func (r *minfsPlugin) getGidTable(className string, min int, max int) (*MinMaxAllocator, error) {
	var err error
	r.gidTableLock.Lock()
	gidTable, ok := r.gidTable[className]
	r.gidTableLock.Unlock()

	if ok {
		err = gidTable.SetRange(min, max)
		if err != nil {
			return nil, err
		}

		return gidTable, nil
	}

	// create a new table and fill it
	newGidTable, err := NewMinMaxAllocator(0, absoluteGidMax)
	if err != nil {
		return nil, err
	}

	// collect gids with the full range
	err = r.collectGids(className, newGidTable)
	if err != nil {
		return nil, err
	}

	// and only reduce the range afterwards
	err = newGidTable.SetRange(min, max)
	if err != nil {
		return nil, err
	}

	// if in the meantime a table appeared, use it

	r.gidTableLock.Lock()
	defer r.gidTableLock.Unlock()

	gidTable, ok = r.gidTable[className]
	if ok {
		err = gidTable.SetRange(min, max)
		if err != nil {
			return nil, err
		}

		return gidTable, nil
	}

	r.gidTable[className] = newGidTable

	return newGidTable, nil
}

func (d *minfsVolumeDeleter) getGid() (int, bool, error) {
	gidStr, ok := d.spec.Annotations[volumehelper.VolumeGidAnnotationKey]

	if !ok {
		return 0, false, nil
	}

	gid, err := convertGid(gidStr)

	return gid, true, err
}

func (d *minfsVolumeDeleter) Delete() error {
	var err error
	glog.V(2).Infof("minfs: delete volume: %s ", d.minfsMounter.path)
	volumeName := d.minfsMounter.path
	volumeID := dstrings.TrimPrefix(volumeName, volPrefix)
	class, err := volutil.GetClassForVolume(d.plugin.host.GetKubeClient(), d.spec)
	if err != nil {
		return err
	}

	cfg, err := parseClassParameters(class.Parameters, d.plugin.host.GetKubeClient())
	if err != nil {
		return err
	}
	d.provisionerConfig = *cfg

	glog.V(4).Infof("minfs: deleting volume %q with configuration %+v", volumeID, d.provisionerConfig)

	gid, exists, err := d.getGid()
	if err != nil {
		glog.Error(err)
	} else if exists {
		gidTable, err := d.plugin.getGidTable(class.Name, cfg.gidMin, cfg.gidMax)
		if err != nil {
			return fmt.Errorf("minfs: failed to get gidTable: %v", err)
		}

		err = gidTable.Release(gid)
		if err != nil {
			return fmt.Errorf("minfs: failed to release gid %v: %v", gid, err)
		}
	}

	cli := gcli.NewClient(d.url, d.user, d.secretValue)
	if cli == nil {
		glog.Errorf("minfs: failed to create minfs rest client")
		return fmt.Errorf("minfs: failed to create minfs rest client, REST server authentication failed")
	}
	err = cli.VolumeDelete(volumeID)
	if err != nil {
		glog.Errorf("minfs: error when deleting the volume :%v", err)
		return err
	}
	glog.V(2).Infof("minfs: volume %s deleted successfully", volumeName)

	//Deleter takes endpoint and endpointnamespace from pv spec.
	pvSpec := d.spec.Spec
	var dynamicEndpoint, dynamicNamespace string
	if pvSpec.ClaimRef == nil {
		glog.Errorf("minfs: ClaimRef is nil")
		return fmt.Errorf("minfs: ClaimRef is nil")
	}
	if pvSpec.ClaimRef.Namespace == "" {
		glog.Errorf("minfs: namespace is nil")
		return fmt.Errorf("minfs: namespace is nil")
	}
	dynamicNamespace = pvSpec.ClaimRef.Namespace
	if pvSpec.Minfs.EndpointsName != "" {
		dynamicEndpoint = pvSpec.Minfs.EndpointsName
	}
	glog.V(3).Infof("minfs: dynamic namespace and endpoint : [%v/%v]", dynamicNamespace, dynamicEndpoint)
	err = d.deleteEndpointService(dynamicNamespace, dynamicEndpoint)
	if err != nil {
		glog.Errorf("minfs: error when deleting endpoint/service :%v", err)
	} else {
		glog.V(1).Infof("minfs: [%v/%v] deleted successfully ", dynamicNamespace, dynamicEndpoint)
	}
	return nil
}

func (r *minfsVolumeProvisioner) Provision() (*v1.PersistentVolume, error) {
	var err error
	if r.options.PVC.Spec.Selector != nil {
		glog.V(4).Infof("minfs: not able to parse your claim Selector")
		return nil, fmt.Errorf("minfs: not able to parse your claim Selector")
	}
	glog.V(4).Infof("minfs: Provison VolumeOptions %v", r.options)
	scName := storageutil.GetClaimStorageClass(r.options.PVC)
	cfg, err := parseClassParameters(r.options.Parameters, r.plugin.host.GetKubeClient())
	if err != nil {
		return nil, err
	}
	r.provisionerConfig = *cfg

	glog.V(4).Infof("minfs: creating volume with configuration %+v", r.provisionerConfig)

	gidTable, err := r.plugin.getGidTable(scName, cfg.gidMin, cfg.gidMax)
	if err != nil {
		return nil, fmt.Errorf("minfs: failed to get gidTable: %v", err)
	}

	gid, _, err := gidTable.AllocateNext()
	if err != nil {
		glog.Errorf("minfs: failed to reserve gid from table: %v", err)
		return nil, fmt.Errorf("minfs: failed to reserve gid from table: %v", err)
	}

	glog.V(2).Infof("minfs: got gid [%d] for PVC %s", gid, r.options.PVC.Name)

	minfs, sizeGB, err := r.CreateVolume(gid)
	if err != nil {
		if release_err := gidTable.Release(gid); release_err != nil {
			glog.Errorf("minfs:  error when releasing gid in storageclass: %s", scName)
		}

		glog.Errorf("minfs: create volume err: %v.", err)
		return nil, fmt.Errorf("minfs: create volume err: %v.", err)
	}
	pv := new(v1.PersistentVolume)
	pv.Spec.PersistentVolumeSource.Minfs = minfs
	pv.Spec.PersistentVolumeReclaimPolicy = r.options.PersistentVolumeReclaimPolicy
	pv.Spec.AccessModes = r.options.PVC.Spec.AccessModes
	if len(pv.Spec.AccessModes) == 0 {
		pv.Spec.AccessModes = r.plugin.GetAccessModes()
	}

	gidStr := strconv.FormatInt(int64(gid), 10)
	pv.Annotations = map[string]string{volumehelper.VolumeGidAnnotationKey: gidStr}

	pv.Spec.Capacity = v1.ResourceList{
		v1.ResourceName(v1.ResourceStorage): resource.MustParse(fmt.Sprintf("%dGi", sizeGB)),
	}
	return pv, nil
}

func (r *minfsVolumeProvisioner) CreateVolume(gid int) (r *v1.MinfsVolumeSource, size int, err error) {
	var clusterIds []string
	capacity := r.options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	volSizeBytes := capacity.Value()
	sz := int(volume.RoundUpSize(volSizeBytes, 1024*1024*1024))
	glog.V(2).Infof("minfs: create volume of size: %d bytes and configuration %+v", volSizeBytes, r.provisionerConfig)
	if r.url == "" {
		glog.Errorf("minfs : rest server endpoint is empty")
		return nil, 0, fmt.Errorf("failed to create minfs REST client, REST URL is empty")
	}
	cli := gcli.NewClient(r.url, r.user, r.secretValue)
	if cli == nil {
		glog.Errorf("minfs: failed to create minfs rest client")
		return nil, 0, fmt.Errorf("failed to create minfs REST client, REST server authentication failed")
	}
	if r.provisionerConfig.clusterId != "" {
		clusterIds = dstrings.Split(r.clusterID, ",")
		glog.V(4).Infof("minfs: provided clusterids: %v", clusterIds)
	}
	gid64 := int64(gid)
	volumeReq := &gapi.VolumeCreateRequest{Size: sz, Clusters: clusterIds, Gid: gid64, Durability: r.volumeType}
	volume, err := cli.VolumeCreate(volumeReq)
	if err != nil {
		glog.Errorf("minfs: error creating volume %v ", err)
		return nil, 0, fmt.Errorf("error creating volume %v", err)
	}
	glog.V(1).Infof("minfs: volume with size: %d and name: %s created", volume.Size, volume.Name)
	clusterinfo, err := cli.ClusterInfo(volume.Cluster)
	if err != nil {
		glog.Errorf("minfs: failed to get cluster details: %v", err)
		return nil, 0, fmt.Errorf("failed to get cluster details: %v", err)
	}
	// For the above dynamically provisioned volume, we gather the list of node IPs
	// of the cluster on which provisioned volume belongs to, as there can be multiple
	// clusters.
	var dynamicHostIps []string
	for _, node := range clusterinfo.Nodes {
		nodei, err := cli.NodeInfo(string(node))
		if err != nil {
			glog.Errorf("minfs: failed to get hostip: %v", err)
			return nil, 0, fmt.Errorf("failed to get hostip: %v", err)
		}
		ipaddr := dstrings.Join(nodei.NodeAddRequest.Hostnames.Storage, "")
		dynamicHostIps = append(dynamicHostIps, ipaddr)
	}
	glog.V(3).Infof("minfs: hostlist :%v", dynamicHostIps)
	if len(dynamicHostIps) == 0 {
		glog.Errorf("minfs: no hosts found: %v", err)
		return nil, 0, fmt.Errorf("no hosts found: %v", err)
	}

	// The 'endpointname' is created in form of 'gluster-dynamic-<claimname>'.
	// createEndpointService() checks for this 'endpoint' existence in PVC's namespace and
	// If not found, it create an endpoint and svc using the IPs we dynamically picked at time
	// of volume creation.
	epServiceName := dynamicEpSvcPrefix + r.options.PVC.Name
	epNamespace := r.options.PVC.Namespace
	endpoint, service, err := r.createEndpointService(epNamespace, epServiceName, dynamicHostIps, r.options.PVC.Name)
	if err != nil {
		glog.Errorf("minfs: failed to create endpoint/service: %v", err)
		err = cli.VolumeDelete(volume.Id)
		if err != nil {
			glog.Errorf("minfs: error when deleting the volume :%v , manual deletion required", err)
		}
		return nil, 0, fmt.Errorf("failed to create endpoint/service %v", err)
	}
	glog.V(3).Infof("minfs: dynamic ep %v and svc : %v ", endpoint, service)
	return &v1.MinfsVolumeSource{
		EndpointsName: endpoint.Name,
		Path:          volume.Name,
		ReadOnly:      false,
	}, sz, nil
}

func (r *minfsVolumeProvisioner) createEndpointService(namespace string, epServiceName string, hostips []string, pvcname string) (endpoint *v1.Endpoints, service *v1.Service, err error) {

	addrlist := make([]v1.EndpointAddress, len(hostips))
	for i, v := range hostips {
		addrlist[i].IP = v
	}
	endpoint = &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      epServiceName,
			Labels: map[string]string{
				"minfs.kubernetes.io/provisioned-for-pvc": pvcname,
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: addrlist,
			Ports:     []v1.EndpointPort{{Port: 1, Protocol: "TCP"}},
		}},
	}
	kubeClient := r.plugin.host.GetKubeClient()
	if kubeClient == nil {
		return nil, nil, fmt.Errorf("minfs: failed to get kube client when creating endpoint service")
	}
	_, err = kubeClient.Core().Endpoints(namespace).Create(endpoint)
	if err != nil && errors.IsAlreadyExists(err) {
		glog.V(1).Infof("minfs: endpoint [%s] already exist in namespace [%s]", endpoint, namespace)
		err = nil
	}
	if err != nil {
		glog.Errorf("minfs: failed to create endpoint: %v", err)
		return nil, nil, fmt.Errorf("error creating endpoint: %v", err)
	}
	service = &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      epServiceName,
			Namespace: namespace,
			Labels: map[string]string{
				"minfs.kubernetes.io/provisioned-for-pvc": pvcname,
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{Protocol: "TCP", Port: 1}}}}
	_, err = kubeClient.Core().Services(namespace).Create(service)
	if err != nil && errors.IsAlreadyExists(err) {
		glog.V(1).Infof("minfs: service [%s] already exist in namespace [%s]", service, namespace)
		err = nil
	}
	if err != nil {
		glog.Errorf("minfs: failed to create service: %v", err)
		return nil, nil, fmt.Errorf("error creating service: %v", err)
	}
	return endpoint, service, nil
}

func (d *minfsVolumeDeleter) deleteEndpointService(namespace string, epServiceName string) (err error) {
	kubeClient := d.plugin.host.GetKubeClient()
	if kubeClient == nil {
		return fmt.Errorf("minfs: failed to get kube client when deleting endpoint service")
	}
	err = kubeClient.Core().Services(namespace).Delete(epServiceName, nil)
	if err != nil {
		glog.Errorf("minfs: error deleting service %s/%s: %v", namespace, epServiceName, err)
		return fmt.Errorf("error deleting service %s/%s: %v", namespace, epServiceName, err)
	}
	glog.V(1).Infof("minfs: service/endpoint %s/%s deleted successfully", namespace, epServiceName)
	return nil
}

// parseSecret finds a given Secret instance and reads user password from it.
func parseSecret(namespace, secretName string, kubeClient clientset.Interface) (string, error) {
	secretMap, err := volutil.GetSecretForPV(namespace, secretName, minfsPluginName, kubeClient)
	if err != nil {
		glog.Errorf("failed to get secret %s/%s: %v", namespace, secretName, err)
		return "", fmt.Errorf("failed to get secret %s/%s: %v", namespace, secretName, err)
	}
	if len(secretMap) == 0 {
		return "", fmt.Errorf("empty secret map")
	}
	secret := ""
	for k, v := range secretMap {
		if k == secretKeyName {
			return v, nil
		}
		secret = v
	}
	// If not found, the last secret in the map wins as done before
	return secret, nil
}

// parseClassParameters parses StorageClass.Parameters
func parseClassParameters(params map[string]string, kubeClient clientset.Interface) (*provisionerConfig, error) {
	var cfg provisionerConfig
	var err error

	cfg.gidMin = defaultGidMin
	cfg.gidMax = defaultGidMax

	authEnabled := true
	parseVolumeType := ""
	for k, v := range params {
		switch dstrings.ToLower(k) {
		case "resturl":
			cfg.url = v
		case "restuser":
			cfg.user = v
		case "restuserkey":
			cfg.userKey = v
		case "secretname":
			cfg.secretName = v
		case "secretnamespace":
			cfg.secretNamespace = v
		case "clusterid":
			if len(v) != 0 {
				cfg.clusterId = v
			}
		case "restauthenabled":
			authEnabled = dstrings.ToLower(v) == "true"
		case "volumetype":
			parseVolumeType = v

		default:
			return nil, fmt.Errorf("minfs: invalid option %q for volume plugin %s", k, minfsPluginName)
		}
	}

	if len(cfg.url) == 0 {
		return nil, fmt.Errorf("StorageClass for provisioner %s must contain 'resturl' parameter", minfsPluginName)
	}

	if len(parseVolumeType) == 0 {
		cfg.volumeType = gapi.VolumeDurabilityInfo{Type: gapi.DurabilityReplicate, Replicate: gapi.ReplicaDurability{Replica: replicaCount}}
	} else {
		parseVolumeTypeInfo := dstrings.Split(parseVolumeType, ":")

		switch parseVolumeTypeInfo[0] {
		case "replicate":
			if len(parseVolumeTypeInfo) >= 2 {
				newReplicaCount, err := convertVolumeParam(parseVolumeTypeInfo[1])
				if err != nil {
					return nil, fmt.Errorf("error [%v] when parsing value %q of option '%s' for volume plugin %s.", err, parseVolumeTypeInfo[1], "volumetype", minfsPluginName)
				}
				cfg.volumeType = gapi.VolumeDurabilityInfo{Type: gapi.DurabilityReplicate, Replicate: gapi.ReplicaDurability{Replica: newReplicaCount}}
			} else {
				cfg.volumeType = gapi.VolumeDurabilityInfo{Type: gapi.DurabilityReplicate, Replicate: gapi.ReplicaDurability{Replica: replicaCount}}
			}
		case "disperse":
			if len(parseVolumeTypeInfo) >= 3 {
				newDisperseData, err := convertVolumeParam(parseVolumeTypeInfo[1])
				if err != nil {
					return nil, fmt.Errorf("error [%v] when parsing value %q of option '%s' for volume plugin %s.", parseVolumeTypeInfo[1], err, "volumetype", minfsPluginName)
				}
				newDisperseRedundancy, err := convertVolumeParam(parseVolumeTypeInfo[2])
				if err != nil {
					return nil, fmt.Errorf("error [%v] when parsing value %q of option '%s' for volume plugin %s.", err, parseVolumeTypeInfo[2], "volumetype", minfsPluginName)
				}
				cfg.volumeType = gapi.VolumeDurabilityInfo{Type: gapi.DurabilityEC, Disperse: gapi.DisperseDurability{Data: newDisperseData, Redundancy: newDisperseRedundancy}}
			} else {
				return nil, fmt.Errorf("StorageClass for provisioner %q must have data:redundancy count set for disperse volumes in storage class option '%s'", minfsPluginName, "volumetype")
			}
		case "none":
			cfg.volumeType = gapi.VolumeDurabilityInfo{Type: gapi.DurabilityDistributeOnly}
		default:
			return nil, fmt.Errorf("error parsing value for option 'volumetype' for volume plugin %s", minfsPluginName)
		}
	}
	if !authEnabled {
		cfg.user = ""
		cfg.secretName = ""
		cfg.secretNamespace = ""
		cfg.userKey = ""
		cfg.secretValue = ""
	}

	if len(cfg.secretName) != 0 || len(cfg.secretNamespace) != 0 {
		// secretName + Namespace has precedence over userKey
		if len(cfg.secretName) != 0 && len(cfg.secretNamespace) != 0 {
			cfg.secretValue, err = parseSecret(cfg.secretNamespace, cfg.secretName, kubeClient)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, fmt.Errorf("StorageClass for provisioner %q must have secretNamespace and secretName either both set or both empty", minfsPluginName)
		}
	} else {
		cfg.secretValue = cfg.userKey
	}

	if cfg.gidMin > cfg.gidMax {
		return nil, fmt.Errorf("StorageClass for provisioner %q must have gidMax value >= gidMin", minfsPluginName)
	}

	return &cfg, nil
}
