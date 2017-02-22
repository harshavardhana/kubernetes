/*
Copyright 2015 The Kubernetes Authors.

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

import ()

// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	// TODO: See the comment below.
	return []volume.VolumePlugin{minfsPlugin{}}
}

// Here is the volume.VolumePlugin interface https://github.com/kubernetes/kubernetes/blob/master/pkg/volume/plugins.go#L65
// the minfsPlugin should implement this interface.
type minfsPlugin struct {
	// TODO: Need to figure what should go inside this struct and implement the volume.VolumePlugin interface.
}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) Init(host volume.VolumeHost) error {

}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) GetPluginName() string {

}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) GetVolumeName(spec *volume.Spec) (string, error) {
}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) CanSupport(spec *volume.Spec) bool {

}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) RequiresRemount() bool {

}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) GetAccessModes() []v1.PersistentVolumeAccessMode {

}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) NewMounter(spec *volume.Spec, pod *v1.Pod, _ volume.VolumeOptions) (volume.Mounter, error) {

}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) NewUnmounter(volName string, podUID types.UID) (volume.Unmounter, error) {

}

// TODO: Implement the method.
// this is to satisfy volume.Volume interface.
func (plugin *minfsPlugin) ConstructVolumeSpec(volumeName, mountPath string) (*volume.Spec, error) {

}

// this struct should the Mounter interface defined in https://github.com/kubernetes/kubernetes/blob/master/pkg/volume/volume.go#L94 .
// looks like the premount checks and the mount happens here.
// TODO: Not sure what fields should go into this struct.
type minfsMounter struct {
	server     string
	exportPath string
	readOnly   bool
}

var _ volume.Mounter = &minfsMounter{}

// methods below are implementing the Mounter interface.
func (b *nfsMounter) GetAttributes() volume.Attributes {
	return volume.Attributes{}
}

// TODO: Retaining NFS's implementation to give you an idea.
// IT looks like the binaries should be manually installed on the pods.
// even from the prerequisite section of the example at https://github.com/kubernetes/kubernetes/tree/7d6eba69848528c06f57090fad80ce23fc8a586c/examples/volumes
// this looks evident.
// Checks prior to mount operations to verify that the required components (binaries, etc.)
// to mount the volume are available on the underlying node.
// If not, it returns an error.
func (nfsMounter *nfsMounter) CanMount() error {
	exe := exec.New()
	switch runtime.GOOS {
	case "linux":
		_, err1 := exe.Command("/bin/ls", "/sbin/mount.nfs").CombinedOutput()
		_, err2 := exe.Command("/bin/ls", "/sbin/mount.nfs4").CombinedOutput()

		if err1 != nil {
			return fmt.Errorf("Required binary /sbin/mount.nfs is missing")
		}
		if err2 != nil {
			return fmt.Errorf("Required binary /sbin/mount.nfs4 is missing")
		}
		return nil
	case "darwin":
		_, err := exe.Command("/bin/ls", "/sbin/mount_nfs").CombinedOutput()
		if err != nil {
			return fmt.Errorf("Required binary /sbin/mount_nfs is missing")
		}
	}
	return nil
}

// SetUp attaches the disk and bind mounts to the volume path.
func (b *nfsMounter) SetUp(fsGroup *int64) error {
	return b.SetUpAt(b.GetPath(), fsGroup)
}

func (b *nfsMounter) SetUpAt(dir string, fsGroup *int64) error {

}
