// This file is part of MinIO Direct CSI
// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package volume

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	directcsi "github.com/minio/direct-csi/pkg/apis/direct.csi.min.io/v1beta2"
	"github.com/minio/direct-csi/pkg/clientset"
	"github.com/minio/direct-csi/pkg/listener2"
	"github.com/minio/direct-csi/pkg/utils"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"

	"k8s.io/klog/v2"
)

func excludeFinalizer(finalizers []string, finalizer string) (result []string, found bool) {
	for _, f := range finalizers {
		if f != finalizer {
			result = append(result, f)
		} else {
			found = true
		}
	}
	return
}

type VolumeEventHandler struct {
	kubeClient      kubernetes.Interface
	directCSIClient clientset.Interface
	nodeID          string
}

func NewVolumeEventHandler(nodeID string) *VolumeEventHandler {
	return &VolumeEventHandler{
		directCSIClient: utils.GetDirectClientset(),
		kubeClient:      utils.GetKubeClient(),
		nodeID:          nodeID,
	}
}

func (handler *VolumeEventHandler) RESTClient() rest.Interface {
	return handler.directCSIClient.DirectV1beta2().RESTClient()
}

func (handler *VolumeEventHandler) KubeClient() kubernetes.Interface {
	return handler.kubeClient
}

func (handler *VolumeEventHandler) Name() string {
	return "volume"
}

func (handler *VolumeEventHandler) Resource() string {
	return "DirectCSIVolumes"
}

func (handler *VolumeEventHandler) Namespace() string {
	return ""
}

func (handler *VolumeEventHandler) NodeID() string {
	return handler.nodeID
}

func (handler *VolumeEventHandler) ObjectType() runtime.Object {
	return &directcsi.DirectCSIVolume{}
}

func (handler *VolumeEventHandler) Handle(ctx context.Context, args listener2.EventArgs) error {
	switch args.Event {
	case listener2.AddEvent:
		return handler.Add(ctx, args.Object.(*directcsi.DirectCSIVolume))
	case listener2.UpdateEvent:
		return handler.Update(ctx, args.OldObject.(*directcsi.DirectCSIVolume), args.Object.(*directcsi.DirectCSIVolume))
	case listener2.DeleteEvent:
		return handler.Delete(ctx, args.Object.(*directcsi.DirectCSIVolume))
	}
	return nil
}

func (handler *VolumeEventHandler) releaseVolume(ctx context.Context, driveName, volumeName string, capacity int64) error {
	drive, err := handler.directCSIClient.DirectV1beta2().DirectCSIDrives().Get(
		ctx, driveName, metav1.GetOptions{
			TypeMeta: utils.DirectCSIDriveTypeMeta(),
		},
	)
	if err != nil {
		return err
	}

	finalizers, found := excludeFinalizer(
		drive.GetFinalizers(), directcsi.DirectCSIDriveFinalizerPrefix+volumeName,
	)

	if found {
		if len(finalizers) == 1 {
			if finalizers[0] == directcsi.DirectCSIDriveFinalizerDataProtection {
				drive.Status.DriveStatus = directcsi.DriveStatusReady
			}
		}

		drive.SetFinalizers(finalizers)
		drive.Status.FreeCapacity += capacity
		drive.Status.AllocatedCapacity = drive.Status.TotalCapacity - drive.Status.FreeCapacity

		_, err = handler.directCSIClient.DirectV1beta2().DirectCSIDrives().Update(
			ctx, drive, metav1.UpdateOptions{
				TypeMeta: utils.DirectCSIDriveTypeMeta(),
			},
		)
	}

	return err
}

func (handler *VolumeEventHandler) remove(ctx context.Context, volume *directcsi.DirectCSIVolume) error {
	// Ignore if volume does not have deletion timestamp.
	if volume.GetDeletionTimestamp().IsZero() {
		return nil
	}

	// Error out if volume is still in published state.
	for _, condition := range volume.Status.Conditions {
		if directcsi.DirectCSIVolumeCondition(condition.Type) == directcsi.DirectCSIVolumeConditionPublished && condition.Status == metav1.ConditionTrue {
			return fmt.Errorf("waiting for volume to be released before cleaning up")
		}
	}

	// Remove associated directory of the volume.
	if err := os.RemoveAll(volume.Status.HostPath); err != nil {
		return err
	}

	// Release volume from associated drive.
	if err := handler.releaseVolume(ctx, volume.Status.Drive, volume.Name, volume.Status.TotalCapacity); err != nil {
		return err
	}

	// Update volume finalizer.
	finalizers, _ := excludeFinalizer(
		volume.GetFinalizers(), string(directcsi.DirectCSIVolumeFinalizerPurgeProtection),
	)
	volume.SetFinalizers(finalizers)

	_, err := handler.directCSIClient.DirectV1beta2().DirectCSIVolumes().Update(
		ctx, volume, metav1.UpdateOptions{
			TypeMeta: utils.DirectCSIVolumeTypeMeta(),
		},
	)

	return err
}

func (handler *VolumeEventHandler) Add(ctx context.Context, obj *directcsi.DirectCSIVolume) error {
	return handler.remove(ctx, obj)
}

func (handler *VolumeEventHandler) Update(ctx context.Context, old, new *directcsi.DirectCSIVolume) error {
	return handler.remove(ctx, new)
}

func (handler *VolumeEventHandler) Delete(ctx context.Context, obj *directcsi.DirectCSIVolume) error {
	return nil
}

func StartController(ctx context.Context, nodeID string) error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	listener := listener2.NewListener(NewVolumeEventHandler(nodeID), "volume-controller", hostname, 40)
	return listener.Run(ctx)
}

func getLabels(ctx context.Context, volume *directcsi.DirectCSIVolume) map[string]string {
	drive, err := utils.GetDirectCSIClient().DirectCSIDrives().Get(
		ctx, volume.Status.Drive, metav1.GetOptions{
			TypeMeta: utils.DirectCSIDriveTypeMeta(),
		},
	)

	var driveName, drivePath string
	if err == nil {
		finalizer := directcsi.DirectCSIDriveFinalizerPrefix + volume.Name
		for _, f := range drive.GetFinalizers() {
			if f == finalizer {
				driveName, drivePath = drive.Name, drive.Status.Path
				break
			}
		}
	}

	labels := volume.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[utils.NodeLabel] = volume.Status.NodeName
	labels[utils.ReservedDrivePathLabel] = filepath.Base(drivePath)
	labels[utils.DriveLabel] = utils.SanitizeLabelV(driveName)
	labels[utils.CreatedByLabel] = "directcsi-controller"

	return labels
}

func SyncVolumes(ctx context.Context, nodeID string) {
	volumeClient := utils.GetDirectCSIClient().DirectCSIVolumes()

	volumeList, err := volumeClient.List(
		ctx, metav1.ListOptions{
			TypeMeta: utils.DirectCSIVolumeTypeMeta(),
		},
	)
	if err != nil {
		klog.V(3).Infof("Error while syncing CRD versions in directcsivolume: %v", err)
		return
	}

	volumes := volumeList.Items
	for _, volume := range volumes {
		// Skip volumes from other nodes
		if volume.Status.NodeName != nodeID {
			continue
		}

		updateLabels := func(volume *directcsi.DirectCSIVolume) func() error {
			return func() error {
				volume, err := volumeClient.Get(
					ctx, volume.Name, metav1.GetOptions{
						TypeMeta: utils.DirectCSIVolumeTypeMeta(),
					},
				)
				if err != nil {
					return err
				}

				// update labels
				volume.SetLabels(getLabels(ctx, volume))
				_, err = volumeClient.Update(
					ctx, volume, metav1.UpdateOptions{
						TypeMeta: utils.DirectCSIVolumeTypeMeta(),
					},
				)
				return err
			}
		}

		if err := retry.RetryOnConflict(retry.DefaultRetry, updateLabels(&volume)); err != nil {
			klog.V(3).Infof("Error while syncing CRD versions in directcsivolume: %v", err)
		}
	}
}
