// This file is part of MinIO DirectPV
// Copyright (c) 2021, 2022 MinIO, Inc.
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

package node

import (
	"context"
	"fmt"
	"strings"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/client"
	"github.com/minio/directpv/pkg/sys"
	"github.com/minio/directpv/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

func (d *driveEventHandler) syncDrive(device *sys.Device, drive *directcsi.DirectCSIDrive) (*directcsi.DirectCSIDrive, error) {
	updatedDrive := d.syncDriveStates(device, drive)
	return updatedDrive, checkDirectCSIDriveStatus(drive, updatedDrive)
}

func (d *driveEventHandler) syncDriveStates(device *sys.Device, drive *directcsi.DirectCSIDrive) *directcsi.DirectCSIDrive {
	updatedDrive := drive.DeepCopy()
	updatedDrive.Status.Filesystem = device.FSType
	updatedDrive.Status.LogicalBlockSize = int64(device.LogicalBlockSize)
	updatedDrive.Status.MountOptions = device.FirstMountOptions
	updatedDrive.Status.Mountpoint = device.FirstMountPoint
	updatedDrive.Status.NodeName = d.nodeID
	updatedDrive.Status.PartitionNum = device.Partition
	updatedDrive.Status.PhysicalBlockSize = int64(device.PhysicalBlockSize)
	updatedDrive.Status.RootPartition = device.Name
	updatedDrive.Status.FilesystemUUID = device.FSUUID
	updatedDrive.Status.PartitionUUID = device.PartUUID
	updatedDrive.Status.MajorNumber = uint32(device.Major)
	updatedDrive.Status.MinorNumber = uint32(device.Minor)
	updatedDrive.Status.Topology = d.topology
	updatedDrive.Status.UeventFSUUID = device.UeventFSUUID
	updatedDrive.Status.DMName = device.DMName
	updatedDrive.Status.DMUUID = device.DMUUID
	updatedDrive.Status.MDUUID = device.MDUUID
	updatedDrive.Status.PartTableUUID = device.PTUUID
	updatedDrive.Status.PartTableType = device.PTType
	updatedDrive.Status.Virtual = device.Virtual
	updatedDrive.Status.ReadOnly = device.ReadOnly
	updatedDrive.Status.Partitioned = device.Partitioned
	updatedDrive.Status.SwapOn = device.SwapOn
	updatedDrive.Status.Master = device.Master

	// fill hwinfo only if it is empty
	if updatedDrive.Status.ModelNumber == "" {
		updatedDrive.Status.ModelNumber = device.Model
	}
	if updatedDrive.Status.SerialNumber == "" {
		updatedDrive.Status.SerialNumber = device.Serial
	}
	if updatedDrive.Status.UeventSerial == "" {
		updatedDrive.Status.UeventSerial = device.UeventSerial
	}
	if updatedDrive.Status.WWID == "" {
		updatedDrive.Status.WWID = device.WWID
	}
	if updatedDrive.Status.Vendor == "" {
		updatedDrive.Status.Vendor = device.Vendor
	}

	// check and update drive status
	updatedDrive.Status.DriveStatus = directcsi.DriveStatusAvailable
	if device.Size < sys.MinSupportedDeviceSize || device.ReadOnly || device.Partitioned || device.SwapOn || device.Master != "" || !isDirectCSIMount(device.MountPoints) {
		updatedDrive.Status.DriveStatus = directcsi.DriveStatusUnavailable
	}

	// update the path and respective label value
	updatedDrive.Status.Path = device.DevPath()
	utils.UpdateLabels(updatedDrive, map[utils.LabelKey]utils.LabelValue{
		utils.DriveLabelKey: utils.NewLabelValue(utils.SanitizeDrivePath(device.Name)),
	})

	// capacity sync
	updatedDrive.Status.TotalCapacity = int64(device.Size)
	if updatedDrive.Status.DriveStatus == directcsi.DriveStatusInUse {
		// FIXME: handle if an in-use drive size is shrinked
		// if updatedDrive.Status.AllocatedCapacity > updatedDrive.Status.TotalCapacity {
		// 	updatedDrive.Status.AllocatedCapacity = updatedDrive.Status.TotalCapacity
		// }
	} else {
		updatedDrive.Status.AllocatedCapacity = int64(device.Size - device.FreeCapacity)
	}
	updatedDrive.Status.FreeCapacity = updatedDrive.Status.TotalCapacity - updatedDrive.Status.AllocatedCapacity

	return updatedDrive
}

func checkDirectCSIDriveStatus(oldDrive, updatedDrive *directcsi.DirectCSIDrive) error {
	errInvalidDrive := func(fieldName string, expected, found interface{}) error {
		return fmt.Errorf("[DRIVE INVALID] %s mismatch. Expected %v found %v",
			fieldName,
			expected,
			found)
	}

	switch oldDrive.Status.DriveStatus {
	case directcsi.DriveStatusInUse, directcsi.DriveStatusReady, directcsi.DriveStatusTerminating:
		switch {
		case oldDrive.Status.UeventFSUUID != updatedDrive.Status.UeventFSUUID:
			// FIXME: drive becomes corrupted/invalid as the format was changed
			return errInvalidDrive("FSUUID", oldDrive.Status.UeventFSUUID, updatedDrive.Status.UeventFSUUID)
		case oldDrive.Status.Mountpoint != updatedDrive.Status.Mountpoint:
			// FIXME: drive becomes corruped/invalid as the mountpoint is changed
			return errInvalidDrive("Mountpoint", oldDrive.Status.Mountpoint, updatedDrive.Status.Mountpoint)
		case updatedDrive.Status.DriveStatus == directcsi.DriveStatusUnavailable:
			// FIXME: the drive becomes unavailable
			return nil
		default:
			return nil
		}
	default:
		return nil
	}
}

func syncVolumeLabels(ctx context.Context, drive *directcsi.DirectCSIDrive) error {
	volumeInterface := client.GetLatestDirectCSIVolumeInterface()
	updateLabels := func(volumeName, driveName string) func() error {
		return func() error {
			volume, err := volumeInterface.Get(
				ctx, volumeName, metav1.GetOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
			)
			if err != nil {
				return err
			}
			volume.Labels[string(utils.DrivePathLabelKey)] = driveName
			_, err = volumeInterface.Update(
				ctx, volume, metav1.UpdateOptions{TypeMeta: utils.DirectCSIVolumeTypeMeta()},
			)
			return err
		}
	}
	for _, finalizer := range drive.GetFinalizers() {
		if !strings.HasPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix) {
			continue
		}
		volumeName := strings.TrimPrefix(finalizer, directcsi.DirectCSIDriveFinalizerPrefix)
		if err := retry.RetryOnConflict(
			retry.DefaultRetry, // FIXME: is retryOnConflict required?
			updateLabels(volumeName, utils.SanitizeDrivePath(drive.Status.Path))); err != nil {
			klog.ErrorS(err, "unable to update volume %v", volumeName)
			return err
		}
	}
	return nil
}

func isDirectCSIMount(mountPoints []string) bool {
	if len(mountPoints) == 0 {
		return true
	}

	for _, mountPoint := range mountPoints {
		if strings.HasPrefix(mountPoint, "/var/lib/direct-csi/") {
			return true
		}
	}
	return false
}
