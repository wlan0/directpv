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
	"errors"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/client"
	"github.com/minio/directpv/pkg/sys"
	"github.com/minio/directpv/pkg/uevent"
	"github.com/minio/directpv/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

var (
	errNoMatchFound = errors.New("no matching drive found")
)

func RunDynamicDriveHandler(ctx context.Context,
	identity, nodeID, rack, zone, region string,
	loopbackOnly bool) error {

	handler := &driveEventHandler{
		nodeID: nodeID,
		topology: map[string]string{
			string(utils.TopologyDriverIdentity): identity,
			string(utils.TopologyDriverRack):     rack,
			string(utils.TopologyDriverZone):     zone,
			string(utils.TopologyDriverRegion):   region,
			string(utils.TopologyDriverNode):     nodeID,
		},
		createDevice: sys.CreateDevice,
	}

	listener, err := uevent.NewListener(handler)
	if err != nil {
		return err
	}
	defer listener.Close()

	return listener.Run(ctx)
}

type driveEventHandler struct {
	nodeID       string
	topology     map[string]string
	createDevice func(event map[string]string) (device *sys.Device, err error)
}

func (d *driveEventHandler) remove(ctx context.Context,
	device *sys.Device,
	drive *directcsi.DirectCSIDrive) error {

	return nil
}

func (d *driveEventHandler) update(ctx context.Context,
	device *sys.Device,
	drive *directcsi.DirectCSIDrive) error {

	var updated, nameChanged, fsUUIDChanged bool

	if !sys.FSTypeEqual(drive.Status.Filesystem, device.FSType) {
		drive.Status.Filesystem = device.FSType
		updated = true
	}

	if drive.Status.TotalCapacity != int64(device.Size) {
		drive.Status.TotalCapacity = int64(device.Size)
		if drive.Status.AllocatedCapacity > drive.Status.TotalCapacity {
			drive.Status.AllocatedCapacity = drive.Status.TotalCapacity
		}
		drive.Status.FreeCapacity = drive.Status.TotalCapacity - drive.Status.AllocatedCapacity
		updated = true
	}

	if drive.Status.LogicalBlockSize != int64(device.LogicalBlockSize) {
		drive.Status.LogicalBlockSize = int64(device.LogicalBlockSize)
		updated = true
	}

	if drive.Status.ModelNumber != device.Model {
		drive.Status.ModelNumber = device.Model
		updated = true
	}

	if drive.Status.PartitionNum != device.Partition {
		drive.Status.PartitionNum = device.Partition
		updated = true
	}

	if drive.Status.Path != "/dev/"+device.Name {
		drive.Status.Path = "/dev/" + device.Name
		if drive.Labels == nil {
			drive.Labels = map[string]string{}
		}
		drive.Labels[string(utils.DriveLabelKey)] = utils.SanitizeDrivePath(device.Name)
		nameChanged = true
		updated = true
	}

	if drive.Status.PhysicalBlockSize != int64(device.PhysicalBlockSize) {
		drive.Status.PhysicalBlockSize = int64(device.PhysicalBlockSize)
		updated = true
	}

	if drive.Status.RootPartition != device.Name {
		drive.Status.RootPartition = device.Name
		updated = true
	}

	if drive.Status.SerialNumber != device.Serial {
		drive.Status.SerialNumber = device.Serial
		updated = true
	}

	if drive.Status.FilesystemUUID != device.FSUUID {
		drive.Status.FilesystemUUID = device.FSUUID
		updated = true
		fsUUIDChanged = true
	}

	if drive.Status.PartitionUUID != device.PartUUID {
		drive.Status.PartitionUUID = device.PartUUID
		updated = true
	}

	if drive.Status.MajorNumber != uint32(device.Major) {
		drive.Status.MajorNumber = uint32(device.Major)
		updated = true
	}

	if drive.Status.MinorNumber != uint32(device.Minor) {
		drive.Status.MinorNumber = uint32(device.Minor)
		updated = true
	}

	if drive.Status.UeventSerial != device.UeventSerial {
		drive.Status.UeventSerial = device.UeventSerial
		updated = true
	}

	if drive.Status.UeventFSUUID != device.UeventFSUUID {
		drive.Status.UeventFSUUID = device.UeventFSUUID
		updated = true
		fsUUIDChanged = true
	}

	if drive.Status.WWID != device.WWID {
		drive.Status.WWID = device.WWID
		updated = true
	}

	if drive.Status.Vendor != device.Vendor {
		drive.Status.Vendor = device.Vendor
		updated = true
	}

	if drive.Status.DMName != device.DMName {
		drive.Status.DMName = device.DMName
		updated = true
	}

	if drive.Status.DMUUID != device.DMUUID {
		drive.Status.DMUUID = device.DMUUID
		updated = true
	}

	if drive.Status.MDUUID != device.MDUUID {
		drive.Status.MDUUID = device.MDUUID
		updated = true
	}

	if drive.Status.PartTableUUID != device.PTUUID {
		drive.Status.PartTableUUID = device.PTUUID
		updated = true
	}

	if !ptTypeEqual(drive.Status.PartTableType, device.PTType) {
		drive.Status.PartTableType = device.PTType
		updated = true
	}

	if drive.Status.Virtual != device.Virtual {
		drive.Status.Virtual = device.Virtual
		updated = true
	}

	if drive.Status.ReadOnly != device.ReadOnly {
		drive.Status.ReadOnly = device.ReadOnly
		updated = true
	}

	if drive.Status.Partitioned != device.Partitioned {
		drive.Status.Partitioned = device.Partitioned
		updated = true
	}

	if drive.Status.SwapOn != device.SwapOn {
		drive.Status.SwapOn = device.SwapOn
		updated = true
	}

	if drive.Status.Master != device.Master {
		drive.Status.Master = device.Master
		updated = true
	}

	switch drive.Status.DriveStatus {
	case directcsi.DriveStatusInUse, directcsi.DriveStatusReady, directcsi.DriveStatusTerminating:
		switch {
		case fsUUIDChanged:
			// FIXME: delete the drive and add a new drive?!
		case device.FirstMountPoint != "" && drive.Status.Mountpoint != device.FirstMountPoint:
			if device.FirstMountPoint != filepath.Join(sys.MountRoot, drive.Status.FilesystemUUID) {
				// FIXME: the device is mounted outside of /var/lib/direct-csi/mnt/<FSUUID>, do we error out or unmount/mount correctly?!
			} else {
				drive.Status.Mountpoint = device.FirstMountPoint
				drive.Status.MountOptions = device.FirstMountOptions
				updated = true
			}
		}
	case directcsi.DriveStatusAvailable, directcsi.DriveStatusUnavailable:
		if drive.Status.Mountpoint != device.FirstMountPoint {
			drive.Status.Mountpoint = device.FirstMountPoint
			drive.Status.MountOptions = device.FirstMountOptions
			updated = true
		}
	}

	if driveStatus := client.NewDriveStatus(device); driveStatus != drive.Status.DriveStatus {
		switch drive.Status.DriveStatus {
		case directcsi.DriveStatusInUse, directcsi.DriveStatusReady, directcsi.DriveStatusTerminating:
			if driveStatus == directcsi.DriveStatusUnavailable {
				// FIXME: due to drive property change, the drive becomes unavailable. What do we need to do?
			}
		default:
			drive.Status.DriveStatus = driveStatus
			updated = true
		}
	}

	if !updated {
		return nil
	}

	_, err := client.GetLatestDirectCSIDriveInterface().Update(
		ctx, drive, metav1.UpdateOptions{TypeMeta: utils.DirectCSIDriveTypeMeta()},
	)
	if err != nil {
		klog.ErrorS(err, "unable to update drive", "Path", drive.Status.Path, "device.Name", device.Name)
		return err
	}

	switch drive.Status.DriveStatus {
	case directcsi.DriveStatusInUse, directcsi.DriveStatusReady:
		// FIXME: mount the drive
	}

	if !nameChanged {
		return nil
	}

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
		go func() { // FIXME: do we need a blocking function?
			err := retry.RetryOnConflict(retry.DefaultRetry, updateLabels(volumeName, utils.SanitizeDrivePath(drive.Status.Path)))
			if err != nil {
				klog.ErrorS(err, "unable to update volume %v", volumeName)
			}
		}()
	}

	return nil
}

func (d *driveEventHandler) add(
	ctx context.Context,
	device *sys.Device) error {
	drive := client.NewDirectCSIDrive(
		uuid.New().String(),
		client.NewDirectCSIDriveStatus(device, d.nodeID, d.topology),
	)
	err := client.CreateDrive(ctx, drive)
	if err != nil {
		klog.ErrorS(err, "unable to create drive", "Status.Path", drive.Status.Path)
	}
	return err
}

func (d *driveEventHandler) findMatchingDrive(drives []directcsi.DirectCSIDrive, device *sys.Device) (*directcsi.DirectCSIDrive, error) {
	//  todo: run matching algorithm to find matching drive
	//  note: return `errNoMatchFound` if no match is found
	return nil, errNoMatchFound
}

func (d *driveEventHandler) Handle(ctx context.Context, event map[string]string) error {

	if sys.LoopRegexp.MatchString(path.Base(event["DEVPATH"])) {
		klog.V(5).InfoS(
			"loopback device is ignored",
			"ACTION", event["ACTION"],
			"DEVPATH", event["DEVPATH"])
		return nil
	}

	device, err := d.createDevice(event)
	if err != nil {
		klog.ErrorS(err, "ACTION", event["ACTION"], "DEVPATH", event["DEVPATH"])
		return nil
	}

	drives, err := client.GetDriveList(
		ctx,
		[]utils.LabelValue{utils.NewLabelValue(d.nodeID)},
		[]utils.LabelValue{utils.NewLabelValue(device.Name)},
		nil,
	)
	if err != nil {
		klog.ErrorS(err, "error while fetching drive list")
		return err
	}

	drive, err := d.findMatchingDrive(drives, device)
	switch {
	case errors.Is(err, errNoMatchFound):
		if event["ACTION"] == uevent.Remove {
			klog.V(3).InfoS(
				"matching drive not found",
				"ACTION", uevent.Remove,
				"DEVPATH", event["DEVPATH"])
			return nil
		}
		return d.add(ctx, device)
	case err == nil:
		switch event["ACTION"] {
		case uevent.Remove:
			return d.remove(ctx, device, drive)
		default:
			return d.update(ctx, device, drive)
		}
	default:
		return err
	}
}
