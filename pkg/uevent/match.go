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

package uevent

import (
	"strings"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/sys"
)

func isDOSPTType(ptType string) bool {
	switch ptType {
	case "dos", "msdos", "mbr":
		return true
	default:
		return false
	}
}

func ptTypeEqual(ptType1, ptType2 string) bool {
	ptType1, ptType2 = strings.ToLower(ptType1), strings.ToLower(ptType2)
	switch {
	case ptType1 == ptType2:
		return true
	case isDOSPTType(ptType1) && isDOSPTType(ptType2):
		return true
	default:
		return false
	}
}

func isHWInfoAvailable(drive *directcsi.DirectCSIDrive) bool {
	return drive.Status.WWID != "" || drive.Status.SerialNumber != "" || drive.Status.UeventSerial != ""
}

func matchDeviceHWInfo(drive *directcsi.DirectCSIDrive, device *sys.UDevData) bool {
	switch {
	case drive.Status.PartitionNum != device.Partition:
		return false
	case drive.Status.WWID != "" && drive.Status.WWID != device.WWID:
		return false
	case drive.Status.SerialNumber != "" && drive.Status.SerialNumber != device.Serial:
		return false
	case drive.Status.UeventSerial != "" && drive.Status.UeventSerial != device.UeventSerial:
		return false
	case drive.Status.ModelNumber != "" && drive.Status.ModelNumber != device.Model:
		return false
	case drive.Status.Vendor != "" && drive.Status.Vendor != device.Vendor:
		return false
	}

	return true
}

func isDMMDUUIDAvailable(drive *directcsi.DirectCSIDrive) bool {
	return drive.Status.DMUUID != "" || drive.Status.MDUUID != ""
}

func matchDeviceDMMDUUID(drive *directcsi.DirectCSIDrive, device *sys.UDevData) bool {
	switch {
	case drive.Status.PartitionNum != device.Partition:
		return false
	case drive.Status.DMUUID != "" && drive.Status.DMUUID != device.DMUUID:
		return false
	case drive.Status.MDUUID != "" && drive.Status.MDUUID != device.MDUUID:
		return false
	}

	return true
}

func isPTUUIDAvailable(drive *directcsi.DirectCSIDrive) bool {
	return drive.Status.PartitionNum <= 0 && drive.Status.PartTableUUID != ""
}

func matchDevicePTUUID(drive *directcsi.DirectCSIDrive, device *sys.UDevData) bool {
	switch {
	case drive.Status.PartitionNum != device.Partition:
		return false
	case drive.Status.PartTableUUID != device.PTUUID:
		return false
	case !ptTypeEqual(drive.Status.PartTableType, device.PTType):
		return false
	}

	return true
}

func isPartUUIDAvailable(drive *directcsi.DirectCSIDrive) bool {
	return drive.Status.PartitionNum > 0 && drive.Status.PartitionUUID != ""
}

func matchDevicePartUUID(drive *directcsi.DirectCSIDrive, device *sys.UDevData) bool {
	switch {
	case drive.Status.PartitionNum != device.Partition:
		return false
	case drive.Status.PartitionUUID != device.PartUUID:
		return false
	}

	return true
}

func isFSUUIDAvailable(drive *directcsi.DirectCSIDrive) bool {
	return drive.Status.FilesystemUUID != "" || drive.Status.UeventFSUUID != ""
}

func matchDeviceFSUUID(drive *directcsi.DirectCSIDrive, device *sys.UDevData) bool {
	switch {
	case drive.Status.PartitionNum != device.Partition:
		return false
	case drive.Status.FilesystemUUID != device.FSUUID:
		return false
	case drive.Status.UeventFSUUID != device.UeventFSUUID:
		return false
	case !sys.FSTypeEqual(drive.Status.Filesystem, device.FSType):
		return false
	}

	return true
}

func splitByMatch(drives []*directcsi.DirectCSIDrive, device *sys.UDevData) (matchedDrives, unmatchedDrives []*directcsi.DirectCSIDrive) {
	for _, drive := range drives {
		if isHWInfoAvailable(drive) {
			if matchDeviceHWInfo(drive, device) {
				matchedDrives = append(matchedDrives, drive)
			} else {
				unmatchedDrives = append(unmatchedDrives, drive)
			}
			continue
		}

		if isDMMDUUIDAvailable(drive) {
			if matchDeviceDMMDUUID(drive, device) {
				matchedDrives = append(matchedDrives, drive)
			} else {
				unmatchedDrives = append(unmatchedDrives, drive)
			}
			continue
		}

		if isPTUUIDAvailable(drive) {
			if matchDevicePTUUID(drive, device) {
				matchedDrives = append(matchedDrives, drive)
			} else {
				unmatchedDrives = append(unmatchedDrives, drive)
			}
			continue
		}

		if isPartUUIDAvailable(drive) {
			if matchDevicePartUUID(drive, device) {
				matchedDrives = append(matchedDrives, drive)
			} else {
				unmatchedDrives = append(unmatchedDrives, drive)
			}
			continue
		}

		if isFSUUIDAvailable(drive) {
			if matchDeviceFSUUID(drive, device) {
				matchedDrives = append(matchedDrives, drive)
			} else {
				unmatchedDrives = append(unmatchedDrives, drive)
			}
			continue
		}
	}

	return matchedDrives, unmatchedDrives
}
