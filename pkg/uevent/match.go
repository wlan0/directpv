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
	"errors"
	"fmt"
	"strings"

	directcsi "github.com/minio/directpv/pkg/apis/direct.csi.min.io/v1beta3"
	"github.com/minio/directpv/pkg/sys"
)

type matchFn func(device *sys.Device, drive *directcsi.DirectCSIDrive) (match bool, consider bool, err error)

var (
	errNoMatchFound = errors.New("no matching drive found")
)

var matchers = []matchFn{
	// v1beta3 matchers
	// HWInfoMatchers (conclusive)
	partitionNumberMatcher,
	ueventSerialNumberMatcher,
	wwidMatcher,
	modelNumberMatcher,
	vendorMatcher,
	// If there are more than one matching drive, continue.. else conclude
	// SWInfoMatchers (non conclusive)
	partitionTableUUIDMatcher,
	partitionUUIDMatcher,
	dmUUIDMatcher,
	mdUUIDMatcher,
	//filesystemMatcher,
	ueventFSUUIDMatcher,

	// // If there are more than one matching drive, continue.. else conclude
	// // v1beta2 conclusive matchers
	fsUUIDMatcher,
	serialNumberMatcher,

	// // If there are more than one matching drive, continue.. else conclude
	// // v1beta1 conclusive matchers
	logicalBlocksizeMatcher,
	// physicalBlocksizeMatcher,
	// totalCapacityMatcher,
	// allocatedCapacityMatcher,
}

func runMatcher(device *sys.Device, drives []*directcsi.DirectCSIDrive) (*directcsi.DirectCSIDrive, error) {
	matchedDrives, _ := getMatchingDrives(device, drives)
	switch len(matchedDrives) {
	case 1:
		return matchedDrives[0], nil
	case 0:
		return nil, errNoMatchFound
	default:
		// handle too many matches
		//
		// case 1: It is possible to have an empty/partial drive (&directCSIDrive.Status{Path: /dev/sdb0, "", "", "", ...})
		//         to match  with a correct match

		// case 2: A terminating drive and an actual drive can be matched with the single device
		//
		// case 3: A duplicate drive (due to any bug)
		//
		// ToDo: make these drives invalid / decide based on drive status / calculate ranks and decide
		return nil, fmt.Errorf("device %s has too many matches", device.Name)
	}
}

func getMatchingDrives(device *sys.Device, drives []*directcsi.DirectCSIDrive) (matchedDrives []*directcsi.DirectCSIDrive, matchList map[*directcsi.DirectCSIDrive][]matchFn) {
	for _, drive := range drives {
		var matchedFn []matchFn
		for _, matchFn := range matchers {
			match, cont, _ := matchFn(device, drive)
			if cont {
				continue
			}
			if match {
				matchedDrives = append(matchedDrives, drive)
			}
			matchedFn = append(matchedFn, matchFn)
			break
		}
		matchList[drive] = matchedFn
	}
	return
}

func isDOSPTType(ptType string) bool {
	switch ptType {
	case "dos", "msdos", "mbr":
		return true
	default:
		return false
	}
}

func partitionNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return drive.Status.PartitionNum == device.Partition, false, nil
}

func ueventSerialNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.UeventSerial == "" {
		return false, true, nil
	}
	return drive.Status.UeventSerial == device.UeventSerial, false, nil
}

func wwidMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.WWID == "" {
		return false, true, nil
	}
	return drive.Status.WWID == device.WWID, false, nil
}

func modelNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.ModelNumber == "" {
		return false, true, nil
	}
	return drive.Status.ModelNumber == device.Model, false, nil
}

func vendorMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.Vendor == "" {
		return false, true, nil
	}
	return drive.Status.Vendor == device.Vendor, false, nil
}

func partitionTableUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.PartTableUUID == "" || drive.Status.PartTableType == "" {
		return false, true, nil
	}
	return drive.Status.PartTableUUID == device.PTUUID &&
		ptTypeEqual(drive.Status.PartTableType, device.PTType), false, nil
}

func partitionUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.PartitionUUID == "" {
		return false, true, nil
	}
	return drive.Status.PartitionUUID == device.PartUUID, false, nil
}

func dmUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.DMUUID == "" {
		return false, true, nil
	}
	return drive.Status.DMUUID == device.DMUUID, false, nil
}

func mdUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.MDUUID == "" {
		return false, true, nil
	}
	return drive.Status.MDUUID == device.MDUUID, false, nil
}

func ueventFSUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.UeventFSUUID == "" {
		return false, true, nil
	}
	return drive.Status.UeventFSUUID == device.UeventFSUUID, false, nil
}

func fsUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.FilesystemUUID == "" {
		return false, true, nil
	}
	return drive.Status.FilesystemUUID == device.FSUUID, false, nil
}

func serialNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	if drive.Status.SerialNumber == "" {
		return false, true, nil
	}
	return drive.Status.SerialNumber == device.Serial, false, nil
}

func logicalBlocksizeMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (bool, bool, error) {
	return drive.Status.LogicalBlockSize == int64(device.LogicalBlockSize), false, nil
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
