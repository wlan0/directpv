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

type matchFn func(device *sys.Device, drive *directcsi.DirectCSIDrive) (match bool, cont bool)

var (
	errNoMatchFound = errors.New("no matching drive found")
)

var matchers = []matchFn{
	fsUUIDMatcher,
	ueventFSUUIDMatcher,
	serialNumberMatcher,
	ueventSerialNumberMatcher,
	wwidMatcher,
	modelNumberMatcher,
	vendorMatcher,
	//partitionNumberMatcher,
	dmUUIDMatcher,
	mdUUIDMatcher,
	partitionUUIDMatcher,
	partitionTableUUIDMatcher,
	// logicalBlocksizeMatcher,
	// physicalBlocksizeMatcher,
	// filesystemMatcher,
	// totalCapacityMatcher,
	// allocatedCapacityMatcher,
	// mountMatcher,
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
			match, cont := matchFn(device, drive)
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

func fsUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	return genericMatcher(drive.Status.FilesystemUUID, device.FSUUID)
}

func ueventFSUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	return genericMatcher(drive.Status.UeventFSUUID, device.UeventFSUUID)
}

func serialNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	return genericMatcher(drive.Status.SerialNumber, device.Serial)
}

func ueventSerialNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	return genericMatcher(drive.Status.UeventSerial, device.UeventSerial)
}

func wwidMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.WWID == "" || drive.Status.WWID == device.WWID
	return matched, matched
}

func modelNumberMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	return genericMatcher(drive.Status.ModelNumber, device.Model)
}

func vendorMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	return genericMatcher(drive.Status.Vendor, device.Vendor)
}

func dmUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.DMUUID == "" || drive.Status.DMUUID == device.DMUUID
	return matched, matched
}

func mdUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.MDUUID == "" || drive.Status.MDUUID == device.MDUUID
	return matched, matched
}

func partitionUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.PartitionUUID == "" || drive.Status.PartitionUUID == device.PartUUID
	return matched, matched
}

func partitionTableUUIDMatcher(device *sys.Device, drive *directcsi.DirectCSIDrive) (matched, cont bool) {
	matched = drive.Status.PartTableUUID == "" || drive.Status.PartTableUUID == device.PTUUID ||
		drive.Status.PartTableType == "" || ptTypeEqual(drive.Status.PartTableType, device.PTType)
	return matched, matched
}

// if alpha is empty, return false, true
// if alpha is not empty and matches beta, then return true, true
// if alpha is not empty and does not match beta, then return false, false
func genericMatcher(alpha, beta string) (matched, cont bool) {
	if alpha == "" {
		cont = true
	} else {
		matched = alpha == beta
		cont = matched
	}
	return matched, cont

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
